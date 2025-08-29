from __future__ import annotations
import io
import json
import traceback
from pathlib import Path
import pandas as pd
from fastapi import FastAPI, Request, Depends, UploadFile, File, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy.orm import Session
from . import use_cases
from .db import create_db_and_tables, get_db, ensure_min_schema
from .models import Participant, ParticipantRole, UsageEvent, EventType, Policy
from .settle import apply_policy_and_settle
from typing import List
from collections import defaultdict

# -----------------------------------------------------------------------------
# App & Templates/Static
# -----------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR.parent / "static"
DEMO_DATA_DIR = STATIC_DIR / "demo_data"

app = FastAPI(title="KYDE PoC", debug=True)
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# -----------------------------------------------------------------------------
# Startup: DB Tabellen anlegen
# -----------------------------------------------------------------------------
@app.on_event("startup")
def on_startup():
    print("Creating database tables and ensuring schema...")
    create_db_and_tables()
    ensure_min_schema()
    print("Startup complete.")

# -----------------------------------------------------------------------------
# Helper: CSV robust einlesen
# -----------------------------------------------------------------------------
def read_csv_robust(content: bytes) -> pd.DataFrame:
    """
    Versucht, CSV mit auto-Delimiter, UTF-8/BOM & Fallbacks einzulesen.
    """
    tries = [
        dict(sep=None, engine="python", encoding="utf-8-sig"),
        dict(sep=";", encoding="utf-8-sig"),
        dict(sep=",", encoding="utf-8-sig"),
    ]
    for params in tries:
        try:
            return pd.read_csv(io.BytesIO(content), **params)
        except Exception:
            continue
    text = content.decode("utf-8", errors="ignore")
    return pd.read_csv(io.StringIO(text), sep=None, engine="python")

def to_float_safe(x) -> float:
    """
    Konvertiert Strings mit Komma als Dezimaltrenner nach float.
    Leere/ungültige Werte -> 0.0
    """
    if x is None:
        return 0.0
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if not s:
        return 0.0
    s = s.replace(",", ".")
    try:
        return float(s)
    except (ValueError, TypeError):
        return 0.0

# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
from fastapi.templating import Jinja2Templates  # placed here to avoid circular import above

@app.get("/", response_class=HTMLResponse)
def case_selector(request: Request):
    return templates.TemplateResponse(
        "case_selector.html",
        {"request": request, "title": "Anwendungsfall auswählen"},
    )

@app.get("/upload", response_class=HTMLResponse)
def upload_page(request: Request, case: str = "mieterstrom"):
    if case != "mieterstrom":
        raise HTTPException(status_code=400, detail="Unknown case, only 'mieterstrom' is supported for this demo.")
    
    default_policy = use_cases.get_default_policy(case)
    title = use_cases.get_use_case_title(case)
    
    return templates.TemplateResponse(
        "uploads.html",
        {
            "request": request,
            "title": title,
            "case": case,
            "default_policy_json": json.dumps(default_policy, indent=2, ensure_ascii=False),
        },
    )

@app.post("/process_data", response_class=HTMLResponse)
async def process_data(
    request: Request,
    db: Session = Depends(get_db),
    case: str = Form(...),
    csv_file: UploadFile = File(...),
    policy_json_str: str = Form(...),
):
    try:
        policy_body = json.loads(policy_json_str)
        policy = Policy(use_case=case, body=policy_body)
        db.add(policy)
        db.commit()

        content = await csv_file.read()
        df = read_csv_robust(content)
        
        required_cols = ["timestamp", "participant_id", "participant_name", "role", "event_type", "quantity", "source"]
        missing = [col for col in required_cols if col not in df.columns]
        if missing:
            raise ValueError(f"Fehlende Spalten in der CSV-Datei: {', '.join(missing)}")
        
        # Normalize strings
        df['role'] = df['role'].astype(str).str.strip().str.lower()
        df['event_type'] = df['event_type'].astype(str).str.strip().str.lower()
        df['source'] = df['source'].astype(str).str.strip().str.lower()
        
        unique_participants = df[['participant_id', 'participant_name', 'role']].drop_duplicates()

        # Load existing by external_id
        unique_ids = [str(uid) for uid in unique_participants['participant_id'].unique()]
        existing = {p.external_id: p for p in db.query(Participant).filter(Participant.external_id.in_(unique_ids)).all()}
        
        new_participants_list = []
        participant_map_dict: dict[str, Participant] = {}

        for _, row in unique_participants.iterrows():
            ext_id = str(row['participant_id'])
            name_from_csv = row.get("participant_name", f"Participant {ext_id}")
            role_str = row["role"]
            try:
                role_enum = ParticipantRole(role_str)
            except Exception:
                raise HTTPException(status_code=400, detail=f"Unbekannte Rolle in CSV: '{role_str}' (bei participant_id={ext_id})")

            p = existing.get(ext_id)
            if p:
                # Sync name/role
                if p.name != name_from_csv:
                    p.name = name_from_csv
                    db.add(p)
                if p.role != role_enum:
                    p.role = role_enum
                    db.add(p)
            else:
                p = Participant(
                    external_id=ext_id,
                    name=name_from_csv,
                    role=role_enum
                )
                new_participants_list.append(p)
            participant_map_dict[ext_id] = p
        
        if new_participants_list:
            db.add_all(new_participants_list)
        
        db.flush()  # IDs für neue Teilnehmer

        # Create UsageEvent rows
        usage_events: list[UsageEvent] = []
        for _, row in df.iterrows():
            ext_id = str(row['participant_id'])
            p = participant_map_dict.get(ext_id)
            if not p:
                raise HTTPException(status_code=500, detail=f"Teilnehmer mit ID {ext_id} nicht in der Datenbank gefunden.")

            try:
                et = EventType(row["event_type"])
            except Exception:
                raise HTTPException(status_code=400, detail=f"Unbekannter event_type in CSV: '{row['event_type']}' (participant_id={ext_id})")

            event = UsageEvent(
                participant_id=p.id,
                event_type=et,
                quantity=to_float_safe(row["quantity"]),
                unit=str(row.get("unit", "kWh")),
                timestamp=row["timestamp"],
                meta={
                    "source": row["source"],
                    "price_eur_per_kwh": to_float_safe(row.get("price_eur_per_kwh", 0.0))
                }
            )
            usage_events.append(event)
        
        db.add_all(usage_events)
        db.commit()
        
        # Settlement/Netting
        batch, result_data, netting_stats = apply_policy_and_settle(db, case, policy_body, usage_events)
        
        # Rows (alle Parteien, inkl. Operator/External-Market)
        rows = []
        sum_credit = sum_debit = 0.0
        id_to_participant = {p.id: p for p in db.query(Participant).all()}

        for pid, data in result_data.items():
            p = id_to_participant.get(pid)
            credit = float(data.get('credit', 0.0))
            debit  = float(data.get('debit', 0.0))
            pre_net = credit - debit  # Brutto vor bilateralem Netting
            post_net = float(data.get('final_net', pre_net))
            rows.append({
                "name": p.name if p else f"#{pid}",
                "role": (p.role.value if p else "unknown"),
                "credit_eur": credit,
                "debit_eur": debit,
                "pre_net_eur": pre_net,
                "net_eur": post_net
            })
            sum_credit += credit
            sum_debit += debit

        total_flow = sum_credit + sum_debit
        net_flow_before = sum(abs(r['pre_net_eur']) for r in rows)
        net_flow_after  = sum(abs(r['net_eur']) for r in rows)

        # Transfers (Wer zahlt wem wie viel?)
        transfers_ui = []
        for deb_id, cred_id, amount in netting_stats.get('transfers_list', []):
            deb = id_to_participant.get(deb_id)
            cred = id_to_participant.get(cred_id)
            transfers_ui.append({
                "from": deb.name if deb else f"#{deb_id}",
                "to": cred.name if cred else f"#{cred_id}",
                "amount_eur": round(float(amount), 2)
            })
        
        kpis = {
            'participants': len(rows),
            'sum_credit': sum_credit,
            'sum_debit': sum_debit,
            'gross_exposure': total_flow,
            'netting_efficiency': (1 - (net_flow_after / net_flow_before)) if net_flow_before > 1e-9 else 0,
            'transfers_count': len(transfers_ui),
        }

        # Erklärungen pro Teilnehmer (kurz gefixt: Base-Fee in EUR, Verbrauch PV vs. Netz getrennt)
        netting_explanations = []
        detailed_records = df.to_dict('records')
        by_pid = defaultdict(list)
        for rec in detailed_records:
            by_pid[str(rec['participant_id'])].append(rec)

        for ext_id, plist in by_pid.items():
            p = participant_map_dict.get(ext_id)
            if not p:
                continue
            sums = defaultdict(float)
            for e in plist:
                et = e['event_type']
                src = e['source']
                qty = to_float_safe(e['quantity'])
                if et == 'consumption':
                    key = f"consumption_{src}"
                    sums[key] += qty
                elif et == 'base_fee':
                    sums['base_fee_eur'] += qty
                else:
                    key = f"{et}_{src}"
                    sums[key] += qty

            d = result_data.get(p.id, {'credit': 0.0, 'debit': 0.0, 'final_net': 0.0})
            parts = [f"**{p.name}** ({p.role.value})"]
            if sums.get('base_fee_eur'):
                parts.append(f"• Grundgebühr: {sums['base_fee_eur']:.2f} €.")
            if sums.get('consumption_local_pv'):
                parts.append(f"• Lokaler PV-Bezug: {sums['consumption_local_pv']:.2f} kWh.")
            if sums.get('consumption_grid_external'):
                parts.append(f"• Netzbezug: {sums['consumption_grid_external']:.2f} kWh.")
            credit = d['credit']
            debit = d['debit']
            parts.append(f"Das ergibt eine Gutschrift von {credit:.2f} € und eine Forderung von {debit:.2f} €.")
            fn = d['final_net']
            if fn > 0.01:
                parts.append(f"**Nettosaldo: {fn:.2f} €** (Erhält eine Auszahlung).")
            elif fn < -0.01:
                parts.append(f"**Nettosaldo: {abs(fn):.2f} €** (Schuldet eine Zahlung).")
            else:
                parts.append("**Nettosaldo: 0.00 €** (Der Saldo ist perfekt ausgeglichen).")
            netting_explanations.append(" ".join(parts))

        df_for_template = df.to_dict('records')

        return templates.TemplateResponse(
            "results.html",
            {
                "request": request,
                "title": f"Ergebnis: {use_cases.get_use_case_title(case)}",
                "batch_id": batch.id,
                "rows": sorted(rows, key=lambda x: x['pre_net_eur'], reverse=True),
                "kpis": kpis,
                "events_raw_data": df_for_template,
                "netting_explanations": netting_explanations,
                "transfers": transfers_ui
            },
        )
    except Exception as e:
        error_details = traceback.format_exc()
        return templates.TemplateResponse(
            "results.html",
            {
                "request": request,
                "title": "Verarbeitung fehlgeschlagen",
                "error": error_details,
            },
            status_code=500,
        )
