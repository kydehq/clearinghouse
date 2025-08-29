# app/main.py
from __future__ import annotations
import io
import json
import traceback
from pathlib import Path
import pandas as pd
from fastapi import FastAPI, Request, Depends, UploadFile, File, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session, joinedload
from . import use_cases
from .db import create_db_and_tables, get_db, ensure_min_schema
from .models import Participant, ParticipantRole, UsageEvent, EventType, Policy
from .settle import apply_policy_and_settle
from typing import List

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
@app.get("/", response_class=HTMLResponse)
def case_selector(request: Request):
    return templates.TemplateResponse(
        "case_selector.html",
        {"request": request, "title": "Anwendungsfall auswählen"},
    )

@app.get("/upload", response_class=HTMLResponse)
def upload_page(request: Request, case: str = "mieterstrom"):
    if case not in ("mieterstrom"):
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
        if not all(col in df.columns for col in required_cols):
            missing = [col for col in required_cols if col not in df.columns]
            raise ValueError(f"Fehlende Spalten in der CSV-Datei: {', '.join(missing)}")
        
        df['role'] = df['role'].str.strip().str.lower()
        df['event_type'] = df['event_type'].str.strip().str.lower()
        df['source'] = df['source'].str.strip().str.lower()
        
        unique_participants = df[['participant_id', 'participant_name', 'role']].drop_duplicates()
        
        # Holen Sie alle Teilnehmer, die in der CSV vorkommen, aus der DB
        unique_ids = [str(uid) for uid in unique_participants['participant_id'].unique()]
        existing_participants = {p.external_id: p for p in db.query(Participant).filter(Participant.external_id.in_(unique_ids)).all()}
        
        new_participants_list = []
        participant_map_dict = {}

        for index, row in unique_participants.iterrows():
            ext_id = str(row['participant_id'])
            name_from_csv = row.get("participant_name", f"Participant {ext_id}")
            role_from_csv = ParticipantRole(row["role"])
            
            p = existing_participants.get(ext_id)
            if p:
                # Synchronisiere den Namen und die Rolle aus der CSV
                if p.name != name_from_csv:
                    p.name = name_from_csv
                    db.add(p)
                if p.role != role_from_csv:
                    p.role = role_from_csv
                    db.add(p)
            else:
                p = Participant(
                    external_id=ext_id,
                    name=name_from_csv,
                    role=role_from_csv
                )
                new_participants_list.append(p)
            
            participant_map_dict[ext_id] = p
        
        if new_participants_list:
            db.add_all(new_participants_list)
        
        db.flush() # Nötig, um IDs für neue Teilnehmer zu erhalten
        
        # Nun können wir die Events hinzufügen, da alle Teilnehmer-IDs bekannt sind
        usage_events = []
        for _, row in df.iterrows():
            ext_id = str(row['participant_id'])
            p = participant_map_dict.get(ext_id)
            if not p:
                raise HTTPException(status_code=500, detail=f"Teilnehmer mit ID {ext_id} nicht in der Datenbank gefunden.")

            event = UsageEvent(
                participant_id=p.id,
                event_type=EventType(row["event_type"]),
                quantity=to_float_safe(row["quantity"]),
                unit=row.get("unit", "kWh"),
                timestamp=row["timestamp"],
                meta={
                    "source": row["source"],
                    "price_eur_per_kwh": to_float_safe(row.get("price_eur_per_kwh", 0.0))
                }
            )
            usage_events.append(event)
        
        db.add_all(usage_events)
        db.commit()
        
        # Netting-Algorithmus ausführen
        batch, result_data = apply_policy_and_settle(db, case, policy_body, usage_events)
        
        # NEU: Iteriere über alle Teilnehmer aus der CSV, nicht nur die, die am Netting beteiligt waren
        rows = []
        sum_credit, sum_debit = 0.0, 0.0
        
        for index, row in unique_participants.iterrows():
            ext_id = str(row['participant_id'])
            p = participant_map_dict.get(ext_id)
            
            # Hole die Salden aus dem Ergebnis oder setze sie auf 0, falls der Teilnehmer keinen Geldfluss hatte
            data = result_data.get(p.id, {'credit': 0.0, 'debit': 0.0, 'final_net': 0.0})

            credit = data.get('credit', 0.0)
            debit = data.get('debit', 0.0)
            rows.append({
                "name": p.name,
                "role": p.role.value,
                "credit_eur": credit,
                "debit_eur": debit,
                "net_eur": data.get('final_net', credit - debit)
            })
            sum_credit += credit
            sum_debit += debit
        
        total_flow = sum_credit + sum_debit
        net_flow = sum(abs(r['net_eur']) for r in rows)
        
        kpis = {
            'participants': len(rows),
            'sum_credit': sum_credit,
            'sum_debit': sum_debit,
            'netting_efficiency': (1 - (net_flow / total_flow)) if total_flow > 0 else 0
        }

        # NEU: Detaillierte Erklärungen basierend auf den Rohdaten
        netting_explanations = []
        raw_events_by_participant = df.groupby('participant_id')

        for index, row in unique_participants.iterrows():
            p_id = row['participant_id']
            name = row['participant_name']
            
            if p_id in raw_events_by_participant.groups:
                p_events = raw_events_by_participant.get_group(p_id)
                summary = defaultdict(lambda: {'total_quantity': 0.0, 'total_eur': 0.0, 'type': '', 'source': ''})
                
                # Gruppiere die Events pro Teilnehmer nach Event-Typ und Quelle
                for _, event_row in p_events.iterrows():
                    event_type = event_row['event_type']
                    source = event_row['source']
                    quantity = to_float_safe(event_row['quantity'])
                    price = to_float_safe(event_row.get('price_eur_per_kwh', 0.0))
                    
                    key = f"{event_type}_{source}"
                    summary[key]['total_quantity'] += quantity
                    summary[key]['type'] = event_type
                    summary[key]['source'] = source
                    
                    # Berechne den Geldfluss
                    if event_type == 'consumption':
                        summary[key]['total_eur'] += quantity * price
                    elif event_type == 'production' and source == 'solar':
                        # Verwende den Policy-Preis, nicht den CSV-Preis
                        solar_price = float(policy_body.get('solar_production_price', 0.18))
                        summary[key]['total_eur'] += quantity * solar_price
                    elif event_type == 'vpp_sale':
                         vpp_price = float(policy_body.get('vpp_sale_price', 0.09))
                         summary[key]['total_eur'] += quantity * vpp_price
                    elif event_type == 'grid_feed':
                         grid_feed_price = float(policy_body.get('grid_compensation', 0.08))
                         summary[key]['total_eur'] += quantity * grid_feed_price
                
                
                explanations = [f"**{name}** hatte folgende Aktivität:"]
                
                for key, data in summary.items():
                    event_type = data['type']
                    source = data['source']
                    quantity = data['total_quantity']
                    total_eur = data['total_eur']
                    
                    if event_type == 'consumption':
                        explanations.append(f"• Bezug von {quantity:.2f} kWh ({source}) zu einem Wert von {total_eur:.2f} €.")
                    elif event_type == 'production':
                        explanations.append(f"• Produktion von {quantity:.2f} kWh ({source}), was eine Gutschrift von {total_eur:.2f} € generierte.")
                    elif event_type == 'battery_charge':
                        explanations.append(f"• Die Batterie wurde um {quantity:.2f} kWh ({source}) aufgeladen.")
                    elif event_type == 'vpp_sale':
                        explanations.append(f"• Verkauf von {quantity:.2f} kWh ({source}) an den Markt, was eine Gutschrift von {total_eur:.2f} € generierte.")
                    elif event_type == 'grid_feed':
                        explanations.append(f"• Einspeisung von {quantity:.2f} kWh ({source}) ins Netz, was eine Gutschrift von {total_eur:.2f} € generierte.")

                # Zeige Nettosaldo am Ende der Erklärung
                net_balance = result_data.get(participant_map_dict.get(p_id).id, {}).get('final_net', 0.0)
                if net_balance > 0.01:
                    explanations.append(f"**Nettosaldo: {net_balance:.2f} €** (Erhält eine Auszahlung).")
                elif net_balance < -0.01:
                    explanations.append(f"**Nettosaldo: {abs(net_balance):.2f} €** (Schuldet eine Zahlung).")
                else:
                    explanations.append("**Nettosaldo: 0.00 €** (Der Saldo ist perfekt ausgeglichen).")

                netting_explanations.append(" ".join(explanations))
            
        df_for_template = df.to_dict('records')

        return templates.TemplateResponse(
            "results.html",
            {
                "request": request,
                "title": f"Ergebnis: {use_cases.get_use_case_title(case)}",
                "batch_id": batch.id,
                "rows": sorted(rows, key=lambda x: x['net_eur'], reverse=True),
                "kpis": kpis,
                "events_raw_data": df_for_template, 
                "netting_explanations": netting_explanations
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