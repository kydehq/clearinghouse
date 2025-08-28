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
        existing_participants = {p.external_id: p for p in db.query(Participant).filter(Participant.external_id.in_(unique_participants['participant_id'])).all()}
        
        new_participants = []
        participant_map = {}
        for _, row in unique_participants.iterrows():
            ext_id = str(row['participant_id'])
            if ext_id not in existing_participants:
                p = Participant(
                    external_id=ext_id,
                    name=row.get("participant_name", f"Participant {ext_id}"),
                    role=ParticipantRole(row["role"]),
                )
                new_participants.append(p)
                participant_map[ext_id] = p
            else:
                participant_map[ext_id] = existing_participants[ext_id]
        
        if new_participants:
            db.add_all(new_participants)
            db.flush()
            db.commit()

        usage_events = []
        for _, row in df.iterrows():
            ext_id = str(row['participant_id'])
            p_id = participant_map[ext_id].id
            
            event = UsageEvent(
                participant_id=p_id,
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
        
        batch, result_data = apply_policy_and_settle(db, case, policy_body, usage_events)
        
        rows = []
        sum_credit, sum_debit = 0.0, 0.0
        for pid, data in result_data.items():
            participant = db.query(Participant).get(pid)
            if not participant: continue 
            
            credit = data.get('credit', 0.0)
            debit = data.get('debit', 0.0)
            rows.append({
                "name": participant.name,
                "role": participant.role.value,
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

        # Konvertiere das DataFrame in eine Liste von Dictionaries für das Template
        df_for_template = df.to_dict('records')

        return templates.TemplateResponse(
            "results.html",
            {
                "request": request,
                "title": f"Ergebnis: {use_cases.get_use_case_title(case)}",
                "batch_id": batch.id,
                "rows": sorted(rows, key=lambda x: x['net_eur'], reverse=True),
                "kpis": kpis,
                "events_raw_data": df_for_template, # <-- Die Rohdaten werden jetzt hier übergeben
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

@app.get("/audit", response_class=HTMLResponse)
def get_audit_trail(request: Request, batch_id: int, db: Session = Depends(get_db)):
    audit_data = audit.get_audit_data(db, batch_id)
    return templates.TemplateResponse(
        "audit.html",
        {"request": request, **audit_data}
    )