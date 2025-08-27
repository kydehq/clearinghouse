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
from sqlalchemy.orm import Session

from . import models, settle, use_cases, audit
from .db import create_db_and_tables, get_db, ensure_min_schema
from .models import Participant, ParticipantRole, UsageEvent, EventType, Policy

# -----------------------------------------------------------------------------
# App & Templates/Static
# -----------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = BASE_DIR / "templates"
# Das static-Verzeichnis liegt jetzt im Hauptverzeichnis, nicht mehr in app
STATIC_DIR = BASE_DIR.parent / "static" 
DEMO_DATA_DIR = STATIC_DIR / "demo_data"

app = FastAPI(title="KYDE PoC", debug=True)

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# Mount static files (CSS, JS, Demo-Daten)
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
    if x is None: return 0.0
    if isinstance(x, (int, float)): return float(x)
    s = str(x).strip().replace(",", ".")
    if not s: return 0.0
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
def upload_page(request: Request, case: str = "energy_community"):
    try:
        default_policy = use_cases.get_default_policy(case)
        title = use_cases.get_use_case_title(case)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

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
        # 1. Policy validieren und speichern
        policy_body = json.loads(policy_json_str)
        policy = Policy(use_case=case, body=policy_body)
        db.add(policy)
        db.commit()

        # 2. CSV einlesen und verarbeiten
        content = await csv_file.read()
        df = read_csv_robust(content)
        
        # 3. Teilnehmer und Events in DB anlegen
        participants = {}
        usage_events = []
        for _, row in df.iterrows():
            ext_id = str(row["participant_id"]).strip()
            if ext_id not in participants:
                p = db.query(Participant).filter_by(external_id=ext_id).first()
                if not p:
                    p = Participant(
                        external_id=ext_id,
                        name=row.get("participant_name", f"Participant {ext_id}"),
                        role=ParticipantRole(str(row["role"]).strip().lower()),
                    )
                    db.add(p)
                    db.flush()
                participants[ext_id] = p
            
            event = UsageEvent(
                participant_id=participants[ext_id].id,
                event_type=EventType(str(row["event_type"]).strip().lower()),
                quantity=to_float_safe(row["quantity"]),
                unit=row.get("unit", "kWh"),
            )
            usage_events.append(event)
        
        db.add_all(usage_events)
        db.commit()
        
        # 4. Settlement-Logik anwenden
        batch, result_data = settle.apply_policy_and_settle(db, case, policy_body, usage_events)
        
        # 5. KPIs und Ergebnis-Daten für das Template aufbereiten
        rows = []
        sum_credit, sum_debit = 0.0, 0.0
        for pid, data in result_data.items():
            participant = db.query(Participant).get(pid)
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

        return templates.TemplateResponse(
            "results.html",
            {
                "request": request,
                "title": f"Ergebnis: {use_cases.get_use_case_title(case)}",
                "batch_id": batch.id,
                "rows": sorted(rows, key=lambda x: x['net_eur'], reverse=True),
                "kpis": kpis,
            },
        )
    except Exception as e:
        # Bei Fehlern eine detaillierte Fehlerseite anzeigen
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