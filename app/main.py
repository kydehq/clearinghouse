# app/main.py
from __future__ import annotations
from fastapi import FastAPI, Request, Depends, UploadFile, File, Form, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from pathlib import Path
import pandas as pd
import json, io

from .db import create_db_and_tables, get_db
from .models import Participant, ParticipantRole, UsageEvent, EventType, Policy, SettlementBatch, SettlementLine
from .settle import apply_policy_and_settle

BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"

app = FastAPI(title="KYDE PoC")

if not TEMPLATES_DIR.exists():
    TEMPLATES_DIR = Path("templates")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

@app.on_event("startup")
def on_startup():
    create_db_and_tables()

@app.get("/", response_class=HTMLResponse)
def case_selector(request: Request):
    return templates.TemplateResponse("case_selector.html", {"request": request, "title": "Anwendungsfall auswählen"})

@app.get("/upload", response_class=HTMLResponse)
def upload_page(request: Request, case: str = "energy_community"):
    if case not in ("energy_community", "mieterstrom"):
        raise HTTPException(status_code=400, detail="Unknown case")
    default_policy = {
        "energy_community": {
            "use_case": "energy_community",
            "prosumer_sell_price": 0.15,
            "consumer_buy_price": 0.12,
            "community_fee_rate": 0.02,
            "grid_feed_price": 0.08
        },
        "mieterstrom": {
            "use_case": "mieterstrom",
            "tenant_price_per_kwh": 0.18,
            "landlord_revenue_share": 0.60,
            "operator_fee_rate": 0.15,
            "grid_compensation": 0.08,
            "base_fee_per_unit": 5.00
        }
    }[case]
    return templates.TemplateResponse("uploads.html", {
        "request": request,
        "title": f"{case} – CSV & Policy",
        "case": case,
        "default_policy_json": json.dumps(default_policy, indent=2, ensure_ascii=False)
    })

@app.post("/process_data")
def process_data(
    request: Request,
    csv_file: UploadFile = File(...),
    policy_json_str: str = Form(...),
    case: str = Form(...),
    db: Session = Depends(get_db),
):
    try:
        policy = json.loads(policy_json_str)
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=400, detail=f"Policy JSON ungültig: {e}")

    if case not in ("energy_community", "mieterstrom"):
        raise HTTPException(status_code=400, detail="Unknown case")

    content = csv_file.file.read()
    try:
        df = pd.read_csv(io.BytesIO(content))
    except Exception:
        csv_file.file.seek(0)
        text = csv_file.file.read().decode("utf-8")
        df = pd.read_csv(io.StringIO(text))

    # Minimal: name, role; optional: generation_kwh, grid_feed_kwh, consumption_kwh, base_fee_eur
    if "name" not in df.columns or "role" not in df.columns:
        raise HTTPException(status_code=400, detail="CSV muss Spalten 'name' und 'role' enthalten.")
    if not any(c in df.columns for c in ["generation_kwh","grid_feed_kwh","consumption_kwh","base_fee_eur"]):
        raise HTTPException(status_code=400, detail="CSV braucht mind. eine Mess-Spalte (generation_kwh / grid_feed_kwh / consumption_kwh / base_fee_eur).")

    events = []
    for _, row in df.iterrows():
        role_str = str(row["role"]).strip().lower()
        try:
            role = ParticipantRole(role_str)
        except Exception:
            raise HTTPException(status_code=400, detail=f"Unbekannte Rolle: {role_str}")
        name = str(row["name"]).strip()

        p = db.query(Participant).filter_by(name=name, role=role).first()
        if not p:
            p = Participant(name=name, role=role)
            db.add(p)
            db.flush()

        def add_event(kind: EventType, qty: float, unit: str):
            ev = UsageEvent(participant_id=p.id, event_type=kind, quantity=float(qty), unit=unit)
            events.append(ev)

        if "generation_kwh" in df.columns and pd.notna(row.get("generation_kwh")) and float(row.get("generation_kwh",0)) != 0:
            add_event(EventType.GENERATION, row["generation_kwh"], "kWh")
        if "grid_feed_kwh" in df.columns and pd.notna(row.get("grid_feed_kwh")) and float(row.get("grid_feed_kwh",0)) != 0:
            add_event(EventType.GRID_FEED, row["grid_feed_kwh"], "kWh")
        if "consumption_kwh" in df.columns and pd.notna(row.get("consumption_kwh")) and float(row.get("consumption_kwh",0)) != 0:
            add_event(EventType.CONSUMPTION, row["consumption_kwh"], "kWh")
        if "base_fee_eur" in df.columns and pd.notna(row.get("base_fee_eur")) and float(row.get("base_fee_eur",0)) != 0:
            add_event(EventType.BASE_FEE, row["base_fee_eur"], "EUR")

    db.add_all(events)
    pol = Policy(use_case=case, body=policy)
    db.add(pol)
    db.commit()

    evs = db.query(UsageEvent).join(Participant).all()
    batch, per_participant = apply_policy_and_settle(db, case, policy, evs)

    totals = {
        "participants": len(per_participant),
        "sum_credit": round(sum(v['credit'] for v in per_participant.values()), 2),
        "sum_debit": round(sum(v['debit'] for v in per_participant.values()), 2),
        "sum_abs_before_net": round(sum(abs(v['credit']) + abs(v['debit']) for v in per_participant.values()), 2),
        "sum_abs_after_net": round(sum(abs(v['net']) for v in per_participant.values()), 2),
    }
    totals["netting_efficiency"] = round(1 - (totals["sum_abs_after_net"]/totals["sum_abs_before_net"]), 2) if totals["sum_abs_before_net"] else 0.0

    rows = []
    for pid, v in per_participant.items():
        p = db.get(Participant, pid)
        rows.append({
            "name": p.name,
            "role": p.role.value,
            "credit_eur": round(v['credit'], 2),
            "debit_eur": round(v['debit'], 2),
            "net_eur": round(v['net'], 2),
        })
    rows = sorted(rows, key=lambda r: r["net_eur"], reverse=True)

    return templates.TemplateResponse("results.html", {
        "request": request,
        "title": "Ergebnisse",
        "case": case,
        "batch_id": batch.id,
        "kpis": totals,
        "rows": rows,
    })

@app.get("/audit", response_class=HTMLResponse)
def audit_page(request: Request, batch_id: int, db: Session = Depends(get_db)):
    batch = db.get(SettlementBatch, batch_id)
    if not batch:
        raise HTTPException(status_code=404, detail="Batch nicht gefunden")
    lines = db.query(SettlementLine).filter(SettlementLine.batch_id==batch_id).all()
    data = []
    for ln in lines:
        p = db.get(Participant, ln.participant_id)
        data.append({
            "name": p.name,
            "role": p.role.value,
            "amount_eur": round(ln.amount_eur, 2),
            "description": ln.description,
        })
    return templates.TemplateResponse("results.html", {
        "request": request,
        "title": f"Audit Batch #{batch_id}",
        "case": batch.use_case,
        "batch_id": batch.id,
        "kpis": None,
        "rows": data,
    })
