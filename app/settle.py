# FIX: Fehlende Imports hinzugefügt
from __future__ import annotations
from pathlib import Path
from typing import List, Optional
from datetime import datetime, timedelta
from collections import defaultdict
import random

from fastapi import FastAPI, Request, Depends, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel

from sqlalchemy.orm import Session

from .db import ensure_min_schema, get_db
from .models import Participant, ParticipantRole, UsageEvent, EventType
from .audit import get_audit_payload

# ---------- App / Templates ----------
BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR.parent / "static"

app = FastAPI(title="KYDE PoC", debug=True)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# ---------- Pydantic Schemas ----------
class EventPayload(BaseModel):
    participant_id: str
    event_type: EventType
    quantity: float
    unit: str
    timestamp: datetime
    source: str
    price_eur_per_kwh: Optional[float] = 0.0

class NettingPreviewPayload(BaseModel):
    use_case: str
    policy_body: dict
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    community_id: Optional[str] = None

class SettlePayload(BaseModel):
    use_case: str
    policy_body: dict
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    community_id: Optional[str] = None

# ---------- Startup ----------
@app.on_event("startup")
def on_startup():
    ensure_min_schema()

# ---------- Routes (HTML) ----------
@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/demo/api-dashboard", response_class=HTMLResponse)
def get_api_dashboard(request: Request):
    return templates.TemplateResponse("api_dashboard.html", {"request": request})
    
@app.get("/demo/poc-dashboard", response_class=HTMLResponse)
def get_poc_dashboard(request: Request):
    # Route für das neue Dashboard
    return templates.TemplateResponse("poc_dashboard.html", {"request": request})

# ---------- API ----------
@app.post("/v1/energy-events", status_code=201)
def ingest_energy_events(events: List[EventPayload], db: Session = Depends(get_db)):
    try:
        existing = {p.external_id: p for p in db.query(Participant).all()}
        new_participants = []

        for ev in events:
            ext_id = ev.participant_id
            if ext_id not in existing:
                p = Participant(
                    external_id=ext_id,
                    name=f"Participant {ext_id}",
                    role=ParticipantRole.prosumer
                )
                db.add(p)
                new_participants.append(p)
                existing[ext_id] = p

        if new_participants:
            db.flush()

        rows: list[UsageEvent] = []
        for ev in events:
            p = existing[ev.participant_id]
            rows.append(UsageEvent(
                participant_id=p.id,
                event_type=ev.event_type,
                quantity=ev.quantity,
                unit=ev.unit,
                timestamp=ev.timestamp,
                meta={"source": ev.source, "price_eur_per_kwh": ev.price_eur_per_kwh or 0.0}
            ))
        db.add_all(rows)
        db.commit()
        return {"status": "success", "message": f"Ingested {len(rows)} events."}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/v1/netting/preview", response_class=JSONResponse)
def netting_preview(payload: NettingPreviewPayload, db: Session = Depends(get_db)):
    try:
        start = payload.start_time or (datetime.utcnow() - timedelta(days=2))
        end = payload.end_time or datetime.utcnow()
        events = db.query(UsageEvent).filter(UsageEvent.timestamp.between(start, end)).all()
        if not events:
            return JSONResponse(status_code=200, content={"message": "No events found in the specified timeframe."})
        ids = [e.participant_id for e in events]
        participants = {p.id: p for p in db.query(Participant).filter(Participant.id.in_(ids)).all()}
        balances = defaultdict(lambda: {"credit": 0.0, "debit": 0.0})
        for ev in events:
            p = participants.get(ev.participant_id)
            if not p: continue
            qty = float(ev.quantity or 0.0)
            price = float((ev.meta or {}).get("price_eur_per_kwh") or 0.0)
            if ev.event_type.value in ("consumption", "base_fee"):
                balances[p.id]["debit"] += qty * price
            elif ev.event_type.value in ("generation", "grid_feed", "vpp_sale"):
                balances[p.id]["credit"] += qty * price
        final_balances, stats, transfers = apply_bilateral_netting(balances, payload.policy_body)
        content = {
            "stats": stats,
            "transfers": transfers,
            "final_balances": {
                participants[pid].external_id: round(val, 2)
                for pid, val in final_balances.items() if abs(val) > 0.01
            }
        }
        return JSONResponse(content=content)
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/v1/settle/execute", response_class=JSONResponse)
def execute_settlement(payload: SettlePayload, db: Session = Depends(get_db)):
    try:
        start = payload.start_time or (datetime.utcnow() - timedelta(days=2))
        end = payload.end_time or datetime.utcnow()
        events = db.query(UsageEvent).filter(UsageEvent.timestamp.between(start, end)).all()
        if not events:
            return JSONResponse(status_code=200, content={"message": "No events found to settle."})
        batch, result_data, _ = apply_policy_and_settle(
            db, payload.use_case, payload.policy_body, events, start_time=start, end_time=end
        )
        pid_map = {p.id: p for p in db.query(Participant).filter(Participant.id.in_(result_data.keys())).all()}
        final_net = {pid_map[i].external_id: round(d["final_net"], 2) for i, d in result_data.items()}
        return JSONResponse(content={
            "status": "success",
            "batch_id": batch.id,
            "message": "Settlement executed and proofs generated.",
            "final_net_balances": final_net
        })
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/v1/audit/{batch_id}", response_class=JSONResponse)
def audit_batch(batch_id: int, explain: bool = False, db: Session = Depends(get_db)):
    try:
        return JSONResponse(content=get_audit_payload(db, batch_id, explain))
    except HTTPException as he:
        raise he
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

# ---------- Neuer PoC-Endpunkt ----------

# FIX: Fehlende Helper-Funktion hinzugefügt
def generate_dummy_escooter_events(count: int) -> list[dict]:
    participants = ["Alice", "Bob", "Clara", "Operator-X", "City-Y", "Provider-Z"]
    events = []
    for _ in range(count):
        p = random.choice(participants)
        amount = round(random.uniform(0.5, 5.0), 2)
        # Sorge dafür, dass Betreiber und Städte eher Geld erhalten
        if "Operator" in p or "City" in p or "Provider" in p:
            amount = -amount
        events.append({"participant_id": p, "amount": amount})
    return events

class PocDemoPayload(BaseModel):
    transaction_count: int = 500
    fee_per_transaction_eur: float = 0.30

@app.post("/v1/poc/run-demo")
def run_poc_demo(payload: PocDemoPayload, db: Session = Depends(get_db)):
    raw_transactions = generate_dummy_escooter_events(payload.transaction_count)
    gross_volume = sum(abs(t['amount']) for t in raw_transactions)
    estimated_fees = payload.transaction_count * payload.fee_per_transaction_eur
    
    balances = defaultdict(float)
    for t in raw_transactions:
        balances[t['participant_id']] += t['amount']
    
    netted_payouts = {pid: round(amount, 2) for pid, amount in balances.items() if abs(amount) > 0.01}
    netted_transaction_count = len(netted_payouts)
    actual_fees = netted_transaction_count * payload.fee_per_transaction_eur

    return {
        "before": {
            "transaction_stream": raw_transactions[:20],
            "metrics": {
                "total_transactions": payload.transaction_count,
                "gross_volume_eur": round(gross_volume, 2),
                "estimated_fees_eur": round(estimated_fees, 2)
            }
        },
        "after": {
            "netted_payouts": netted_payouts,
             "metrics": {
                "netted_transactions": netted_transaction_count,
                "actual_fees_eur": round(actual_fees, 2),
                "savings_eur": round(estimated_fees - actual_fees, 2)
            }
        },
        "api_proof": {
            "request_body_snippet": {"events": raw_transactions[:2]},
            "response_body": {"batch_id": random.randint(100,200), "final_net_balances": netted_payouts}
        }
    }