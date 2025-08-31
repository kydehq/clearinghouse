from __future__ import annotations
from pathlib import Path
from typing import List, Optional, Dict, Any
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
from .settle import apply_policy_and_settle, apply_bilateral_netting
from .audit import get_audit_payload

# ---------- App / Templates ----------
BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR.parent / "static"

app = FastAPI(title="KYDE PoC", debug=False)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))

# ---------- Health ----------
@app.get("/healthz")
def healthz():
    return {"ok": True}

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
    policy_body: Dict[str, Any]
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    community_id: Optional[str] = None

class SettlePayload(BaseModel):
    use_case: str
    policy_body: Dict[str, Any]
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    community_id: Optional[str] = None

# Realistischer PoC-Body
class PocDemoPayload(BaseModel):
    transaction_count: int = 500           # Anzahl Rides/Bookings im Stream
    fee_per_transaction_eur: float = 0.30  # Zahlungsgebühr pro Auszahlung
    scenario: str = "mixed"                # "scooter" | "car" | "mixed"
    riders: int = 400                      # unterschiedliche Nutzer:innen
    operators: int = 2                     # Anzahl Betreiber
    cities: int = 1                        # Anzahl Kommunen

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
        events = db.query(UsageEvent).filter(
            UsageEvent.timestamp >= start,
            UsageEvent.timestamp < end
        ).all()
        if not events:
            return JSONResponse(status_code=200, content={"message": "No events found in the specified timeframe."})
        ids = [e.participant_id for e in events]
        participants = {p.id: p for p in db.query(Participant).filter(Participant.id.in_(ids)).all()}
        balances = defaultdict(lambda: {"credit": 0.0, "debit": 0.0})
        for ev in events:
            p = participants.get(ev.participant_id)
            if not p:
                continue
            qty = float(ev.quantity or 0.0)
            price = float((ev.meta or {}).get("price_eur_per_kwh") or 0.0)
            if ev.event_type.value in ("consumption", "base_fee"):
                amount = qty if (ev.unit or "").lower() in ("eur", "") else qty * price
                balances[p.id]["debit"] += amount
            elif ev.event_type.value in ("generation", "grid_feed", "vpp_sale"):
                amount = qty if (ev.unit or "").lower() == "eur" else qty * price
                balances[p.id]["credit"] += amount
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
        events = db.query(UsageEvent).filter(
            UsageEvent.timestamp >= start,
            UsageEvent.timestamp < end
        ).all()
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

# ---------- PoC Endpoint (realistisch) ----------
def _fare_for(mode: str, rng: random.Random) -> float:
    if mode == "car":
        return round(rng.uniform(8.0, 30.0), 2)
    if mode == "scooter":
        return round(rng.uniform(1.0, 5.0), 2)
    # mixed
    roll = rng.random()
    if roll < 0.6:    # scooter
        return round(rng.uniform(1.0, 5.0), 2)
    elif roll < 0.9:  # car
        return round(rng.uniform(8.0, 30.0), 2)
    else:             # charge/misc
        return round(rng.uniform(0.5, 3.0), 2)

@app.post("/v1/poc/run-demo")
def run_poc_demo(payload: PocDemoPayload, db: Session = Depends(get_db)):
    """
    Erzeugt viele unterschiedliche Rider (z.B. 400), verteilt die Transaktionen,
    und rechnet am Ende typische Anteile an Betreiber/City zu.
    -> Netted Payouts umfassen realistisch viele Empfänger:innen.
    """
    rng = random.Random(42)  # deterministisch für Demos
    riders = [f"Rider-{i:03d}" for i in range(1, max(1, payload.riders) + 1)]
    operators = [f"Operator-{chr(65+i)}" for i in range(max(1, payload.operators))]
    cities = [f"City-{i+1}" for i in range(max(1, payload.cities))]

    # Simulation
    raw_transactions: List[Dict[str, Any]] = []
    balances: Dict[str, float] = defaultdict(float)
    gross_volume = 0.0

    # pro Transaktion: Rider zahlt Fahrpreis; Verteilung: Operator-Share, City-Share
    # einfache Heuristik je Modus:
    if payload.scenario == "car":
        operator_share, city_share = 0.92, 0.08
    elif payload.scenario == "scooter":
        operator_share, city_share = 0.85, 0.15
    else:  # mixed
        operator_share, city_share = 0.9, 0.1

    for _ in range(max(1, payload.transaction_count)):
        rider = rng.choice(riders)
        fare = _fare_for(payload.scenario, rng)

        # Rider zahlt (positiv)
        balances[rider] += fare
        raw_transactions.append({"participant_id": rider, "amount": fare})

        gross_volume += abs(fare)

    # Verteilen auf Operatoren / Cities (negativ = erhalten)
    total_rider_sum = sum(v for k, v in balances.items() if k.startswith("Rider-"))
    op_total = -(total_rider_sum * operator_share)
    city_total = -(total_rider_sum * city_share)

    # auf mehrere Operatoren/Kommunen verteilen
    for i, op in enumerate(operators):
        share = op_total * ((i + 1) / sum(range(1, len(operators) + 1)))  # simple Gewichtung
        balances[op] += share
    for j, ct in enumerate(cities):
        share = city_total * ((j + 1) / sum(range(1, len(cities) + 1)))
        balances[ct] += share

    # Ergebnis zusammenstellen
    netted_payouts = {pid: round(amt, 2) for pid, amt in balances.items() if abs(amt) > 0.01}
    netted_transaction_count = len(netted_payouts)  # viele Empfänger:innen (Rider + Operator + City)

    estimated_fees = payload.transaction_count * payload.fee_per_transaction_eur
    actual_fees = netted_transaction_count * payload.fee_per_transaction_eur
    compression_ratio = round(payload.transaction_count / max(1, netted_transaction_count), 2)

    return {
        "before": {
            "transaction_stream": raw_transactions[:50],  # kurzer Ausschnitt
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
                "savings_eur": round(estimated_fees - actual_fees, 2),
                "compression_ratio": compression_ratio
            }
        },
        "api_proof": {
            "request_body_snippet": {
                "transaction_count": payload.transaction_count,
                "fee_per_transaction_eur": payload.fee_per_transaction_eur,
                "scenario": payload.scenario,
                "riders": payload.riders,
                "operators": payload.operators,
                "cities": payload.cities
            },
            "response_body": {
                "final_net_balances": netted_payouts
            }
        }
    }
