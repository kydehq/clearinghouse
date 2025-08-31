from __future__ import annotations
from pathlib import Path
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP, ROUND_HALF_EVEN
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

class PocDemoPayload(BaseModel):
    transaction_count: int = 500
    # Backwards-compat optional fields
    fee_per_transaction_eur: Optional[float] = None
    scenario: Optional[str] = None
    riders: Optional[int] = None
    operators: Optional[int] = None
    cities: Optional[int] = None
    policy_body: Optional[Dict[str, Any]] = None

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

# ---------- API (bestehend, unverändert bis hier) ----------
@app.post("/v1/energy-events", status_code=201)
def ingest_energy_events(events: List[EventPayload], db: Session = Depends(get_db)):
    try:
        existing = {p.external_id: p for p in db.query(Participant).all()}
        new_participants = []
        for ev in events:
            ext_id = ev.participant_id
            if ext_id not in existing:
                p = Participant(external_id=ext_id, name=f"Participant {ext_id}", role=ParticipantRole.prosumer)
                db.add(p); new_participants.append(p); existing[ext_id] = p
        if new_participants: db.flush()
        rows: list[UsageEvent] = []
        for ev in events:
            p = existing[ev.participant_id]
            rows.append(UsageEvent(
                participant_id=p.id, event_type=ev.event_type, quantity=ev.quantity, unit=ev.unit,
                timestamp=ev.timestamp, meta={"source": ev.source, "price_eur_per_kwh": ev.price_eur_per_kwh or 0.0}
            ))
        db.add_all(rows); db.commit()
        return {"status": "success", "message": f"Ingested {len(rows)} events."}
    except Exception as e:
        db.rollback(); raise HTTPException(status_code=400, detail=str(e))

@app.post("/v1/netting/preview", response_class=JSONResponse)
def netting_preview(payload: NettingPreviewPayload, db: Session = Depends(get_db)):
    try:
        start = payload.start_time or (datetime.utcnow() - timedelta(days=2))
        end = payload.end_time or datetime.utcnow()
        events = db.query(UsageEvent).filter(UsageEvent.timestamp >= start, UsageEvent.timestamp < end).all()
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
                amount = qty if (ev.unit or "").lower() in ("eur", "") else qty * price
                balances[p.id]["debit"] += amount
            elif ev.event_type.value in ("generation", "grid_feed", "vpp_sale"):
                amount = qty if (ev.unit or "").lower() == "eur" else qty * price
                balances[p.id]["credit"] += amount
        final_balances, stats, transfers = apply_bilateral_netting(balances, payload.policy_body)
        content = {
            "stats": stats,
            "transfers": transfers,
            "final_balances": { participants[pid].external_id: round(val, 2)
                                for pid, val in final_balances.items() if abs(val) > 0.01 }
        }
        return JSONResponse(content=content)
    except Exception as e:
        db.rollback(); raise HTTPException(status_code=500, detail=str(e))

@app.post("/v1/settle/execute", response_class=JSONResponse)
def execute_settlement(payload: SettlePayload, db: Session = Depends(get_db)):
    try:
        start = payload.start_time or (datetime.utcnow() - timedelta(days=2))
        end = payload.end_time or datetime.utcnow()
        events = db.query(UsageEvent).filter(UsageEvent.timestamp >= start, UsageEvent.timestamp < end).all()
        if not events:
            return JSONResponse(status_code=200, content={"message": "No events found to settle."})
        batch, result_data, _ = apply_policy_and_settle(
            db, payload.use_case, payload.policy_body, events, start_time=start, end_time=end
        )
        pid_map = {p.id: p for p in db.query(Participant).filter(Participant.id.in_(result_data.keys())).all()}
        final_net = {pid_map[i].external_id: round(d["final_net"], 2) for i, d in result_data.items()}
        return JSONResponse(content={
            "status": "success", "batch_id": batch.id,
            "message": "Settlement executed and proofs generated.",
            "final_net_balances": final_net
        })
    except Exception as e:
        db.rollback(); raise HTTPException(status_code=500, detail=str(e))

# ---------- PoC Endpoint mit Policy-DSL ----------
def _round_amt(x: float, mode: str) -> float:
    d = Decimal(str(x))
    if mode == "bankers":
        return float(d.quantize(Decimal("0.01"), rounding=ROUND_HALF_EVEN))
    return float(d.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))

def _fare_for(mode: str, rng: random.Random) -> float:
    if mode == "car": return round(rng.uniform(8.0, 30.0), 2)
    if mode == "scooter": return round(rng.uniform(1.0, 5.0), 2)
    # mixed
    roll = rng.random()
    if roll < 0.6: return round(rng.uniform(1.0, 5.0), 2)      # scooter
    if roll < 0.9: return round(rng.uniform(8.0, 30.0), 2)     # car
    return round(rng.uniform(0.5, 3.0), 2)                      # charge/misc

@app.post("/v1/poc/run-demo")
def run_poc_demo(payload: PocDemoPayload, db: Session = Depends(get_db)):
    """
    Policy-DSL (vereinfachtes Beispielformat):

    {
      "scenario": "mixed" | "scooter" | "car",
      "fees": {"per_payout_eur": 0.30},
      "actors": {"riders": 400, "operators": 20, "cities": 10},
      "splits": {"operator_share": 0.90, "city_share": 0.10},
      "thresholds": {"min_payout_eur": 1.00},
      "rules": [
        {"match":{"role":"city"}, "min_payout_eur":5.00},
        {"match":{"role":"operator"}, "min_payout_eur":1.00},
        {"match":{"role":"all"}, "round":"half_up"}  # oder "bankers"
      ],
      "optimize": {"distribution":"uniform" | "concentrated"}
    }
    """
    policy = payload.policy_body or {}

    scenario = (policy.get("scenario") or payload.scenario or "mixed").lower()
    actors = policy.get("actors") or {}
    riders_n = int(actors.get("riders") or payload.riders or 400)
    ops_n = int(actors.get("operators") or payload.operators or 20)
    cities_n = int(actors.get("cities") or payload.cities or 10)

    splits = policy.get("splits") or {}
    operator_share = float(splits.get("operator_share") or 0.90)
    city_share = float(splits.get("city_share") or 0.10)

    fees = policy.get("fees") or {}
    fee_per_payout = float(fees.get("per_payout_eur") or payload.fee_per_transaction_eur or 0.30)

    thresholds = policy.get("thresholds") or {}
    min_payout_global = float(thresholds.get("min_payout_eur") or 0.0)

    # rules
    rules = policy.get("rules") or []
    round_mode = "half_up"
    min_city = min_payout_global
    min_op = min_payout_global
    for r in rules:
        m = (r.get("match") or {})
        if m.get("role") == "all" and r.get("round"):
            round_mode = r["round"]
        if m.get("role") == "city" and r.get("min_payout_eur") is not None:
            min_city = float(r["min_payout_eur"])
        if m.get("role") == "operator" and r.get("min_payout_eur") is not None:
            min_op = float(r["min_payout_eur"])

    optimize = policy.get("optimize") or {}
    distribution = (optimize.get("distribution") or "uniform").lower()

    # Simulation (deterministisch reproduzierbar)
    rng = random.Random(42)
    riders = [f"Rider-{i:04d}" for i in range(1, max(1, riders_n) + 1)]
    operators = [f"Operator-{i+1}" for i in range(max(1, ops_n))]
    cities = [f"City-{i+1}" for i in range(max(1, cities_n))]

    # Gewichtete Auswahl (concentrated -> top 20% erhalten 80% der Rides)
    def pick_weighted(lst: List[str], k_weight: float = 0.8) -> str:
        if len(lst) <= 1: return lst[0]
        if distribution == "concentrated":
            top = max(1, int(len(lst) * 0.2))
            if rng.random() < k_weight:
                return lst[rng.randrange(top)]
        return lst[rng.randrange(len(lst))]

    gross_volume = 0.0
    raw_transactions: List[Dict[str, Any]] = []

    # Nur Empfänger saldieren (Operatoren/Städte). Rider sind Zahler (keine Payouts).
    balances: Dict[str, float] = defaultdict(float)

    for _ in range(max(1, payload.transaction_count)):
        rider = rng.choice(riders)
        fare = _fare_for(scenario, rng)
        gross_volume += fare
        raw_transactions.append({"participant_id": rider, "amount": fare})

        op = pick_weighted(operators)
        ct = pick_weighted(cities)

        balances[op] += -fare * operator_share   # empfangen (negativ = erhält)
        balances[ct] += -fare * city_share

    # Runden & Schwellen anwenden je Rolle
    def role_of(pid: str) -> str:
        if pid.startswith("City-"): return "city"
        if pid.startswith("Operator-"): return "operator"
        return "other"

    rounded: Dict[str, float] = {}
    for pid, amt in balances.items():
        role = role_of(pid)
        amt_r = _round_amt(amt, round_mode)
        thr = min_payout_global
        if role == "city": thr = max(thr, min_city)
        if role == "operator": thr = max(thr, min_op)
        if abs(amt_r) < thr:
            amt_r = 0.0
        if abs(amt_r) >= 0.01:
                rounded[pid] = amt_r

    netted_payouts = {pid: round(val, 2) for pid, val in rounded.items()}
    netted_transaction_count = len(netted_payouts)

    estimated_fees = payload.transaction_count * fee_per_payout
    actual_fees = netted_transaction_count * fee_per_payout
    compression_ratio = round(payload.transaction_count / max(1, netted_transaction_count), 2)

    return {
        "before": {
            "transaction_stream": raw_transactions[:50],
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
                "policy_body": policy
            },
            "response_body": { "final_net_balances": netted_payouts }
        }
    }

