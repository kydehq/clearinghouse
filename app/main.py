from __future__ import annotations
from pathlib import Path
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from collections import defaultdict
from decimal import Decimal, ROUND_HALF_UP, ROUND_HALF_EVEN
import random
import os

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
    use_case: Optional[str] = "mobility"   # <— neu
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
    # Für Demos: mit KYDE_SKIP_DB_INIT=1 DB-Migration überspringen
    try:
        if os.getenv("KYDE_SKIP_DB_INIT", "0") == "1":
            print("[startup] Skipping DB init due to KYDE_SKIP_DB_INIT=1")
            return
        ensure_min_schema()
    except Exception as e:
        # Nicht blockieren – lieber starten und Demo-Routen verfügbar machen
        print(f"[startup] DB init failed or skipped: {e}")



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
def run_poc_demo(payload: PocDemoPayload):
    policy = payload.policy_body or {}
    use_case = (payload.use_case or policy.get("use_case") or "mobility").lower()
    if use_case == "energy":
        return _simulate_energy(policy, payload.transaction_count)
    return _simulate_mobility(policy, payload.transaction_count)


# ------------------ Mobility (bestehende Logik, kompakt extrahiert) ------------------

def _simulate_mobility(policy: Dict[str, Any], tx_count: int):
    # Policy lesen
    scenario = (policy.get("scenario") or "mixed").lower()
    actors = policy.get("actors") or {}
    riders_n = int(actors.get("riders") or 400)
    fleet_n  = int(actors.get("fleet_partners") or actors.get("operators") or 20)
    cities_n = int(actors.get("cities") or 10)

    splits = policy.get("splits") or {}
    fleet_share = float(splits.get("fleet_share") or splits.get("operator_share") or 0.90)
    city_share  = float(splits.get("city_share") or 0.10)

    fees = policy.get("fees") or {}
    fee_per_payout = float(fees.get("per_payout_eur") or 0.30)

    thresholds = policy.get("thresholds") or {}
    min_payout_global = float(thresholds.get("min_payout_eur") or 0.0)

    rules = policy.get("rules") or []
    round_mode = "half_up"
    min_city = min_payout_global
    min_fleet = min_payout_global
    for r in rules:
        m = (r.get("match") or {})
        if m.get("role") == "all" and r.get("round"): round_mode = r["round"]
        if m.get("role") == "city" and r.get("min_payout_eur") is not None: min_city = float(r["min_payout_eur"])
        if m.get("role") in ("fleet_partner", "operator") and r.get("min_payout_eur") is not None: min_fleet = float(r["min_payout_eur"])

    distribution = (policy.get("optimize") or {}).get("distribution","uniform").lower()

    rng = random.Random(42)
    riders = [f"Rider-{i:04d}" for i in range(1, max(1, riders_n)+1)]
    fleet  = [f"Fleet-{i+1}" for i in range(max(1, fleet_n))]
    cities = [f"City-{i+1}"  for i in range(max(1, cities_n))]

    def pick_weighted(lst: List[str], k_weight: float = 0.8) -> str:
        if len(lst) <= 1: return lst[0]
        if distribution == "concentrated":
            top = max(1, int(len(lst) * 0.2))
            if rng.random() < k_weight: return lst[rng.randrange(top)]
        return lst[rng.randrange(len(lst))]

    gross_volume = 0.0
    raw_transactions: List[Dict[str, Any]] = []
    operator_owes_party: Dict[str, float] = defaultdict(float)
    party_owes_operator: Dict[str, float] = defaultdict(float)
    obligations_created = 0
    penalties_count = 0

    for _ in range(max(1, tx_count)):
        rider = rng.choice(riders)
        # eigene Helper: _fare_for & _round_amt sind in deiner Datei schon definiert
        fare = _fare_for(scenario, rng)
        gross_volume += fare
        raw_transactions.append({"participant_id": rider, "amount": fare})

        fp = pick_weighted(fleet); ct = pick_weighted(cities)
        operator_owes_party[fp] += fare * fleet_share; obligations_created += 1
        operator_owes_party[ct] += fare * city_share;  obligations_created += 1

        if rng.random() < 0.12:
            penalty = round(rng.uniform(0.5, 3.0), 2)
            party_owes_operator[fp] += penalty
            obligations_created += 1; penalties_count += 1

    # Offset & Nettos
    internal_offset_eur = 0.0
    balances: Dict[str, float] = {}
    all_parties = set(operator_owes_party) | set(party_owes_operator)
    for pid in all_parties:
        to_party = operator_owes_party.get(pid, 0.0)
        to_op    = party_owes_operator.get(pid, 0.0)
        internal_offset_eur += min(to_party, to_op)
        balances[pid] = -(to_party - to_op)  # negativ = Auszahlung

    def role_of(pid: str) -> str:
        if pid.startswith("City-"):  return "city"
        if pid.startswith("Fleet-"): return "fleet_partner"
        return "other"

    rounded: Dict[str, float] = {}
    for pid, amt in balances.items():
        role = role_of(pid)
        amt_r = _round_amt(amt, round_mode)
        thr = min_payout_global
        if role == "city":          thr = max(thr, min_city)
        if role == "fleet_partner": thr = max(thr, min_fleet)
        if abs(amt_r) >= max(0.01, thr): rounded[pid] = round(amt_r, 2)

    netted_payouts = dict(sorted(rounded.items(), key=lambda kv: abs(kv[1]), reverse=True))
    netted_transaction_count = len(netted_payouts)

    est_fees = tx_count * fee_per_payout
    act_fees = netted_transaction_count * fee_per_payout
    ratio = round(tx_count / max(1, netted_transaction_count), 2)

    return {
        "before": {
            "transaction_stream": raw_transactions[:50],
            "metrics": {
                "total_transactions": tx_count,
                "gross_volume_eur": round(gross_volume, 2),
                "estimated_fees_eur": round(est_fees, 2),
                "obligations_created": int(obligations_created),
                "obligations_breakdown": { "share_splits": int(tx_count*2), "penalties": int(penalties_count) }
            }
        },
        "after": {
            "netted_payouts": netted_payouts,
            "metrics": {
                "netted_transactions": netted_transaction_count,
                "actual_fees_eur": round(act_fees, 2),
                "savings_eur": round(est_fees - act_fees, 2),
                "compression_ratio": ratio,
                "internal_offset_eur": round(internal_offset_eur, 2)
            }
        },
        "api_proof": {
            "request_body_snippet": { "transaction_count": tx_count, "policy_body": policy, "use_case": "mobility" },
            "response_body": { "final_net_balances": netted_payouts }
        }
    }


# ------------------ Energy Community (realistisches N↔N, Österreich-artig) ------------------

def _simulate_energy(policy: Dict[str, Any], tx_count: int):
    # Teilnehmer & Parameter
    actors = policy.get("actors") or {}
    participants_n = int(actors.get("participants") or actors.get("riders") or 120)  # Haushalte/Prosumer
    rng = random.Random(43)

    # Preise (können später in policy.prices konfiguriert werden)
    prices = (policy.get("prices") or {})
    pv_price      = float(prices.get("pv_eur_per_kwh")      or 0.14)  # Vergütung an Prosumer
    local_price   = float(prices.get("local_eur_per_kwh")   or 0.18)  # Haushalt zahlt an Community
    grid_min      = float(prices.get("grid_min_eur_per_kwh")or 0.25)
    grid_max      = float(prices.get("grid_max_eur_per_kwh")or 0.45)
    flex_min      = float(prices.get("flex_min_eur_per_kwh")or 0.08)
    flex_max      = float(prices.get("flex_max_eur_per_kwh")or 0.16)
    community_fee = float(prices.get("community_fee_eur")   or 1.50)

    # Schwellen/Rundung übernehmen (wir mappen fleet->household/prosumer, city->DSO)
    thresholds = policy.get("thresholds") or {}
    min_payout_global = float(thresholds.get("min_payout_eur") or 0.0)
    rules = policy.get("rules") or []
    round_mode = "half_up"
    min_household = min_payout_global
    min_dso = min_payout_global
    for r in rules:
        m = (r.get("match") or {})
        if m.get("role") == "all" and r.get("round"): round_mode = r["round"]
        if m.get("role") in ("fleet_partner","operator"):  # als Household/Prosumer interpretieren
            if r.get("min_payout_eur") is not None: min_household = float(r["min_payout_eur"])
        if m.get("role") == "city":  # als DSO interpretieren
            if r.get("min_payout_eur") is not None: min_dso = float(r["min_payout_eur"])

    # Entitäten
    HH = [f"HH-{i:04d}" for i in range(1, participants_n+1)]
    DSO = "DSO"
    MARKET = "External-Market"

    # Pools
    local_pool_kwh = 0.0  # erzeugter PV-Strom in Community

    # Verpflichtungen rund um "Community-Treasury" (Operator-Idee beibehalten)
    operator_owes_party: Dict[str, float] = defaultdict(float)   # Community -> Partei (Prosumer/DSO)
    party_owes_operator: Dict[str, float] = defaultdict(float)   # Partei -> Community (Haushalt/Market)

    obligations_created = 0
    breakdown = {"consumption": 0, "pv_generation": 0, "flex_revenue": 0, "fees": 0}

    # Events generieren
    for _ in range(max(1, tx_count)):
        roll = rng.random()

        # 0.55 Konsum, 0.35 PV, 0.10 Flex
        if roll < 0.55:
            h = rng.choice(HH)
            kwh = round(rng.uniform(0.6, 4.0), 2)
            grid_price = round(rng.uniform(grid_min, grid_max), 2)

            # 50% des Verbrauchs versuchen wir aus dem lokalen Pool zu decken
            want_local = round(0.5 * kwh, 2)
            use_local  = min(local_pool_kwh, want_local)
            from_grid  = round(kwh - use_local, 2)

            if use_local > 0:
                party_owes_operator[h] += use_local * local_price
                local_pool_kwh = round(local_pool_kwh - use_local, 4)
                obligations_created += 1; breakdown["consumption"] += 1

            if from_grid > 0:
                operator_owes_party[DSO] += from_grid * grid_price
                obligations_created += 1; breakdown["consumption"] += 1

            # gelegentliche Community-Fee
            if rng.random() < 0.05:
                party_owes_operator[h] += community_fee
                obligations_created += 1; breakdown["fees"] += 1

        elif roll < 0.90:
            # PV-Erzeugung – Prosumer werden von Community vergütet, Pool steigt
            h = rng.choice(HH)
            gen = round(rng.uniform(0.3, 2.5), 2)
            operator_owes_party[h] += gen * pv_price
            local_pool_kwh = round(local_pool_kwh + gen, 4)
            obligations_created += 1; breakdown["pv_generation"] += 1

        else:
            # Flex-/Regelenergie – Market zahlt an Community
            flex = round(rng.uniform(0.5, 3.0), 2)
            price = round(rng.uniform(flex_min, flex_max), 2)
            party_owes_operator[MARKET] += flex * price
            obligations_created += 1; breakdown["flex_revenue"] += 1

    # Interner Offset & Nettos
    internal_offset_eur = 0.0
    balances: Dict[str, float] = {}
    all_parties = set(operator_owes_party) | set(party_owes_operator)
    for pid in all_parties:
        to_party = operator_owes_party.get(pid, 0.0)
        to_op    = party_owes_operator.get(pid, 0.0)
        internal_offset_eur += min(to_party, to_op)
        balances[pid] = -(to_party - to_op)  # negativ = Auszahlung an Partei

    def role_of(pid: str) -> str:
        if pid == DSO:                 return "dso"
        if pid == MARKET:              return "external_market"
        if pid.startswith("HH-"):      return "household"
        return "other"

    rounded: Dict[str, float] = {}
    for pid, amt in balances.items():
        role = role_of(pid)
        amt_r = _round_amt(amt, round_mode)
        thr = min_payout_global
        if role in ("household",): thr = max(thr, min_household)
        if role in ("dso",):       thr = max(thr, min_dso)
        if abs(amt_r) >= max(0.01, thr): rounded[pid] = round(amt_r, 2)

    netted_payouts = dict(sorted(rounded.items(), key=lambda kv: abs(kv[1]), reverse=True))
    netted_transaction_count = len(netted_payouts)

    # Payout-Fee wie gehabt
    fees = (policy.get("fees") or {})
    fee_per_payout = float(fees.get("per_payout_eur") or 0.30)
    est_fees = tx_count * fee_per_payout
    act_fees = netted_transaction_count * fee_per_payout
    ratio = round(tx_count / max(1, netted_transaction_count), 2)

    # Für die „Before“-Spalte zeigen wir bewusst Events (künstlich), exakt wie bei Mobility
    sample_stream = []
    # ein paar Beispiele aus den oben gezählten Events zusammenbauen:
    for _ in range(min(50, tx_count)):
        sample_stream.append({"participant_id": rng.choice(HH), "amount": round(rng.uniform(0.5, 8.0),2)})

    return {
        "before": {
            "transaction_stream": sample_stream,
            "metrics": {
                "total_transactions": tx_count,
                "gross_volume_eur": None,  # bei Energy weniger aussagekräftig
                "estimated_fees_eur": round(est_fees, 2),
                "obligations_created": int(obligations_created),
                "obligations_breakdown": breakdown
            }
        },
        "after": {
            "netted_payouts": netted_payouts,
            "metrics": {
                "netted_transactions": netted_transaction_count,
                "actual_fees_eur": round(act_fees, 2),
                "savings_eur": round(est_fees - act_fees, 2),
                "compression_ratio": ratio,
                "internal_offset_eur": round(internal_offset_eur, 2)
            }
        },
        "api_proof": {
            "request_body_snippet": { "transaction_count": tx_count, "policy_body": policy, "use_case": "energy" },
            "response_body": { "final_net_balances": netted_payouts }
        }
    }

