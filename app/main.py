from __future__ import annotations
import io, json, traceback
from pathlib import Path
from collections import defaultdict
from typing import List, Optional
from fastapi import FastAPI, Request, Depends, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from pydantic import BaseModel
from datetime import datetime

from . import use_cases
from .db import ensure_min_schema, get_db
from .models import Participant, ParticipantRole, UsageEvent, EventType, SettlementBatch, SettlementLine
from .settle import apply_policy_and_settle, apply_bilateral_netting, create_transaction_hash

# ---------- App / Templates ----------
BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR.parent / "static"

app = FastAPI(title="KYDE PoC", debug=True)
templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")

# Pydantic-Modell für API-Payloads
class EventPayload(BaseModel):
    participant_id: str
    event_type: str
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
    print("DB init...")
    try:
        ensure_min_schema()
        print("Startup complete.")
    except Exception as e:
        print(f"ERROR: Application startup failed with an exception: {e}")
        traceback.print_exc()
        raise

# ---------- API Routes ----------
@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})

@app.get("/demo/api-dashboard", response_class=HTMLResponse)
def get_api_dashboard(request: Request):
    return templates.TemplateResponse("api_dashboard.html", {"request": request})

@app.post("/v1/energy-events", status_code=201)
def ingest_energy_events(events: List[EventPayload], db: Session = Depends(get_db)):
    try:
        new_participants_list = []
        participant_map_dict = {p.external_id: p for p in db.query(Participant).all()}

        for event in events:
            ext_id = event.participant_id
            p = participant_map_dict.get(ext_id)

            if not p:
                # Korrektur: Role in Kleinbuchstaben konvertieren
                role = ParticipantRole.PROSUMER.lower()
                p = Participant(external_id=ext_id, name=f"Participant {ext_id}", role=role)
                db.add(p)
                new_participants_list.append(p)
                participant_map_dict[ext_id] = p
        
        if new_participants_list: db.flush()
        
        usage_events: list[UsageEvent] = []
        for event in events:
            ext_id = event.participant_id
            p = participant_map_dict.get(ext_id)
            if not p:
                raise HTTPException(status_code=500, detail=f"Teilnehmer mit ID {ext_id} konnte nicht erstellt oder gefunden werden.")

            usage_event = UsageEvent(
                participant_id=p.id,
                # Korrektur: event_type in Kleinbuchstaben konvertieren
                event_type=EventType(event.event_type.lower()),
                quantity=event.quantity,
                unit=event.unit,
                timestamp=event.timestamp,
                meta={"source": event.source, "price_eur_per_kwh": event.price_eur_per_kwh}
            )
            usage_events.append(usage_event)

        db.add_all(usage_events)
        db.commit()
        return {"status": "success", "message": f"Ingested {len(events)} events."}
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/v1/netting/preview", response_class=JSONResponse)
def netting_preview(payload: NettingPreviewPayload, db: Session = Depends(get_db)):
    try:
        events = db.query(UsageEvent).filter(UsageEvent.timestamp.between(payload.start_time, payload.end_time)).all()
        if not events:
            return JSONResponse(status_code=200, content={"message": "No events found in the specified timeframe."})

        participant_ids = [e.participant_id for e in events]
        participants = db.query(Participant).filter(Participant.id.in_(participant_ids)).all()
        id_to_participant = {p.id: p for p in participants}

        balances = defaultdict(lambda: {'credit': 0.0, 'debit': 0.0})
        for ev in events:
            p = id_to_participant.get(ev.participant_id)
            if not p: continue
            
            qty = float(ev.quantity or 0.0)
            meta = ev.meta or {}
            price_meta = float(meta.get('price_eur_per_kwh') or 0.0)
            
            if ev.event_type.value in ('consumption', 'base_fee'):
                balances[p.id]['debit'] += qty * price_meta
            elif ev.event_type.value in ('generation', 'grid_feed', 'vpp_sale'):
                balances[p.id]['credit'] += qty * price_meta

        final_balances, stats, transfers = apply_bilateral_netting(balances, payload.policy_body)
        
        response_data = {
            "stats": stats,
            "transfers": transfers,
            "final_balances": {
                id_to_participant[pid].external_id: round(balance, 2)
                for pid, balance in final_balances.items() if abs(balance) > 0.01
            }
        }
        return JSONResponse(content=response_data)
        
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/v1/settle/execute", response_class=JSONResponse)
def execute_settlement(payload: SettlePayload, db: Session = Depends(get_db)):
    try:
        events = db.query(UsageEvent).filter(UsageEvent.timestamp.between(payload.start_time, payload.end_time)).all()
        if not events:
            return JSONResponse(status_code=200, content={"message": "No events found to settle."})
        
        batch, result_data, netting_stats = apply_policy_and_settle(
            db, payload.use_case, payload.policy_body, events, 
            start_time=payload.start_time, end_time=payload.end_time
        )
        
        return JSONResponse(content={
            "status": "success",
            "batch_id": batch.id,
            "message": "Settlement executed and proofs generated.",
            "final_net_balances": {
                p.external_id: round(result_data[p.id]['final_net'], 2)
                for p in db.query(Participant).filter(Participant.id.in_(result_data.keys())).all()
            }
        })
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))

# ---------- Helper Functions ----------
def _generate_human_readable_explanation(participant, events, final_amount, use_case):
    """Generiert eine kurze, menschlich lesbare Zusammenfassung."""
    
    role_descriptions = {
        'tenant': 'Mieter',
        'commercial': 'Gewerbemieter', 
        'landlord': 'Vermieter',
        'operator': 'Betreiber',
        'external_market': 'Externer Markt',
        'prosumer': 'Prosumer'
    }
    
    role_name = role_descriptions.get(participant.role.value, participant.role.value)
    
    if not events:
        return f"{participant.name} ({role_name}) hat keine Events im Abrechnungszeitraum. Finalbetrag: {final_amount:.2f} EUR"
    
    # Schnelle Zusammenfassung der wichtigsten Aktivitäten
    consumption_local = sum(e.quantity for e in events if e.event_type.value == 'consumption' and e.meta.get('source', '').lower() in ['local_pv', 'battery', 'local_battery'])
    consumption_grid = sum(e.quantity for e in events if e.event_type.value == 'consumption' and e.meta.get('source', '').lower() not in ['local_pv', 'battery', 'local_battery'])
    generation = sum(e.quantity for e in events if e.event_type.value in ['generation', 'grid_feed'])
    base_fee_total = sum(e.quantity for e in events if e.event_type.value == 'base_fee')
    
    summary = f"{participant.name} ({role_name}): "
    
    activities = []
    if consumption_local > 0:
        activities.append(f"{consumption_local:.1f} kWh lokaler Strom")
    if consumption_grid > 0:
        activities.append(f"{consumption_grid:.1f} kWh Netzstrom")
    if generation > 0:
        activities.append(f"{generation:.1f} kWh erzeugt/eingespeist")
    if base_fee_total > 0:
        activities.append(f"{base_fee_total:.2f} EUR Grundgebühr")
    
    if activities:
        summary += ", ".join(activities) + ". "
    else:
        summary += "Keine relevanten Aktivitäten. "
    
    if final_amount > 0:
        summary += f"Zahlt {final_amount:.2f} EUR."
    elif final_amount < 0:
        summary += f"Erhält {abs(final_amount):.2f} EUR."
    else:
        summary += "Ausgeglichen (0 EUR)."
    
    return summary

@app.get("/v1/audit/{batch_id}", response_class=JSONResponse)
def audit_batch(batch_id: int, db: Session = Depends(get_db), explain: bool = False):
    try:
        batch = db.query(SettlementBatch).filter(SettlementBatch.id == batch_id).first()
        if not batch:
            raise HTTPException(status_code=404, detail="Batch not found.")
        
        lines = db.query(SettlementLine).filter(SettlementLine.batch_id == batch_id).all()
        
        # Verbessert: Frage nur Events im Zeitraum des Batches ab
        relevant_events = db.query(UsageEvent).filter(
            UsageEvent.timestamp >= batch.start_time,
            UsageEvent.timestamp <= batch.end_time
        ).all()
        
        all_participants = {p.id: p for p in db.query(Participant).all()}
        
        # Erstelle ein Dictionary für den schnellen Zugriff auf Events pro Teilnehmer
        participant_events_map = defaultdict(list)
        for ev in relevant_events:
            participant_events_map[ev.participant_id].append(ev)
        
        audit_data = {
            "batch_id": batch.id,
            "use_case": batch.use_case,
            "created_at": batch.created_at.isoformat(),
            "settlement_lines": []
        }
        
        for line in lines:
            transaction_data = {
                "batch_id": line.batch_id,
                "participant_id": line.participant_id,
                "amount_eur": line.amount_eur,
                "description": line.description
            }
            recreated_hash = create_transaction_hash(transaction_data)
            
            participant = all_participants.get(line.participant_id)
            
            line_data = {
                "line_id": line.id,
                "participant_id": line.participant_id,
                "participant_name": participant.name if participant else "Unbekannt",
                "participant_role": participant.role.value if participant else "Unbekannt",
                "amount_eur": line.amount_eur,
                "description": line.description,
                "proof_hash": line.proof_hash,
                "is_verified": (recreated_hash == line.proof_hash)
            }
            
            if explain and participant:
                # Nutze das vorbereitete Dictionary
                participant_events = participant_events_map.get(line.participant_id, [])
                human_readable = _generate_human_readable_explanation(
                    participant, participant_events, line.amount_eur, batch.use_case
                )
                line_data["human_readable_explanation"] = human_readable
            
            audit_data["settlement_lines"].append(line_data)
            
        return JSONResponse(content=audit_data)

    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))