from __future__ import annotations
import io, json, traceback
from pathlib import Path
import pandas as pd
from collections import defaultdict
from typing import List, Optional

from fastapi import FastAPI, Request, Depends, UploadFile, File, Form, HTTPException
from fastapi.responses import HTMLResponse, RedirectResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from pydantic import BaseModel
from datetime import datetime

from . import use_cases
from .db import create_db_and_tables, get_db, ensure_min_schema
from .models import Participant, ParticipantRole, UsageEvent, EventType, Policy, SettlementBatch, LedgerEntry
from .settle import apply_policy_and_settle, apply_bilateral_netting
from .audit import get_audit_data

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

# ---------- Startup ----------
@app.on_event("startup")
def on_startup():
    print("DB init...")
    create_db_and_tables()
    ensure_min_schema()
    print("Startup complete.")
    for r in app.routes:
        try: print("ROUTE:", r.path, r.methods)
        except Exception: pass

# ---------- API Routes ----------
# Startseite, die direkt zum Dashboard führt
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
                role = ParticipantRole.PROSUMER
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
            src = (meta.get('source') or '').lower()
            price_meta = float(meta.get('price_eur_per_kwh') or 0.0)
            
            if ev.event_type.value in ('consumption', 'base_fee'):
                balances[p.id]['debit'] += qty * price_meta
            elif ev.event_type.value in ('generation', 'grid_feed', 'vpp_sale'):
                balances[p.id]['credit'] += qty * price_meta

        final_balances, stats, transfers = apply_bilateral_netting(balances)
        
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

# ---------- UI Routes to be removed ----------
# Diese Routen sind für den API-First-Flow nicht mehr nötig und werden entfernt.
# @app.get("/upload", response_class=HTMLResponse)
# ...
# @app.post("/process_data", response_class=HTMLResponse)
# ...
# @app.get("/audit", response_class=HTMLResponse)
# ...
# @app.get("/start")
# ...