from __future__ import annotations
from collections import defaultdict
from typing import List
from fastapi import HTTPException
from sqlalchemy.orm import Session

from .models import SettlementBatch, SettlementLine, UsageEvent, Participant
from app.utils.crypto import create_transaction_hash  # ABSOLUTE IMPORT

def human_readable_explanation(participant, events: List[UsageEvent], final_amount: float, use_case: str) -> str:
    role_names = {
        "tenant": "Mieter", "commercial": "Gewerbemieter", "landlord": "Vermieter",
        "operator": "Betreiber", "external_market": "Externer Markt", "prosumer": "Prosumer",
        "consumer": "Verbraucher",
    }
    role = role_names.get(getattr(participant.role, "value", participant.role), "Unbekannt")

    if not events:
        return f"{participant.name} ({role}) hat keine Events. Finalbetrag: {final_amount:.2f} EUR"

    consumption_local = sum(e.quantity for e in events
                            if e.event_type.value == "consumption"
                            and (e.meta or {}).get("source", "").lower() in ["local_pv", "battery", "local_battery"])
    consumption_grid = sum(e.quantity for e in events
                           if e.event_type.value == "consumption"
                           and (e.meta or {}).get("source", "").lower() not in ["local_pv", "battery", "local_battery"])
    generation = sum(e.quantity for e in events if e.event_type.value in ["generation", "grid_feed"])
    base_fee_total = sum(e.quantity for e in events if e.event_type.value == "base_fee")

    parts = []
    if consumption_local > 0: parts.append(f"{consumption_local:.1f} kWh lokaler Strom")
    if consumption_grid > 0: parts.append(f"{consumption_grid:.1f} kWh Netzstrom")
    if generation > 0: parts.append(f"{generation:.1f} kWh erzeugt/eingespeist")
    if base_fee_total > 0: parts.append(f"{base_fee_total:.2f} EUR Grundgebühr")

    summary = f"{participant.name} ({role}): " + (", ".join(parts) + ". " if parts else "Keine relevanten Aktivitäten. ")
    if final_amount > 0: summary += f"Zahlt {final_amount:.2f} EUR."
    elif final_amount < 0: summary += f"Erhält {abs(final_amount):.2f} EUR."
    else: summary += "Ausgeglichen (0 EUR)."
    return summary

def get_audit_payload(db: Session, batch_id: int, explain: bool = False):
    batch = db.query(SettlementBatch).filter(SettlementBatch.id == batch_id).first()
    if not batch:
        raise HTTPException(status_code=404, detail="Batch not found.")

    lines = db.query(SettlementLine).filter(SettlementLine.batch_id == batch_id).all()
    relevant_events = db.query(UsageEvent).filter(
        UsageEvent.timestamp >= batch.start_time,
        UsageEvent.timestamp <= batch.end_time
    ).all()

    all_participants = {p.id: p for p in db.query(Participant).all()}
    events_by_participant = defaultdict(list)
    for ev in relevant_events:
        events_by_participant[ev.participant_id].append(ev)

    payload = {
        "batch_id": batch.id,
        "use_case": batch.use_case,
        "created_at": batch.created_at.isoformat(),
        "settlement_lines": []
    }

    for line in lines:
        base = {
            "batch_id": line.batch_id,
            "participant_id": line.participant_id,
            "amount_eur": line.amount_eur,
            "description": line.description,
        }
        recreated = create_transaction_hash(base)
        participant = all_participants.get(line.participant_id)
        line_obj = {
            "line_id": line.id,
            "participant_id": line.participant_id,
            "participant_name": participant.name if participant else "Unbekannt",
            "participant_role": participant.role.value if participant else "Unbekannt",
            "amount_eur": line.amount_eur,
            "description": line.description,
            "proof_hash": line.proof_hash,
            "is_verified": (recreated == line.proof_hash),
        }
        if explain and participant:
            line_obj["human_readable_explanation"] = human_readable_explanation(
                participant,
                events_by_participant.get(line.participant_id, []),
                line.amount_eur,
                batch.use_case
            )
        payload["settlement_lines"].append(line_obj)
    return payload
