from __future__ import annotations
from collections import defaultdict
from typing import List, Dict, Any
from fastapi import HTTPException
from sqlalchemy.orm import Session

from .models import SettlementBatch, SettlementLine, UsageEvent, Participant
from app.utils.crypto import create_transaction_hash  # abs. Import, kein Zyklus

def human_readable_explanation(
    participant: Participant,
    events: List[UsageEvent],
    final_amount: float,
    use_case: str
) -> str:
    role_names = {
        "tenant": "Mieter",
        "commercial": "Gewerbemieter",
        "landlord": "Vermieter",
        "operator": "Betreiber",
        "external_market": "Externer Markt",
        "prosumer": "Prosumer",
        "consumer": "Verbraucher",
        "community_fee_collector": "Community Fee Collector",
    }
    role = role_names.get(getattr(participant.role, "value", participant.role), "Unbekannt")

    if not events:
        return f"{participant.name} ({role}) hat keine relevanten Events. Finalbetrag: {final_amount:.2f} EUR."

    def _src(ev: UsageEvent) -> str:
        return (ev.meta or {}).get("source", "").lower()

    consumption_local = sum(
        float(e.quantity or 0.0)
        for e in events
        if e.event_type.value == "consumption" and _src(e) in ["local_pv", "battery", "local_battery"]
    )
    consumption_grid = sum(
        float(e.quantity or 0.0)
        for e in events
        if e.event_type.value == "consumption" and _src(e) not in ["local_pv", "battery", "local_battery"]
    )
    generation = sum(
        float(e.quantity or 0.0)
        for e in events
        if e.event_type.value in ["generation", "grid_feed"]
    )
    base_fee_total = sum(
        float(e.quantity or 0.0)
        for e in events
        if e.event_type.value == "base_fee"
    )

    parts = []
    if consumption_local > 0:
        parts.append(f"{consumption_local:.1f} kWh lokaler Strom")
    if consumption_grid > 0:
        parts.append(f"{consumption_grid:.1f} kWh Netzstrom")
    if generation > 0:
        parts.append(f"{generation:.1f} kWh erzeugt/eingespeist")
    if base_fee_total > 0:
        parts.append(f"{base_fee_total:.2f} EUR Grundgebühr")

    summary = f"{participant.name} ({role}): " + (", ".join(parts) + ". " if parts else "Keine relevanten Aktivitäten. ")
    if final_amount > 0:
        summary += f"Zahlt {final_amount:.2f} EUR."
    elif final_amount < 0:
        summary += f"Erhält {abs(final_amount):.2f} EUR."
    else:
        summary += "Ausgeglichen (0 EUR)."
    return summary

def get_audit_payload(db: Session, batch_id: int, explain: bool = False) -> Dict[str, Any]:
    batch = db.query(SettlementBatch).filter(SettlementBatch.id == batch_id).first()
    if not batch:
        raise HTTPException(status_code=404, detail="Batch not found.")

    lines = db.query(SettlementLine).filter(SettlementLine.batch_id == batch_id).all()

    # Halb-offenes Intervall wie im Settlement (>= start, < end), um Doppelzählungen zu vermeiden
    relevant_events = db.query(UsageEvent).filter(
        UsageEvent.timestamp >= batch.start_time,
        UsageEvent.timestamp < batch.end_time
    ).all()

    all_participants = {p.id: p for p in db.query(Participant).all()}

    events_by_participant: Dict[int, List[UsageEvent]] = defaultdict(list)
    for ev in relevant_events:
        events_by_participant[ev.participant_id].append(ev)

    payload: Dict[str, Any] = {
        "batch_id": batch.id,
        "use_case": batch.use_case,
        "created_at": batch.created_at.isoformat(),
        "start_time": batch.start_time.isoformat(),
        "end_time": batch.end_time.isoformat(),
        "settlement_lines": []
    }

    for line in lines:
        base = {
            "batch_id": line.batch_id,
            "participant_id": line.participant_id,
            "amount_eur": float(line.amount_eur),
            "description": line.description,
        }
        recreated = create_transaction_hash(base)

        participant = all_participants.get(line.participant_id)
        line_obj: Dict[str, Any] = {
            "line_id": line.id,
            "participant_id": line.participant_id,
            "participant_name": participant.name if participant else "Unbekannt",
            "participant_role": (participant.role.value if participant and hasattr(participant.role, "value") else "Unbekannt"),
            "amount_eur": float(line.amount_eur),
            "description": line.description,
            "proof_hash": line.proof_hash,
            "is_verified": (recreated == line.proof_hash),
        }

        if explain and participant:
            line_obj["human_readable_explanation"] = human_readable_explanation(
                participant,
                events_by_participant.get(line.participant_id, []),
                float(line.amount_eur),
                batch.use_case
            )

        payload["settlement_lines"].append(line_obj)

    return payload
