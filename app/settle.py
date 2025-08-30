import hashlib
import json
from collections import defaultdict
from datetime import datetime
from typing import Dict, Any, Tuple, List
from sqlalchemy.orm import Session
from .models import UsageEvent, EventType, SettlementBatch, SettlementLine, Participant

def create_transaction_hash(data: Dict[str, Any]) -> str:
    """Deterministischer Hash für Settlement-Lines."""
    data_to_hash = {
        "batch_id": data.get("batch_id"),
        "participant_id": data.get("participant_id"),
        "amount_eur": round(float(data.get("amount_eur", 0.0)), 2),
        "description": data.get("description"),
    }
    encoded = json.dumps(data_to_hash, sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()

def apply_bilateral_netting(balances: defaultdict, policy_body: dict) -> Tuple[Dict, Dict, Dict]:
    """Einfache bilaterale Netting-Logik."""
    final_balances = {}
    for p_id, bal in balances.items():
        final_balances[p_id] = float(bal["debit"]) - float(bal["credit"])

    stats = {
        "total_participants": len(final_balances),
        "total_debit": sum(b["debit"] for b in balances.values()),
        "total_credit": sum(b["credit"] for b in balances.values()),
    }
    transfers = {}  # Platzhalter für spätere Peer-to-Peer-Transfers
    return final_balances, stats, transfers

def apply_policy_and_settle(
    db: Session,
    use_case: str,
    policy_body: dict,
    events: List[UsageEvent],
    start_time: datetime,
    end_time: datetime,
):
    """Policy anwenden, netten, Settlement-Batch + Lines erzeugen."""
    batch = SettlementBatch(use_case=use_case, start_time=start_time, end_time=end_time)
    db.add(batch)
    db.flush()  # Batch-ID

    participant_map = {p.id: p for p in db.query(Participant).all()}
    balances = defaultdict(lambda: {"credit": 0.0, "debit": 0.0})

    for ev in events:
        p = participant_map.get(ev.participant_id)
        if not p:
            continue
        qty = float(ev.quantity or 0.0)
        price = float((ev.meta or {}).get("price_eur_per_kwh") or 0.0)

        if ev.event_type == EventType.consumption or ev.event_type == EventType.base_fee:
            balances[p.id]["debit"] += qty * price
        elif ev.event_type in (EventType.generation, EventType.grid_feed, EventType.vpp_sale):
            balances[p.id]["credit"] += qty * price
        # battery_charge/discharge optional je nach Policy

    final_net_balances, netting_stats, _ = apply_bilateral_netting(balances, policy_body)

    result = {}
    for p_id, amount in final_net_balances.items():
        desc = f"Nettoabrechnung für {start_time.isoformat()} bis {end_time.isoformat()}"
        payload = {
            "batch_id": batch.id,
            "participant_id": p_id,
            "amount_eur": amount,
            "description": desc,
        }
        proof_hash = create_transaction_hash(payload)
        line = SettlementLine(
            batch_id=batch.id,
            participant_id=p_id,
            amount_eur=amount,
            description=desc,
            proof_hash=proof_hash,
        )
        db.add(line)
        result[p_id] = {"final_net": amount, "proof_hash": proof_hash}

    db.commit()
    return batch, result, netting_stats
