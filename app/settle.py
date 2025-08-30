import hashlib
import json
from collections import defaultdict
from sqlalchemy.orm import Session
from datetime import datetime
from typing import Dict, Any, Tuple

from .models import UsageEvent, EventType, SettlementBatch, SettlementLine, Participant, LedgerEntry

def create_transaction_hash(data: Dict[str, Any]) -> str:
    """Creates a deterministic hash of the transaction data."""
    # Ensure all data types are consistent for hashing
    data_to_hash = {
        'batch_id': data.get('batch_id'),
        'participant_id': data.get('participant_id'),
        'amount_eur': round(data.get('amount_eur', 0.0), 2), # Round to ensure consistent hashing
        'description': data.get('description')
    }
    encoded_data = json.dumps(data_to_hash, sort_keys=True).encode('utf-8')
    return hashlib.sha256(encoded_data).hexdigest()

def apply_bilateral_netting(balances: defaultdict, policy_body: dict) -> Tuple[Dict, Dict, Dict]:
    """Applies bilateral netting logic based on balances and policy rules."""
    final_balances = defaultdict(float)
    transfers = defaultdict(float)
    
    # Simple bilateral netting for now
    for p_id, bal in balances.items():
        net_balance = bal['debit'] - bal['credit']
        final_balances[p_id] = net_balance
    
    stats = {
        'total_participants': len(final_balances),
        'total_debit': sum(b['debit'] for b in balances.values()),
        'total_credit': sum(b['credit'] for b in balances.values()),
    }
    
    return final_balances, stats, transfers


def apply_policy_and_settle(db: Session, use_case: str, policy_body: dict, events: list[UsageEvent], start_time: datetime, end_time: datetime):
    """
    Applies the given policy to a list of events, performs netting,
    and generates a verifiable settlement batch.
    """
    # 1. Erstelle einen Abrechnungs-Batch (Finalität)
    batch = SettlementBatch(use_case=use_case, start_time=start_time, end_time=end_time)
    db.add(batch)
    db.flush() # Wichtig, um die Batch-ID zu erhalten

    # 2. Aggregiere die Events nach Teilnehmern und berechne Rohtransaktionen
    participant_map = {p.id: p for p in db.query(Participant).all()}
    aggregated_balances = defaultdict(lambda: {'credit': 0.0, 'debit': 0.0})
    
    for event in events:
        p = participant_map.get(event.participant_id)
        if not p:
            continue
        
        qty = float(event.quantity or 0.0)
        price = float(event.meta.get('price_eur_per_kwh') or 0.0)
        
        if event.event_type.value == 'consumption':
            aggregated_balances[p.id]['debit'] += qty * price
        elif event.event_type.value == 'generation':
            aggregated_balances[p.id]['credit'] += qty * price

    # 3. Wende Netting-Logik an
    final_net_balances, netting_stats, _ = apply_bilateral_netting(aggregated_balances, policy_body)
    
    # 4. Erstelle Settlement Lines und Hashes
    result_data = {}
    for p_id, final_amount in final_net_balances.items():
        description = f"Nettoabrechnung für {start_time.isoformat()} bis {end_time.isoformat()}"
        
        # Erstelle ein Daten-Wörterbuch für den Hash
        transaction_data = {
            "batch_id": batch.id,
            "participant_id": p_id,
            "amount_eur": final_amount,
            "description": description
        }
        
        # Generiere den Hash für den Proof-Layer
        proof_hash = create_transaction_hash(transaction_data)
        
        # Erstelle die Settlement Line
        line = SettlementLine(
            batch_id=batch.id,
            participant_id=p_id,
            amount_eur=final_amount,
            description=description,
            proof_hash=proof_hash
        )
        db.add(line)
        result_data[p_id] = {
            "final_net": final_amount,
            "proof_hash": proof_hash
        }

    db.commit()
    return batch, result_data, netting_stats