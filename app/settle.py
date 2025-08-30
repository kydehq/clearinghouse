from __future__ import annotations
from typing import Dict, Iterable, Tuple, List
from collections import defaultdict
from sqlalchemy.orm import Session
from .models import (
    Participant, UsageEvent, SettlementBatch, SettlementLine,
    ParticipantRole, EventType
)
import hashlib
import json

EPS = 1e-9

def create_transaction_hash(data: Dict) -> str:
    """Creates a SHA-256 hash for a given transaction data."""
    sorted_data = json.dumps(data, sort_keys=True)
    return hashlib.sha256(sorted_data.encode('utf-8')).hexdigest()

def apply_bilateral_netting(
    participants_balances: Dict[int, dict], 
    policy_body: Dict[str, any]
) -> Tuple[Dict[int, float], Dict[str, float], List[Dict]]:
    """Bilaterales Netting mit Policy-Awareness."""
    min_threshold = float(policy_body.get('min_payment_threshold_eur', 0.0))

    internal_netted = {}
    total_abs_before_internal = 0.0

    for pid, balances in participants_balances.items():
        credit = balances.get('credit', 0.0)
        debit = balances.get('debit', 0.0)
        net = credit - debit
        internal_netted[pid] = net
        total_abs_before_internal += abs(credit) + abs(debit)

    total_abs_after_internal = sum(abs(net) for net in internal_netted.values())

    positive_balances = [(pid, amount) for pid, amount in internal_netted.items() if amount > EPS]
    negative_balances = [(pid, -amount) for pid, amount in internal_netted.items() if amount < -EPS]
    positive_balances.sort(key=lambda x: x[1], reverse=True)
    negative_balances.sort(key=lambda x: x[1], reverse=True)

    final_balances = {pid: 0.0 for pid in internal_netted.keys()}
    transfers: List[Dict] = []

    i, j = 0, 0
    while i < len(positive_balances) and j < len(negative_balances):
        creditor_id, credit_amount = positive_balances[i]
        debtor_id, debt_amount = negative_balances[j]
        transfer_amount = min(credit_amount, debt_amount)

        # Apply policy-aware logic: skip if below threshold
        if transfer_amount > min_threshold:
            transfers.append({
                'from_id': debtor_id,
                'to_id': creditor_id,
                'amount_eur': round(transfer_amount, 2)
            })
            positive_balances[i] = (creditor_id, credit_amount - transfer_amount)
            negative_balances[j] = (debtor_id, debt_amount - transfer_amount)
        
        if positive_balances[i][1] < EPS: i += 1
        if negative_balances[j][1] < EPS: j += 1

    for creditor_id, remaining_credit in positive_balances:
        final_balances[creditor_id] = remaining_credit
    for debtor_id, remaining_debt in negative_balances:
        final_balances[debtor_id] = -remaining_debt

    total_abs_after_bilateral = sum(abs(balance) for balance in final_balances.values())
    
    gross_volume = sum(abs(balance['credit']) + abs(balance['debit']) for balance in participants_balances.values())
    net_volume = sum(abs(balance) for balance in final_balances.values())
    netting_efficiency = 1 - (net_volume / gross_volume) if gross_volume > 0 else 0

    netting_stats = {
        'total_transfers': len(transfers),
        'netting_efficiency': netting_efficiency,
        'gross_volume': round(gross_volume, 2),
        'net_volume': round(net_volume, 2),
    }

    return final_balances, netting_stats, transfers

def _ensure_external_market(db: Session) -> Participant:
    external = db.query(Participant).filter(Participant.role == ParticipantRole.EXTERNAL_MARKET).first()
    if external:
        return external
    external = Participant(external_id="EXTERNAL", name="DSO/Market", role=ParticipantRole.EXTERNAL_MARKET)
    db.add(external)
    db.flush()
    return external

def apply_policy_and_settle(
    db: Session,
    use_case: str,
    policy_body: dict,
    events: Iterable[UsageEvent]
) -> Tuple[SettlementBatch, Dict, Dict]:
    if use_case != 'mieterstrom':
        raise ValueError(f"Unbekannter use_case: {use_case}")

    local_pv_price = float(policy_body.get('local_pv_price_eur_kwh', 0.20))
    feed_in_price  = float(policy_body.get('feed_in_price_eur_kwh', 0.08))
    vpp_sale_price = float(policy_body.get('vpp_sale_price_eur_kwh', 0.10))

    landlord = db.query(Participant).filter(Participant.role == ParticipantRole.LANDLORD).first()
    operator = db.query(Participant).filter(Participant.role == ParticipantRole.OPERATOR).first()
    external = _ensure_external_market(db)
    if not landlord:
        raise ValueError("Kein LANDLORD im Datensatz gefunden. (role='landlord')")

    result: Dict[int, Dict[str, float]] = defaultdict(lambda: {'debit': 0.0, 'credit': 0.0})

    for ev in events:
        p = ev.participant
        qty = float(ev.quantity or 0.0)
        meta = ev.meta or {}
        src = (meta.get('source') or '').lower()
        price_meta = float(meta.get('price_eur_per_kwh') or 0.0)

        if p.role in (ParticipantRole.TENANT, ParticipantRole.COMMERCIAL):
            if ev.event_type == EventType.CONSUMPTION and qty > EPS:
                if src in ('local_pv', 'battery', 'local_battery'):
                    cost = qty * local_pv_price
                    result[p.id]['debit'] += cost
                    result[landlord.id]['credit'] += cost
                else:
                    cost = qty * price_meta
                    result[p.id]['debit'] += cost
                    result[external.id]['credit'] += cost
            elif ev.event_type == EventType.BASE_FEE and operator:
                amount_eur = qty
                if abs(amount_eur) > EPS:
                    result[p.id]['debit'] += amount_eur
                    result[operator.id]['credit'] += amount_eur

        elif p.role == ParticipantRole.LANDLORD:
            if ev.event_type == EventType.GRID_FEED and qty > EPS:
                price = price_meta if price_meta > 0 else feed_in_price
                revenue = qty * price
                result[p.id]['credit'] += revenue
                result[external.id]['debit'] += revenue
            elif ev.event_type == EventType.VPP_SALE and qty > EPS:
                price = price_meta if price_meta > 0 else vpp_sale_price
                revenue = qty * price
                result[p.id]['credit'] += revenue
                result[external.id]['debit'] += revenue
            elif ev.event_type == EventType.BATTERY_CHARGE:
                if qty > EPS and src in ('grid_external', 'grid'):
                    cost = qty * price_meta
                    result[p.id]['debit'] += cost
                    result[external.id]['credit'] += cost

    final_balances, netting_stats, transfers = apply_bilateral_netting(result, policy_body)

    participant_result: Dict[int, Dict[str, float]] = {}
    all_ids = set(result.keys()) | set(final_balances.keys())
    for pid in all_ids:
        credit = result.get(pid, {}).get('credit', 0.0)
        debit  = result.get(pid, {}).get('debit', 0.0)
        participant_result[pid] = {
            'credit': credit,
            'debit': debit,
            'net': final_balances.get(pid, credit - debit),
            'final_net': final_balances.get(pid, credit - debit),
        }

    batch = SettlementBatch(use_case=use_case)
    db.add(batch)
    db.flush()
    db.refresh(batch)

    for pid, final_net in final_balances.items():
        if abs(final_net) < EPS: continue
        transaction_data = {
            "batch_id": batch.id,
            "participant_id": pid,
            "amount_eur": round(final_net, 2),
            "description": f"Final net balance for {use_case}"
        }
        transaction_hash = create_transaction_hash(transaction_data)

        db.add(SettlementLine(
            batch_id=batch.id,
            participant_id=pid,
            amount_eur=round(final_net, 2),
            description=f"Net after bilateral netting ({use_case})",
            proof_hash=transaction_hash
        ))
    
    db.commit()

    return batch, participant_result, netting_stats