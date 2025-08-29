from __future__ import annotations
from typing import Dict, Iterable, Tuple, List
from collections import defaultdict
from sqlalchemy.orm import Session
from .models import (
    Participant, UsageEvent, Policy, SettlementBatch, SettlementLine,
    ParticipantRole, EventType
)

def apply_bilateral_netting(participants_balances: Dict[int, dict]) -> Tuple[Dict[int, float], Dict[str, float]]:
    """
    Führt bilaterales Netting zwischen Teilnehmern durch.
    Returns:
    - final_balances: {participant_id: net_amount} nach Cross-Participant Netting
    - netting_stats: Statistiken über die Netting-Effizienz
    """
    internal_netted = {}
    total_abs_before_internal = 0.0
    for pid, balances in participants_balances.items():
        credit = balances.get('credit', 0.0)
        debit = balances.get('debit', 0.0)
        net = credit - debit
        internal_netted[pid] = net
        total_abs_before_internal += abs(credit) + abs(debit)
    total_abs_after_internal = sum(abs(net) for net in internal_netted.values())
    
    positive_balances = [(pid, amount) for pid, amount in internal_netted.items() if amount > 1e-9]
    negative_balances = [(pid, -amount) for pid, amount in internal_netted.items() if amount < -1e-9]
    
    positive_balances.sort(key=lambda x: x[1], reverse=True)
    negative_balances.sort(key=lambda x: x[1], reverse=True)
    final_balances = {pid: 0.0 for pid in internal_netted.keys()}
    transfers = []
    i, j = 0, 0
    while i < len(positive_balances) and j < len(negative_balances):
        creditor_id, credit_amount = positive_balances[i]
        debtor_id, debt_amount = negative_balances[j]
        
        transfer_amount = min(credit_amount, debt_amount)
        if transfer_amount > 1e-9:
            transfers.append((debtor_id, creditor_id, transfer_amount))
        
        positive_balances[i] = (creditor_id, credit_amount - transfer_amount)
        negative_balances[j] = (debtor_id, debt_amount - transfer_amount)
        
        if positive_balances[i][1] < 1e-9:
            i += 1
        if negative_balances[j][1] < 1e-9:
            j += 1
            
    for creditor_id, remaining_credit in positive_balances:
        final_balances[creditor_id] = remaining_credit
    for debtor_id, remaining_debt in negative_balances:
        final_balances[debtor_id] = -remaining_debt
        
    total_abs_after_bilateral = sum(abs(balance) for balance in final_balances.values())
    netting_stats = {
        'total_transfers': len(transfers),
        'internal_netting_efficiency': 1 - (total_abs_after_internal / total_abs_before_internal) if total_abs_before_internal > 0 else 0,
        'bilateral_netting_efficiency': 1 - (total_abs_after_bilateral / total_abs_after_internal) if total_abs_after_internal > 0 else 0,
        'overall_netting_efficiency': 1 - (total_abs_after_bilateral / total_abs_before_internal) if total_abs_before_internal > 0 else 0,
        'volume_reduction': total_abs_before_internal - total_abs_after_bilateral,
        'transfers_list': transfers
    }
    return final_balances, netting_stats

def apply_policy_and_settle(
    db: Session,
    use_case: str,
    policy_body: dict,
    events: Iterable[UsageEvent]
) -> Tuple[SettlementBatch, Dict]:
    # 1) Money-Flows nach Policy je Event berechnen
    result = defaultdict(lambda: {'debit': 0.0, 'credit': 0.0})
    
    if use_case == 'mieterstrom':
        landlord_revenue_share = float(policy_body.get('landlord_revenue_share', 0.60))
        operator_fee_rate = float(policy_body.get('operator_fee_rate', 0.15))
        
        # Summe der Verbrauchs-Zahlungen für die Verteilung
        total_consumption_payments = 0.0
        
        # 2) Erster Durchlauf: Einzelne Events verarbeiten
        for ev in events:
            p = ev.participant
            
            # Mieter zahlen ihren Verbrauch an die Gemeinschaft
            if p.role == ParticipantRole.TENANT:
                if ev.event_type == EventType.CONSUMPTION:
                    price = ev.meta.get('price_eur_per_kwh', 0.0)
                    cost = ev.quantity * price
                    result[p.id]['debit'] += cost
                    total_consumption_payments += cost
                elif ev.event_type == EventType.BASE_FEE:
                    # Grundgebühr ist eine direkte Zahlung vom Mieter an den Operator
                    operator = db.query(Participant).filter(Participant.role == ParticipantRole.OPERATOR).first()
                    if operator:
                        result[p.id]['debit'] += ev.quantity
                        result[operator.id]['credit'] += ev.quantity
            
            # Vermieter hat Generation & Einspeisung
            elif p.role == ParticipantRole.LANDLORD:
                if ev.event_type == EventType.GRID_FEED:
                    revenue = ev.quantity * ev.meta.get('price_eur_per_kwh', 0.0)
                    result[p.id]['credit'] += revenue
        
        # 3) Zweiter Durchlauf: Verteilungslogik anwenden, nur auf Basis der Verbrauchszahlungen
        landlord = db.query(Participant).filter(Participant.role == ParticipantRole.LANDLORD).first()
        operator = db.query(Participant).filter(Participant.role == ParticipantRole.OPERATOR).first()

        # Einnahmen für Vermieter & Operator
        if landlord:
            landlord_revenue = total_consumption_payments * landlord_revenue_share
            result[landlord.id]['credit'] += landlord_revenue
        
        if operator:
            operator_fee = total_consumption_payments * operator_fee_rate
            result[operator.id]['credit'] += operator_fee

    else:
        raise ValueError(f"Unbekannter use_case: {use_case}")

    # 4) Bilateral Netting anwenden
    final_balances, netting_stats = apply_bilateral_netting(result)

    # 5) KPIs berechnen und Daten für die UI aufbereiten
    participant_result = {}
    for pid in final_balances.keys():
        participant_result[pid] = {
            'credit': result.get(pid, {}).get('credit', 0.0),
            'debit': result.get(pid, {}).get('debit', 0.0),
            'net': final_balances[pid],
            'final_net': final_balances[pid]
        }
    
    # In DB speichern
    batch = SettlementBatch(use_case=use_case)
    db.add(batch)
    db.flush()
    for pid, final_net in final_balances.items():
        if abs(final_net) < 1e-9:
            continue
        db.add(SettlementLine(
            batch_id=batch.id,
            participant_id=pid,
            amount_eur=round(final_net, 2),
            description=f"Net after bilateral netting ({use_case})",
        ))
    db.commit()
    db.refresh(batch)

    return batch, participant_result