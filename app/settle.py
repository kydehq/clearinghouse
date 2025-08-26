# app/settle.py
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
    # Schritt 1: Interne Teilnehmer-Netting (Credit - Debit pro Person)
    internal_netted = {}
    total_abs_before_internal = 0.0
    
    for pid, balances in participants_balances.items():
        credit = balances['credit']
        debit = balances['debit']
        net = credit - debit
        internal_netted[pid] = net
        total_abs_before_internal += abs(credit) + abs(debit)
    
    total_abs_after_internal = sum(abs(net) for net in internal_netted.values())
    
    # Schritt 2: Cross-Participant Bilateral Netting
    # Teilnehmer mit positiven und negativen Salden matchen
    positive_balances = [(pid, amount) for pid, amount in internal_netted.items() if amount > 1e-9]
    negative_balances = [(pid, -amount) for pid, amount in internal_netted.items() if amount < -1e-9]
    
    # Sortieren für optimales Matching
    positive_balances.sort(key=lambda x: x[1], reverse=True)  # Größte zuerst
    negative_balances.sort(key=lambda x: x[1], reverse=True)  # Größte Schulden zuerst
    
    final_balances = {pid: 0.0 for pid in internal_netted.keys()}
    transfers = []  # Für Audit-Trail: [(from_pid, to_pid, amount)]
    
    i, j = 0, 0
    while i < len(positive_balances) and j < len(negative_balances):
        creditor_id, credit_amount = positive_balances[i]
        debtor_id, debt_amount = negative_balances[j]
        
        # Minimaler Transfer zwischen den beiden
        transfer_amount = min(credit_amount, debt_amount)
        
        if transfer_amount > 1e-9:  # Nur relevante Beträge
            transfers.append((debtor_id, creditor_id, transfer_amount))
            
            # Balancen aktualisieren
            positive_balances[i] = (creditor_id, credit_amount - transfer_amount)
            negative_balances[j] = (debtor_id, debt_amount - transfer_amount)
        
        # Verbrauchte Balancen weiterschalten
        if positive_balances[i][1] < 1e-9:
            i += 1
        if negative_balances[j][1] < 1e-9:
            j += 1
    
    # Finale Salden berechnen
    for creditor_id, remaining_credit in positive_balances:
        final_balances[creditor_id] = remaining_credit
    
    for debtor_id, remaining_debt in negative_balances:
        final_balances[debtor_id] = -remaining_debt
    
    # Statistiken
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
) -> Tuple[SettlementBatch, Dict[int, dict]]:
    # 1) Events aggregieren
    agg = defaultdict(lambda: defaultdict(float))
    participants: Dict[int, Participant] = {}
    for ev in events:
        participants[ev.participant_id] = ev.participant
        if ev.event_type == EventType.GENERATION:
            agg[ev.participant_id]['generation_kwh'] += ev.quantity
        elif ev.event_type == EventType.GRID_FEED:
            agg[ev.participant_id]['grid_feed_kwh'] += ev.quantity
        elif ev.event_type == EventType.CONSUMPTION:
            agg[ev.participant_id]['consumption_kwh'] += ev.quantity
        elif ev.event_type == EventType.BASE_FEE:
            agg[ev.participant_id]['base_fee_eur'] += ev.quantity

    # 2) Money-Flows je Use Case
    result = defaultdict(lambda: {'debit': 0.0, 'credit': 0.0, 'net': 0.0})

    if use_case == 'energy_community':
        sell_price = float(policy_body.get('prosumer_sell_price', 0.15))
        buy_price = float(policy_body.get('consumer_buy_price', 0.12))
        fee_rate = float(policy_body.get('community_fee_rate', 0.02))
        grid_feed_price = float(policy_body.get('grid_feed_price', 0.08))

        for pid, vals in agg.items():
            p = participants[pid]
            if p.role == ParticipantRole.PROSUMER:
                gen = vals.get('generation_kwh', 0.0)
                feed = vals.get('grid_feed_kwh', 0.0)
                cons = vals.get('consumption_kwh', 0.0)  # Prosumer verbrauchen auch
                
                # Credits: Generation + Grid Feed
                generation_revenue = gen * sell_price
                grid_feed_revenue = feed * grid_feed_price
                result[pid]['credit'] += generation_revenue + grid_feed_revenue
                
                # Debits: Community Fee + eigener Verbrauch
                community_fee = (generation_revenue + grid_feed_revenue) * fee_rate
                consumption_cost = cons * buy_price
                result[pid]['debit'] += community_fee + consumption_cost
                
            elif p.role == ParticipantRole.CONSUMER:
                cons = vals.get('consumption_kwh', 0.0)
                cost = cons * buy_price
                result[pid]['debit'] += cost

        # Community Fee Collector bekommt alle Gebühren
        collector = db.query(Participant).filter(
            Participant.role == ParticipantRole.COMMUNITY_FEE_COLLECTOR
        ).first()
        if collector:
            total_fee_amount = 0.0
            for pid, vals in agg.items():
                if participants[pid].role == ParticipantRole.PROSUMER:
                    gen = vals.get('generation_kwh', 0.0)
                    feed = vals.get('grid_feed_kwh', 0.0)
                    revenue = gen * sell_price + feed * grid_feed_price
                    total_fee_amount += revenue * fee_rate
            result[collector.id]['credit'] += total_fee_amount

    elif use_case == 'mieterstrom':
        tenant_price = float(policy_body.get('tenant_price_per_kwh', 0.18))
        landlord_share = float(policy_body.get('landlord_revenue_share', 0.60))
        operator_fee_rate = float(policy_body.get('operator_fee_rate', 0.15))
        grid_compensation = float(policy_body.get('grid_compensation', 0.08))
        base_fee = float(policy_body.get('base_fee_per_unit', 5.0))

        total_tenant_payments = 0.0
        total_generation = 0.0
        
        # Mieter zahlen
        for pid, vals in agg.items():
            p = participants[pid]
            if p.role == ParticipantRole.TENANT:
                cons = vals.get('consumption_kwh', 0.0)
                base = vals.get('base_fee_eur', 0.0) or base_fee
                cost = cons * tenant_price + base
                result[pid]['debit'] += cost
                total_tenant_payments += cost

        # Landlord & Operator Flows (können sowohl Credits als auch Debits haben)
        for pid, vals in agg.items():
            p = participants[pid]
            if p.role == ParticipantRole.LANDLORD:
                # Credits: Revenue Share von Tenant Payments
                landlord_revenue = total_tenant_payments * landlord_share
                result[pid]['credit'] += landlord_revenue
                
                # Debits: Grid Compensation für Einspeisung
                gen = vals.get('generation_kwh', 0.0)  # Falls Landlord auch Generation hat
                if gen > 0:
                    grid_comp = gen * grid_compensation
                    result[pid]['debit'] += grid_comp
                else:
                    # Pauschale Grid Compensation
                    result[pid]['debit'] += grid_compensation
                    
            elif p.role == ParticipantRole.OPERATOR:
                operator_fee = total_tenant_payments * operator_fee_rate
                result[pid]['credit'] += operator_fee
    else:
        raise ValueError(f"Unbekannter use_case: {use_case}")

    # 3) Bilateral Netting anwenden
    final_balances, netting_stats = apply_bilateral_netting(result)

    # 4) Erweiterte KPIs berechnen
    for pid, final_net in final_balances.items():
        if pid in result:
            result[pid]['net'] = final_net
            result[pid]['final_net'] = final_net
        else:
            result[pid] = {'credit': 0.0, 'debit': 0.0, 'net': final_net, 'final_net': final_net}

    # Netting-Statistiken zu result hinzufügen
    result['_netting_stats'] = netting_stats

    # 5) In DB speichern (nur finale Netto-Beträge)
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
    return batch, result