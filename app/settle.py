# app/settle.py
from __future__ import annotations
from typing import Dict, Iterable, Tuple
from collections import defaultdict
from sqlalchemy.orm import Session
from .models import (
    Participant, UsageEvent, Policy, SettlementBatch, SettlementLine,
    ParticipantRole, EventType
)

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
                revenue = gen * sell_price + feed * grid_feed_price
                fee = revenue * fee_rate
                result[pid]['credit'] += revenue
                result[pid]['debit'] += fee
            elif p.role == ParticipantRole.CONSUMER:
                cons = vals.get('consumption_kwh', 0.0)
                cost = cons * buy_price
                result[pid]['debit'] += cost

        # Optional: Gebühren-Sammelteilnehmer
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
        # Mieter zahlen
        for pid, vals in agg.items():
            p = participants[pid]
            if p.role == ParticipantRole.TENANT:
                cons = vals.get('consumption_kwh', 0.0)
                base = vals.get('base_fee_eur', 0.0) or base_fee
                cost = cons * tenant_price + base
                result[pid]['debit'] += cost
                total_tenant_payments += cost

        # Landlord & Operator bekommen Einnahmen/Gebühren
        for pid, vals in agg.items():
            p = participants[pid]
            if p.role == ParticipantRole.LANDLORD:
                landlord_rev = total_tenant_payments * landlord_share - grid_compensation
                result[pid]['credit'] += landlord_rev
            elif p.role == ParticipantRole.OPERATOR:
                operator_fee = total_tenant_payments * operator_fee_rate
                result[pid]['credit'] += operator_fee
    else:
        raise ValueError(f"Unbekannter use_case: {use_case}")

    # 3) Netting pro Teilnehmer
    for pid, v in result.items():
        v['net'] = v['credit'] - v['debit']

    # 4) In DB speichern
    batch = SettlementBatch(use_case=use_case)
    db.add(batch)
    db.flush()

    for pid, v in result.items():
        if abs(v['net']) < 1e-9:
            continue
        db.add(SettlementLine(
            batch_id=batch.id,
            participant_id=pid,
            amount_eur=round(v['net'], 2),
            description=f"Net per participant ({use_case})",
        ))

    db.commit()
    db.refresh(batch)
    return batch, result
