from __future__ import annotations
from typing import Dict, Iterable, Tuple, List
from collections import defaultdict
from sqlalchemy.orm import Session
from .models import (
    Participant, UsageEvent, Policy, SettlementBatch, SettlementLine,
    ParticipantRole, EventType
)

EPS = 1e-9

def apply_bilateral_netting(participants_balances: Dict[int, dict]) -> Tuple[Dict[int, float], Dict[str, float]]:
    """
    Bilaterales Netting: reduziert Zahlungsströme auf minimale Transfers.
    Returns:
    - final_balances: {participant_id: net_amount}
    - netting_stats: Kennzahlen + Transferliste (debtor, creditor, amount)
    """
    internal_netted = {}
    total_abs_before_internal = 0.0

    # 1) Teilnehmer-internes Netting (credit - debit)
    for pid, balances in participants_balances.items():
        credit = balances.get('credit', 0.0)
        debit = balances.get('debit', 0.0)
        net = credit - debit
        internal_netted[pid] = net
        total_abs_before_internal += abs(credit) + abs(debit)

    total_abs_after_internal = sum(abs(net) for net in internal_netted.values())

    # 2) Bilaterales Netting zwischen Teilnehmern
    positive_balances = [(pid, amount) for pid, amount in internal_netted.items() if amount > EPS]
    negative_balances = [(pid, -amount) for pid, amount in internal_netted.items() if amount < -EPS]

    positive_balances.sort(key=lambda x: x[1], reverse=True)
    negative_balances.sort(key=lambda x: x[1], reverse=True)

    final_balances = {pid: 0.0 for pid in internal_netted.keys()}
    transfers: List[Tuple[int, int, float]] = []

    i, j = 0, 0
    while i < len(positive_balances) and j < len(negative_balances):
        creditor_id, credit_amount = positive_balances[i]
        debtor_id, debt_amount = negative_balances[j]

        transfer_amount = min(credit_amount, debt_amount)
        if transfer_amount > EPS:
            transfers.append((debtor_id, creditor_id, transfer_amount))

        positive_balances[i] = (creditor_id, credit_amount - transfer_amount)
        negative_balances[j] = (debtor_id, debt_amount - transfer_amount)

        if positive_balances[i][1] < EPS:
            i += 1
        if negative_balances[j][1] < EPS:
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
        'transfers_list': transfers,
    }
    return final_balances, netting_stats

def _ensure_external_market(db: Session) -> Participant:
    """Sorgt dafür, dass ein External-Market-Teilnehmer existiert."""
    external = db.query(Participant).filter(Participant.role == ParticipantRole.EXTERNAL_MARKET).first()
    if external:
        return external
    external = Participant(
        external_id="EXTERNAL",
        name="DSO/Market",
        role=ParticipantRole.EXTERNAL_MARKET,
    )
    db.add(external)
    db.flush()
    return external

def apply_policy_and_settle(
    db: Session,
    use_case: str,
    policy_body: dict,
    events: Iterable[UsageEvent]
) -> Tuple[SettlementBatch, Dict, Dict]:
    """
    Double-Entry-Settlement (Mieterstrom):
      - local_pv/battery -> TENANT/COMMERCIAL zahlen an LANDLORD (Policy-Preis).
      - grid_external    -> TENANT/COMMERCIAL zahlen an EXTERNAL_MARKET (Spot aus CSV).
      - grid_feed        -> EXTERNAL_MARKET zahlt an LANDLORD (Einspeise-Preis).
      - vpp_sale         -> EXTERNAL_MARKET zahlt an LANDLORD (VPP-Preis).
      - base_fee (EUR)   -> TENANT/COMMERCIAL zahlen an OPERATOR.
    """
    if use_case != 'mieterstrom':
        raise ValueError(f"Unbekannter use_case: {use_case}")

    # Preise aus Policy (Fallbacks)
    local_pv_price = float(policy_body.get('local_pv_price_eur_kwh', 0.20))
    feed_in_price = float(policy_body.get('feed_in_price_eur_kwh', 0.08))
    vpp_sale_price = float(policy_body.get('vpp_sale_price_eur_kwh', 0.10))

    # Teilnehmer
    landlord = db.query(Participant).filter(Participant.role == ParticipantRole.LANDLORD).first()
    operator = db.query(Participant).filter(Participant.role == ParticipantRole.OPERATOR).first()
    external = _ensure_external_market(db)

    if not landlord:
        raise ValueError("Kein LANDLORD im Datensatz gefunden. (role='landlord')")

    # Ergebniscontainer: Geldflüsse je Teilnehmer
    result: Dict[int, Dict[str, float]] = defaultdict(lambda: {'debit': 0.0, 'credit': 0.0})

    # 1) Ereignisse in Double-Entry buchen
    for ev in events:
        p = ev.participant
        qty = float(ev.quantity or 0.0)
        meta = ev.meta or {}
        src = (meta.get('source') or '').lower()
        price_meta = float(meta.get('price_eur_per_kwh') or 0.0)

        # --- Mieter & Gewerbe ---
        if p.role in (ParticipantRole.TENANT, ParticipantRole.COMMERCIAL):
            if ev.event_type == EventType.CONSUMPTION and qty > EPS:
                if src in ('local_pv', 'battery', 'local_battery'):
                    # Local PV/Batterie: Mieter -> Landlord
                    cost = qty * local_pv_price
                    result[p.id]['debit'] += cost
                    result[landlord.id]['credit'] += cost
                elif src in ('grid_external', 'grid'):
                    # Grid: Mieter -> External Market (Spot aus CSV)
                    cost = qty * price_meta
                    result[p.id]['debit'] += cost
                    result[external.id]['credit'] += cost
                else:
                    # Fallback: behandle wie Grid mit vorhandenem Preis (ggf. 0)
                    cost = qty * price_meta
                    result[p.id]['debit'] += cost
                    result[external.id]['credit'] += cost

            elif ev.event_type == EventType.BASE_FEE and operator:
                # BASE_FEE ist EUR in 'quantity'
                amount_eur = qty
                if abs(amount_eur) > EPS:
                    result[p.id]['debit'] += amount_eur
                    result[operator.id]['credit'] += amount_eur

        # --- Landlord & Marktinteraktionen ---
        elif p.role == ParticipantRole.LANDLORD:
            if ev.event_type == EventType.GRID_FEED and qty > EPS:
                # External -> Landlord (Einspeisevergütung)
                price = price_meta if price_meta > 0 else feed_in_price
                revenue = qty * price
                result[p.id]['credit'] += revenue
                result[external.id]['debit'] += revenue

            elif ev.event_type == EventType.VPP_SALE and qty > EPS:
                # External -> Landlord (VPP-Erlös)
                price = price_meta if price_meta > 0 else vpp_sale_price
                revenue = qty * price
                result[p.id]['credit'] += revenue
                result[external.id]['debit'] += revenue

            elif ev.event_type == EventType.BATTERY_CHARGE:
                # Positive qty = Charge, negative qty = Discharge (kein Geldfluss für Discharge)
                if qty > EPS and src in ('grid_external', 'grid'):
                    # Landlord kauft Strom vom Markt zum Laden
                    cost = qty * price_meta
                    result[p.id]['debit'] += cost
                    result[external.id]['credit'] += cost
                # Charge aus local_pv -> kein Geldfluss; Discharge -> kein Geldfluss hier

            elif ev.event_type in (EventType.PRODUCTION, EventType.GENERATION):
                # Nur physikalisch, kein Geldfluss
                pass

        # Weitere Rollen bei Bedarf ergänzen

    # 2) Bilaterales Netting anwenden
    final_balances, netting_stats = apply_bilateral_netting(result)

    # 3) Teilnehmer-Result für UI zusammenstellen
    participant_result: Dict[int, Dict[str, float]] = {}
    all_ids = set(result.keys()) | set(final_balances.keys())
    for pid in all_ids:
        credit = result.get(pid, {}).get('credit', 0.0)
        debit = result.get(pid, {}).get('debit', 0.0)
        participant_result[pid] = {
            'credit': credit,
            'debit': debit,
            'net': final_balances.get(pid, credit - debit),
            'final_net': final_balances.get(pid, credit - debit),
        }

    # 4) Batch + SettlementLines persistieren
    batch = SettlementBatch(use_case=use_case)
    db.add(batch)
    db.flush()

    for pid, final_net in final_balances.items():
        if abs(final_net) < EPS:
            continue
        db.add(SettlementLine(
            batch_id=batch.id,
            participant_id=pid,
            amount_eur=round(final_net, 2),
            description=f"Net after bilateral netting ({use_case})",
        ))

    db.commit()
    db.refresh(batch)

    return batch, participant_result, netting_stats
