from __future__ import annotations
from typing import Dict, Tuple, List, Any
from collections import defaultdict
from datetime import datetime
from sqlalchemy.orm import Session

from .models import UsageEvent, SettlementBatch, SettlementLine
from .utils.crypto import create_transaction_hash

# balances = { pid: {"credit": float, "debit": float} }
# final_net = { pid: float }  # >0 = zahlt, <0 = erhält

def _compute_final_balances(balances: Dict[int, Dict[str, float]]) -> Dict[int, float]:
    final_net: Dict[int, float] = {}
    for pid, bd in balances.items():
        debit = float(bd.get("debit", 0.0))
        credit = float(bd.get("credit", 0.0))
        final_net[pid] = round(debit - credit, 10)
    return final_net

def apply_bilateral_netting(
    balances: Dict[int, Dict[str, float]],
    policy_body: Dict[str, Any] | None = None
) -> Tuple[Dict[int, float], Dict[str, Any], List[Dict[str, Any]]]:
    final_net = _compute_final_balances(balances)

    debtors: List[Tuple[int, float]] = [(pid, amt) for pid, amt in final_net.items() if amt > 0.0001]
    creditors: List[Tuple[int, float]] = [(pid, -amt) for pid, amt in final_net.items() if amt < -0.0001]

    # deterministisch
    debtors.sort(key=lambda x: (x[1], x[0]), reverse=True)
    creditors.sort(key=lambda x: (x[1], x[0]), reverse=True)

    transfers: List[Dict[str, Any]] = []
    i, j = 0, 0
    while i < len(debtors) and j < len(creditors):
        d_pid, d_amt = debtors[i]
        c_pid, c_amt = creditors[j]
        pay = min(d_amt, c_amt)

        transfers.append({"from": d_pid, "to": c_pid, "amount_eur": round(pay, 2)})

        d_amt -= pay
        c_amt -= pay
        debtors[i] = (d_pid, d_amt)
        creditors[j] = (c_pid, c_amt)

        if d_amt <= 0.0001:
            i += 1
        if c_amt <= 0.0001:
            j += 1

    stats = {
        "participants": len(final_net),
        "debtors": len([1 for v in final_net.values() if v > 0.0001]),
        "creditors": len([1 for v in final_net.values() if v < -0.0001]),
        "total_owed_eur": round(sum(v for v in final_net.values() if v > 0), 2),
        "total_due_eur": round(sum(-v for v in final_net.values() if v < 0), 2),
        "transfer_count": len(transfers),
    }

    return final_net, stats, transfers

def apply_policy_and_settle(
    db: Session,
    use_case: str,
    policy_body: Dict[str, Any],
    events: List[UsageEvent],
    start_time: datetime,
    end_time: datetime
):
    """
    Erzeugt einen SettlementBatch + SettlementLines.
    Pricing:
      - consumption/base_fee → debit
      - generation/grid_feed/vpp_sale → credit
      - unit==EUR → quantity ist direkt EUR
      - sonst → kWh * price_eur_per_kwh
    """
    balances: Dict[int, Dict[str, float]] = defaultdict(lambda: {"credit": 0.0, "debit": 0.0})

    def add_debit(pid: int, amount_eur: float):
        if amount_eur > 0:
            balances[pid]["debit"] += amount_eur

    def add_credit(pid: int, amount_eur: float):
        if amount_eur > 0:
            balances[pid]["credit"] += amount_eur

    for ev in events:
        price = float((ev.meta or {}).get("price_eur_per_kwh") or 0.0)
        qty = float(ev.quantity or 0.0)
        unit = (ev.unit or "").lower()

        if ev.event_type.value in ("consumption",):
            amount = qty if unit == "eur" else qty * price
            add_debit(ev.participant_id, amount)

        elif ev.event_type.value in ("base_fee",):
            amount = qty if unit in ("eur", "") else qty * price
            add_debit(ev.participant_id, amount)

        elif ev.event_type.value in ("generation", "grid_feed", "vpp_sale"):
            amount = qty if unit == "eur" else qty * price
            add_credit(ev.participant_id, amount)

        # battery_charge/discharge/production sind hier neutral

    final_net, stats, transfers = apply_bilateral_netting(balances, policy_body)

    # Optional: Min-Payout-Threshold aus policy
    threshold = float((policy_body or {}).get("min_payout_eur", 0.0))
    if threshold > 0:
        final_net = {pid: (amt if abs(amt) >= threshold else 0.0) for pid, amt in final_net.items()}

    # Batch
    batch = SettlementBatch(
        use_case=use_case,
        start_time=start_time,
        end_time=end_time,
    )
    db.add(batch)
    db.flush()

    # Lines
    result_data: Dict[int, Dict[str, float]] = {}
    description = f"Settlement {use_case} {start_time.isoformat()} – {end_time.isoformat()}"
    for pid, amount in final_net.items():
        base = {
            "batch_id": batch.id,
            "participant_id": pid,
            "amount_eur": round(float(amount), 2),
            "description": description,
        }
        proof = create_transaction_hash(base)
        line = SettlementLine(
            batch_id=batch.id,
            participant_id=pid,
            amount_eur=base["amount_eur"],
            description=description,
            proof_hash=proof,
        )
        db.add(line)
        result_data[pid] = {"final_net": float(amount)}

    db.commit()
    return batch, result_data, transfers
