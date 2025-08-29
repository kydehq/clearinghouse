from __future__ import annotations
import json
from fastapi import HTTPException
from sqlalchemy.orm import Session
from . import models

def get_audit_data(db: Session, batch_id: int) -> dict:
    """Holt alle relevanten Daten für einen Audit-Trail zu einem Settlement-Batch."""
    batch = db.query(models.SettlementBatch).filter_by(id=batch_id).first()
    if not batch:
        raise HTTPException(status_code=404, detail=f"Settlement Batch mit ID {batch_id} nicht gefunden.")

    # Zugehörige (letzte) Policy vor Batch-Erstellung
    policy = (
        db.query(models.Policy)
        .filter(models.Policy.use_case == batch.use_case)
        .filter(models.Policy.created_at <= batch.created_at)
        .order_by(models.Policy.created_at.desc())
        .first()
    )

    lines = (
        db.query(models.SettlementLine)
        .filter(models.SettlementLine.batch_id == batch_id)
        .order_by(models.SettlementLine.amount_eur.desc())
        .all()
    )

    return {
        "title": f"Audit-Trail für Batch #{batch.id}",
        "batch": batch,
        "policy": policy,
        "policy_json": json.dumps(policy.body, indent=2, ensure_ascii=False) if policy else "Keine Policy gefunden.",
        "lines": lines,
    }
