from __future__ import annotations
import hashlib
import json

def create_transaction_hash(base: dict) -> str:
    """
    Deterministischer Hash über ein JSON-Objekt.
    - sort_keys: stabile Reihenfolge
    - separators: kompakt
    - default=str: z.B. datetime/Decimal serialisierbar
    """
    try:
        payload = json.dumps(base, sort_keys=True, separators=(",", ":"), default=str)
    except TypeError:
        payload = json.dumps({k: str(v) for k, v in base.items()},
                             sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()
