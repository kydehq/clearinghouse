# app/use_cases.py
from __future__ import annotations

_USE_CASES = {
    "energy_community": {
        "title": "Energie-Community",
        "default_policy": {
            "use_case": "energy_community",
            "prosumer_sell_price": 0.15,
            "consumer_buy_price": 0.12,
            "community_fee_rate": 0.02,
            "grid_feed_price": 0.08,
        },
    },
    "mieterstrom": {
        "title": "Mieterstrom",
        "default_policy": {
            "use_case": "mieterstrom",
            "tenant_price_per_kwh": 0.18,
            "landlord_revenue_share": 0.60,
            "operator_fee_rate": 0.15,
            "grid_compensation": 0.08,
            "base_fee_per_unit": 5.00,
        },
    },
}

def get_default_policy(case: str) -> dict:
    """Gibt die Standard-Policy für einen gegebenen Anwendungsfall zurück."""
    if case not in _USE_CASES:
        raise ValueError(f"Unbekannter Anwendungsfall: '{case}'")
    return _USE_CASES[case]["default_policy"]

def get_use_case_title(case: str) -> str:
    """Gibt den Titel für einen gegebenen Anwendungsfall zurück."""
    if case not in _USE_CASES:
        raise ValueError(f"Unbekannter Anwendungsfall: '{case}'")
    return _USE_CASES[case]["title"]