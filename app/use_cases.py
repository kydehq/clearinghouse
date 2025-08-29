from __future__ import annotations

def get_default_policy(use_case: str) -> dict:
    if use_case == "mieterstrom":
        return {
            "local_pv_price_eur_kwh": 0.20,
            "feed_in_price_eur_kwh": 0.08,
            "vpp_sale_price_eur_kwh": 0.10
        }
    return {}

def get_use_case_title(use_case: str) -> str:
    if use_case == "mieterstrom":
        return "Mieterstrom – Mehrparteienhaus"
    return use_case
