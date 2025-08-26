# app/main.py
from __future__ import annotations

import io
import json
import traceback
from pathlib import Path
from typing import List

import pandas as pd
from fastapi import FastAPI, Request, Depends, UploadFile, File, Form, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session

from .db import create_db_and_tables, get_db
# Robust: ensure_min_schema optional import
try:
    from .db import ensure_min_schema  # wird aufgerufen, wenn vorhanden
except Exception:
    def ensure_min_schema():
        # Fallback: nichts tun, nur nicht crashen
        print("ensure_min_schema() not found — skipping self-heal on startup.")


from .models import (
    Participant,
    ParticipantRole,
    UsageEvent,
    EventType,
    Policy,
    SettlementBatch,
    SettlementLine,
)
from .settle import apply_policy_and_settle

# -----------------------------------------------------------------------------
# App & Templates/Static
# -----------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR = BASE_DIR / "static"

app = FastAPI(title="KYDE PoC", debug=True)

# Jinja Templates (mit Fallback, falls Ordner anders liegt)
templates = Jinja2Templates(directory=str(TEMPLATES_DIR if TEMPLATES_DIR.exists() else Path("templates")))

# Static (optional)
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# -----------------------------------------------------------------------------
# Startup: DB Tabellen anlegen
# -----------------------------------------------------------------------------
@app.on_event("startup")
def on_startup():
    create_db_and_tables()
    ensure_min_schema()


# -----------------------------------------------------------------------------
# Helper: CSV robust einlesen
# -----------------------------------------------------------------------------
def read_csv_robust(content: bytes) -> pd.DataFrame:
    """
    Versucht, CSV mit auto-Delimiter, UTF-8/BOM & Fallbacks einzulesen.
    """
    tries = [
        dict(sep=None, engine="python", encoding="utf-8-sig"),
        dict(sep=";", encoding="utf-8-sig"),
        dict(sep=",", encoding="utf-8-sig"),
    ]
    for params in tries:
        try:
            return pd.read_csv(io.BytesIO(content), **params)
        except Exception:
            pass
    # Fallback: erst als Text decodieren
    text = content.decode("utf-8", errors="ignore")
    return pd.read_csv(io.StringIO(text), sep=None, engine="python")


def to_float_safe(x) -> float:
    """
    Konvertiert Strings mit Komma als Dezimaltrenner nach float.
    Leere/ungültige Werte -> 0.0
    """
    if x is None:
        return 0.0
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip()
    if not s:
        return 0.0
    # deutsches Komma erlauben
    s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0


# -----------------------------------------------------------------------------
# Routes
# -----------------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
def case_selector(request: Request):
    # Deine vorhandene Template-Datei: case_selector.html
    return templates.TemplateResponse(
        "case_selector.html",
        {"request": request, "title": "Anwendungsfall auswählen"},
    )


@app.get("/upload", response_class=HTMLResponse)
def upload_page(request: Request, case: str = "energy_community"):
    if case not in ("energy_community", "mieterstrom"):
        raise HTTPException(status_code=400, detail="Unknown case")

    default_policy = {
        "energy_community": {
            "use_case": "energy_community",
            "prosumer_sell_price": 0.15,
            "consumer_buy_price": 0.12,
            "community_fee_rate": 0.02,
            "grid_feed_price": 0.08,
        },
        "mieterstrom": {
            "use_case": "mieterstrom",
            "tenant_price_per_kwh": 0.18,
            "landlord_revenue_share": 0.60,
            "operator_fee_rate": 0.15,
            "grid_compensation": 0.08,
            "base_fee_per_unit": 5.00,
        },
    }[case]

    # Deine vorhandene Template-Datei: uploads.html
    return templates.TemplateResponse(
        "uploads.html",
        {
            "request": request,
            "title": f"{case} – CSV & Policy",
            "case": case,
            "default_policy_json": json.dumps(default_policy, indent=2, ensure_ascii=False),
        },
    )


@app.post("/process_data", response_class=HTMLResponse)
def process_data(
    request: Request,
    csv_file: UploadFile = File(...),
    policy_json_str: str = Form(...),
    case: str = Form(...),
    db: Session = Depends(get_db),
):
    try:
        # Policy laden
        try:
            policy = json.loads(policy_json_str)
        except json.JSONDecodeError as e:
            raise HTTPException(status_code=400, detail=f"Policy JSON ungültig: {e}")

        if case not in ("energy_community", "mieterstrom"):
            raise HTTPException(status_code=400, detail="Unknown case")

        # CSV lesen & Spalten normalisieren
        content = csv_file.file.read()
        df = read_csv_robust(content)
        # Debug: Spalten anzeigen (Railway Logs)
        print("CSV columns (raw):", df.columns.tolist())

        df.columns = [str(c).strip().lower().replace("\ufeff", "") for c in df.columns]
        print("CSV columns (normalized):", df.columns.tolist())

        required = {"name", "role"}
        if not required.issubset(set(df.columns)):
            raise HTTPException(status_code=400, detail="CSV muss Spalten 'name' und 'role' enthalten.")

        measurement_cols: List[str] = [
            "generation_kwh",
            "grid_feed_kwh",
            "consumption_kwh",
            "base_fee_eur",
        ]
        if not any(col in df.columns for col in measurement_cols):
            raise HTTPException(
                status_code=400,
                detail="CSV braucht mind. eine Mess-Spalte (generation_kwh / grid_feed_kwh / consumption_kwh / base_fee_eur).",
            )

        # Messwerte in floats konvertieren (Komma zulassen)
        for col in measurement_cols:
            if col in df.columns:
                df[col] = df[col].apply(to_float_safe)

        # Teilnehmer + Events anlegen
        events_to_add: List[UsageEvent] = []
        for _, row in df.iterrows():
            role_str = str(row["role"]).strip().lower()
            try:
                role = ParticipantRole(role_str)
            except Exception:
                raise HTTPException(status_code=400, detail=f"Unbekannte Rolle: {role_str}")

            name = str(row["name"]).strip()
            if not name:
                raise HTTPException(status_code=400, detail="Ungültiger Teilnehmer: 'name' fehlt.")

            # Teilnehmer holen/erstellen
            p = db.query(Participant).filter_by(name=name, role=role).first()
            if not p:
                p = Participant(name=name, role=role)
                db.add(p)
                db.flush()  # ID verfügbar machen

            # Helper zum Erstellen eines Events
            def add_event(kind: EventType, qty: float, unit: str):
                qty = float(qty)
                if qty == 0.0:
                    return
                ev = UsageEvent(participant_id=p.id, event_type=kind, quantity=qty, unit=unit)
                events_to_add.append(ev)

            # Events je nach vorhandenen Spalten
            if "generation_kwh" in df.columns:
                add_event(EventType.GENERATION, row.get("generation_kwh", 0.0), "kWh")
            if "grid_feed_kwh" in df.columns:
                add_event(EventType.GRID_FEED, row.get("grid_feed_kwh", 0.0), "kWh")
            if "consumption_kwh" in df.columns:
                add_event(EventType.CONSUMPTION, row.get("consumption_kwh", 0.0), "kWh")
            if "base_fee_eur" in df.columns:
                add_event(EventType.BASE_FEE, row.get("base_fee_eur", 0.0), "EUR")

        # Persistiere neue Events + Policy
        if events_to_add:
            db.add_all(events_to_add)
        pol = Policy(use_case=case, body=policy)
        db.add(pol)
        db.commit()

        # Alle Events (dieser Demo) laden und Settlement fahren
        evs = db.query(UsageEvent).join(Participant).all()
        batch, per_participant = apply_policy_and_settle(db, case, policy, evs)

        # KPIs berechnen
        totals = {
            "participants": len(per_participant),
            "sum_credit": round(sum(v["credit"] for v in per_participant.values()), 2),
            "sum_debit": round(sum(v["debit"] for v in per_participant.values()), 2),
            "sum_abs_before_net": round(
                sum(abs(v["credit"]) + abs(v["debit"]) for v in per_participant.values()), 2
            ),
            "sum_abs_after_net": round(sum(abs(v["net"]) for v in per_participant.values()), 2),
        }
        totals["netting_efficiency"] = (
            round(1 - (totals["sum_abs_after_net"] / totals["sum_abs_before_net"]), 2)
            if totals["sum_abs_before_net"]
            else 0.0
        )

        # Tabellenzeilen für Template
        rows = []
        for pid, v in per_participant.items():
            p = db.get(Participant, pid)
            rows.append(
                {
                    "name": p.name,
                    "role": p.role.value,
                    "credit_eur": round(v["credit"], 2),
                    "debit_eur": round(v["debit"], 2),
                    "net_eur": round(v["net"], 2),
                }
            )
        rows = sorted(rows, key=lambda r: r["net_eur"], reverse=True)

        # Deine results.html zeigt KPIs + Tabelle
        return templates.TemplateResponse(
            "results.html",
            {
                "request": request,
                "title": "Ergebnisse",
                "case": case,
                "batch_id": batch.id,
                "kpis": totals,
                "rows": rows,
            },
        )

    except HTTPException as he:
        # Freundliche Fehlermeldung ins Template
        return templates.TemplateResponse(
            "results.html",
            {
                "request": request,
                "title": "Fehler",
                "message": f"HTTP-Fehler: {he.status_code}",
                "error": str(he.detail),
            },
            status_code=he.status_code,
        )
    except Exception:
        tb = traceback.format_exc()
        print(tb)  # erscheint in Railway Logs
        return templates.TemplateResponse(
            "results.html",
            {
                "request": request,
                "title": "Interner Fehler",
                "message": "Beim Verarbeiten ist ein Fehler aufgetreten.",
                "error": tb,
            },
            status_code=500,
        )


@app.get("/audit", response_class=HTMLResponse)
def audit_page(request: Request, batch_id: int, db: Session = Depends(get_db)):
    batch = db.get(SettlementBatch, batch_id)
    if not batch:
        raise HTTPException(status_code=404, detail="Batch nicht gefunden")
    lines = db.query(SettlementLine).filter(SettlementLine.batch_id == batch_id).all()

    data = []
    for ln in lines:
        p = db.get(Participant, ln.participant_id)
        data.append(
            {
                "name": p.name,
                "role": p.role.value,
                "amount_eur": round(ln.amount_eur, 2),
                "description": ln.description,
            }
        )

    # Re-Use von results.html (zeigt hier nur die Tabelle/Audit)
    return templates.TemplateResponse(
        "results.html",
        {
            "request": request,
            "title": f"Audit Batch #{batch_id}",
            "case": batch.use_case,
            "batch_id": batch.id,
            "kpis": None,
            # Map auf gleiche Keys wie in process_data (damit Template-Cells stimmen)
            "rows": [
                {
                    "name": r["name"],
                    "role": r["role"],
                    "credit_eur": 0.0,
                    "debit_eur": 0.0,
                    "net_eur": r["amount_eur"],
                }
                for r in data
            ],
        },
    )
