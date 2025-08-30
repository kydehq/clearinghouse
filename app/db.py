from __future__ import annotations
import os
import sqlalchemy as sa
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL Umgebungsvariable ist nicht gesetzt.")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False, future=True)
Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ---------- Low-level helpers ----------
def _enum_exists(conn, enum_name: str) -> bool:
    row = conn.execute(
        text("SELECT 1 FROM pg_type WHERE typname = :n AND typtype = 'e' LIMIT 1"),
        {"n": enum_name},
    ).first()
    return row is not None

def _enum_has_value(conn, enum_name: str, value: str) -> bool:
    row = conn.execute(
        text("""
        SELECT 1
        FROM pg_enum e
        JOIN pg_type t ON e.enumtypid = t.oid
        WHERE t.typname = :n AND e.enumlabel = :v
        LIMIT 1
        """),
        {"n": enum_name, "v": value},
    ).first()
    return row is not None

def _ensure_enum_values(conn, enum_name: str, values: list[str]):
    if not _enum_exists(conn, enum_name):
        lit = "', '".join(values)
        conn.execute(text(f"CREATE TYPE {enum_name} AS ENUM ('{lit}')"))
        return
    for v in values:
        if not _enum_has_value(conn, enum_name, v):
            conn.execute(text(f"ALTER TYPE {enum_name} ADD VALUE '{v}'"))

def _column_exists(conn, table: str, column: str) -> bool:
    inspector = sa.inspect(conn)
    return any(col["name"] == column for col in inspector.get_columns(table))

def _add_json_column_if_missing(conn, table: str, column: str):
    if not _column_exists(conn, table, column):
        conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column} JSON"))
    conn.execute(text(f"UPDATE {table} SET {column} = '{{}}'::json WHERE {column} IS NULL"))
    conn.execute(text(f"ALTER TABLE {table} ALTER COLUMN {column} SET DEFAULT '{{}}'::json"))
    conn.execute(text(f"ALTER TABLE {table} ALTER COLUMN {column} SET NOT NULL"))

def _add_timestamptz_column_if_missing(conn, table: str, column: str):
    if not _column_exists(conn, table, column):
        conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column} TIMESTAMPTZ"))
    conn.execute(text(f"UPDATE {table} SET {column} = NOW() WHERE {column} IS NULL"))
    conn.execute(text(f"ALTER TABLE {table} ALTER COLUMN {column} SET DEFAULT NOW()"))
    conn.execute(text(f"ALTER TABLE {table} ALTER COLUMN {column} SET NOT NULL"))

def _add_float_column_if_missing(conn, table: str, column: str, default: float = 0.0):
    if not _column_exists(conn, table, column):
        conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column} DOUBLE PRECISION"))
    conn.execute(text(f"UPDATE {table} SET {column} = {default} WHERE {column} IS NULL"))
    conn.execute(text(f"ALTER TABLE {table} ALTER COLUMN {column} SET DEFAULT {default}"))
    conn.execute(text(f"ALTER TABLE {table} ALTER COLUMN {column} SET NOT NULL"))

def _add_integer_column_if_missing(conn, table: str, column: str, default: int = 0):
    if not _column_exists(conn, table, column):
        conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column} INTEGER"))
    conn.execute(text(f"UPDATE {table} SET {column} = {default} WHERE {column} IS NULL"))
    conn.execute(text(f"ALTER TABLE {table} ALTER COLUMN {column} SET DEFAULT {default}"))
    conn.execute(text(f"ALTER TABLE {table} ALTER COLUMN {column} SET NOT NULL"))

def _add_varchar_column_if_missing(conn, table: str, column: str, default: str = ""):
    if not _column_exists(conn, table, column):
        conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column} VARCHAR"))
    lit = default.replace("'", "''")
    conn.execute(text(f"UPDATE {table} SET {column} = '{lit}' WHERE {column} IS NULL"))
    conn.execute(text(f"ALTER TABLE {table} ALTER COLUMN {column} SET DEFAULT '{lit}'"))
    conn.execute(text(f"ALTER TABLE {table} ALTER COLUMN {column} SET NOT NULL"))

def _add_enum_column_if_missing(conn, table: str, column: str, enum_type: str, default_value: str):
    if not _column_exists(conn, table, column):
        conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column} {enum_type}"))
    # backfill + defaults
    conn.execute(text(f"UPDATE {table} SET {column} = :dv::" + enum_type + f" WHERE {column} IS NULL"), {"dv": default_value})
    conn.execute(text(f"ALTER TABLE {table} ALTER COLUMN {column} SET DEFAULT :dv::" + enum_type), {"dv": default_value})
    conn.execute(text(f"ALTER TABLE {table} ALTER COLUMN {column} SET NOT NULL"))

def ensure_min_schema():
    """Enums/Tables/Spalten sicherstellen + Werte normalisieren."""
    with engine.begin() as conn:
        # Enums (lowercase)
        _ensure_enum_values(conn, "eventtype", [
            "generation", "consumption", "grid_feed", "base_fee",
            "battery_charge", "production", "vpp_sale", "battery_discharge"
        ])
        _ensure_enum_values(conn, "participantrole", [
            "prosumer", "consumer", "landlord", "tenant", "operator",
            "commercial", "community_fee_collector", "external_market"
        ])

        # Tables (stub anlegen, Spalten folgen separat)
        conn.execute(text("CREATE TABLE IF NOT EXISTS participants (id SERIAL PRIMARY KEY)"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS usage_events (id SERIAL PRIMARY KEY)"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS policies (id SERIAL PRIMARY KEY)"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS settlement_batches (id SERIAL PRIMARY KEY)"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS settlement_lines (id SERIAL PRIMARY KEY)"))
        conn.execute(text("CREATE TABLE IF NOT EXISTS ledger_entries (id SERIAL PRIMARY KEY)"))

        # participants
        _add_varchar_column_if_missing(conn, "participants", "external_id", "")
        _add_varchar_column_if_missing(conn, "participants", "name", "")
        _add_enum_column_if_missing(conn, "participants", "role", "participantrole", "consumer")
        conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS participants_external_id_idx ON participants(external_id)"))

        # usage_events
        _add_integer_column_if_missing(conn, "usage_events", "participant_id", 0)
        if not _column_exists(conn, "usage_events", "meta"):
            conn.execute(text("ALTER TABLE usage_events ADD COLUMN meta JSON"))
        conn.execute(text("ALTER TABLE usage_events ALTER COLUMN meta SET DEFAULT '{}'::json"))
        _add_enum_column_if_missing(conn, "usage_events", "event_type", "eventtype", "consumption")
        _add_float_column_if_missing(conn, "usage_events", "quantity", 0.0)
        _add_varchar_column_if_missing(conn, "usage_events", "unit", "kWh")
        _add_timestamptz_column_if_missing(conn, "usage_events", "timestamp")

        # policies
        _add_varchar_column_if_missing(conn, "policies", "use_case", "mieterstrom")
        _add_json_column_if_missing(conn, "policies", "body")
        _add_timestamptz_column_if_missing(conn, "policies", "created_at")

        # settlement_batches
        _add_varchar_column_if_missing(conn, "settlement_batches", "use_case", "mieterstrom")
        _add_timestamptz_column_if_missing(conn, "settlement_batches", "created_at")
        _add_timestamptz_column_if_missing(conn, "settlement_batches", "start_time")
        _add_timestamptz_column_if_missing(conn, "settlement_batches", "end_time")

        # settlement_lines
        _add_integer_column_if_missing(conn, "settlement_lines", "participant_id", 0)
        _add_integer_column_if_missing(conn, "settlement_lines", "batch_id", 0)
        _add_float_column_if_missing(conn, "settlement_lines", "amount_eur", 0.0)
        _add_varchar_column_if_missing(conn, "settlement_lines", "description", "")
        _add_varchar_column_if_missing(conn, "settlement_lines", "proof_hash", "")

        # ledger_entries
        _add_integer_column_if_missing(conn, "ledger_entries", "sender_id", 0)
        _add_integer_column_if_missing(conn, "ledger_entries", "receiver_id", 0)
        _add_integer_column_if_missing(conn, "ledger_entries", "batch_id", 0)
        _add_float_column_if_missing(conn, "ledger_entries", "amount_eur", 0.0)
        _add_varchar_column_if_missing(conn, "ledger_entries", "transaction_hash", "")

        # ---- Daten normalisieren (wichtiger Teil) ----
        # Mögliche Altlasten in Großbuchstaben auf lowercase-Enums mappen
        conn.execute(text("""
            UPDATE participants
            SET role = LOWER(role::text)::participantrole
            WHERE role::text <> LOWER(role::text)
        """))
        conn.execute(text("""
            UPDATE usage_events
            SET event_type = LOWER(event_type::text)::eventtype
            WHERE event_type::text <> LOWER(event_type::text)
        """))
