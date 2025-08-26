# app/db.py
from __future__ import annotations

import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base

# ---------------------------------------------------------------------
# Engine / Session / Base
# ---------------------------------------------------------------------
DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("DATABASE_URL Umgebungsvariable ist nicht gesetzt.")
# Railway liefert teils 'postgres://', SQLAlchemy erwartet 'postgresql://'
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False, future=True)
Base = declarative_base()


def create_db_and_tables() -> None:
    """legt neue Tabellen an (migriert nicht bestehende)."""
    from . import models  # noqa: F401
    Base.metadata.create_all(bind=engine)


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ---------------------------------------------------------------------
# Mini-"Auto-Migration" (idempotent, ohne Bind-Parameter in DDL)
# ---------------------------------------------------------------------
def _column_exists(conn, table: str, column: str) -> bool:
    row = conn.execute(
        text("""
            SELECT 1
            FROM information_schema.columns
            WHERE table_name = :t AND column_name = :c
            LIMIT 1
        """),
        {"t": table, "c": column},
    ).first()
    return row is not None


def _add_json_column_if_missing(conn, table: str, column: str):
    if not _column_exists(conn, table, column):
        conn.execute(text(f'ALTER TABLE {table} ADD COLUMN {column} JSON'))
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


def _drop_column_if_exists(conn, table: str, column: str):
    if _column_exists(conn, table, column):
        conn.execute(text(f"ALTER TABLE {table} DROP COLUMN IF EXISTS {column}"))


def _enum_exists(conn, enum_name: str) -> bool:
    """Prüft ob ein PostgreSQL Enum existiert."""
    row = conn.execute(
        text("""
            SELECT 1
            FROM pg_type
            WHERE typname = :enum_name AND typtype = 'e'
            LIMIT 1
        """),
        {"enum_name": enum_name},
    ).first()
    return row is not None


def _enum_has_value(conn, enum_name: str, value: str) -> bool:
    """Prüft ob ein Enum-Wert in einem PostgreSQL Enum existiert."""
    row = conn.execute(
        text("""
            SELECT 1
            FROM pg_enum pe
            JOIN pg_type pt ON pe.enumtypid = pt.oid
            WHERE pt.typname = :enum_name AND pe.enumlabel = :value
            LIMIT 1
        """),
        {"enum_name": enum_name, "value": value},
    ).first()
    return row is not None


def _ensure_eventtype_enum(conn):
    """Stellt sicher, dass der eventtype Enum alle benötigten Werte hat."""
    required_values = ['GENERATION', 'CONSUMPTION', 'GRID_FEED', 'BASE_FEE']
    
    if not _enum_exists(conn, 'eventtype'):
        # Enum erstellen falls nicht vorhanden
        values_str = "', '".join(required_values)
        conn.execute(text(f"CREATE TYPE eventtype AS ENUM ('{values_str}')"))
        print("Created eventtype enum with all values")
    else:
        # Fehlende Werte hinzufügen
        for value in required_values:
            if not _enum_has_value(conn, 'eventtype', value):
                conn.execute(text(f"ALTER TYPE eventtype ADD VALUE '{value}'"))
                print(f"Added '{value}' to eventtype enum")


def ensure_min_schema():
    """
    Heilt Schema-Drift für PoC-Tabellen:
      - policies: body(JSON), created_at(TIMESTAMPTZ), DROP definition
      - usage_events: quantity(FLOAT), unit(VARCHAR 'kWh'), meta(JSON), timestamp(TIMESTAMPTZ)
      - settlement_batches: use_case(VARCHAR), created_at(TIMESTAMPTZ)
      - settlement_lines: participant_id, batch_id, amount_eur, description
      - eventtype enum: stellt sicher dass alle Werte vorhanden sind
    """
    with engine.begin() as conn:
        # ZUERST den Enum sicherstellen
        _ensure_eventtype_enum(conn)
        
        # policies minimal sichern
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS policies (
                id SERIAL PRIMARY KEY,
                use_case VARCHAR NOT NULL
            );
        """))
        _add_json_column_if_missing(conn,  "policies", "body")
        _add_timestamptz_column_if_missing(conn, "policies", "created_at")
        _drop_column_if_exists(conn, "policies", "definition")

        # settlement_batches sichern
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS settlement_batches (
                id SERIAL PRIMARY KEY
            );
        """))
        _add_varchar_column_if_missing(conn, "settlement_batches", "use_case", "energy_community")
        _add_timestamptz_column_if_missing(conn, "settlement_batches", "created_at")

        # settlement_lines sichern
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS settlement_lines (
                id SERIAL PRIMARY KEY
            );
        """))
        _add_integer_column_if_missing(conn, "settlement_lines", "participant_id", 0)
        _add_integer_column_if_missing(conn, "settlement_lines", "batch_id", 0)
        _add_float_column_if_missing(conn, "settlement_lines", "amount_eur", 0.0)
        _add_varchar_column_if_missing(conn, "settlement_lines", "description", "")

        # usage_events fehlende Spalten nachrüsten
        _add_float_column_if_missing(conn,   "usage_events", "quantity", 0.0)
        _add_varchar_column_if_missing(conn, "usage_events", "unit", "kWh")
        if not _column_exists(conn, "usage_events", "meta"):
            conn.execute(text("ALTER TABLE usage_events ADD COLUMN meta JSON"))
        _add_timestamptz_column_if_missing(conn, "usage_events", "timestamp")