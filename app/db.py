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
# Railway liefert manchmal 'postgres://', SQLAlchemy erwartet 'postgresql://'
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DATABASE_URL, pool_pre_ping=True, future=True)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False, future=True)
Base = declarative_base()


def create_db_and_tables() -> None:
    """
    Legt Tabellen gemäß SQLAlchemy-Models an (nur neue Tabellen/Spalten).
    Migriert KEINE bestehenden Tabellen – dafür gibt es ensure_min_schema().
    """
    # Wichtig: Models importieren, damit Base alle Klassen kennt.
    from . import models  # noqa: F401
    Base.metadata.create_all(bind=engine)


def get_db():
    """FastAPI-Dependency für eine DB-Session."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ---------------------------------------------------------------------
# Leichte "Auto-Migration" für Policies (Schema-Drift heilen)
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


def _add_json_column_if_missing(conn, table: str, column: str, default_json: str = "{}"):
    # Spalte hinzufügen, falls sie fehlt
    if not _column_exists(conn, table, column):
        conn.execute(
            text(f"ALTER TABLE {table} ADD COLUMN {column} JSON DEFAULT :d::json"),
            {"d": default_json},
        )
    # Nulls füllen, Default + NOT NULL setzen
    conn.execute(text(f"UPDATE {table} SET {column} = :d::json WHERE {column} IS NULL"), {"d": default_json})
    conn.execute(text(f"ALTER TABLE {table} ALTER COLUMN {column} SET DEFAULT :d::json"), {"d": default_json})
    conn.execute(text(f"ALTER TABLE {table} ALTER COLUMN {column} SET NOT NULL"))


def _add_timestamptz_column_if_missing(conn, table: str, column: str):
    if not _column_exists(conn, table, column):
        conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column} TIMESTAMPTZ DEFAULT NOW()"))
    conn.execute(text(f"UPDATE {table} SET {column} = NOW() WHERE {column} IS NULL"))
    conn.execute(text(f"ALTER TABLE {table} ALTER COLUMN {column} SET DEFAULT NOW()"))
    conn.execute(text(f"ALTER TABLE {table} ALTER COLUMN {column} SET NOT NULL"))


def _drop_column_if_exists(conn, table: str, column: str):
    # IF EXISTS ist idempotent; separat prüfen ist optional – wir prüfen trotzdem.
    if _column_exists(conn, table, column):
        conn.execute(text(f"ALTER TABLE {table} DROP COLUMN IF EXISTS {column}"))


def ensure_min_schema():
    """
    Für immer Ruhe:
    - Stellt sicher, dass die Tabelle 'policies' existiert (falls nicht, minimal anlegen).
    - Fügt 'body' (JSON NOT NULL DEFAULT '{}') hinzu, falls fehlend.
    - Fügt 'created_at' (TIMESTAMPTZ NOT NULL DEFAULT NOW()) hinzu, falls fehlend.
    - Entfernt Alt-Spalte 'definition', falls vorhanden.
    Mehr brauchst du für den PoC/Prototype nicht.
    """
    with engine.begin() as conn:
        # Minimalen Tabellenrumpf sicherstellen (falls create_all() zuvor nicht lief)
        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS policies (
                id SERIAL PRIMARY KEY,
                use_case VARCHAR NOT NULL
            );
        """))
        # Zielschema sicherstellen
        _add_json_column_if_missing(conn,  "policies", "body")
        _add_timestamptz_column_if_missing(conn, "policies", "created_at")
        # Legacy-Feld beseitigen
        _drop_column_if_exists(conn, "policies", "definition")
