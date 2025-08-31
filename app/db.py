from __future__ import annotations
import os
import sqlalchemy as sa
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, declarative_base

# ---- Konfiguration ----
DATABASE_URL = os.getenv("DATABASE_URL", "")
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

Base = declarative_base()

_engine = None
SessionLocal = None

def _make_engine():
    """Erzeuge Engine nur, wenn eine DB konfiguriert ist."""
    global _engine, SessionLocal
    if not DATABASE_URL:
        print("[db] No DATABASE_URL set. Running without DB.")
        return None
    # kurzer Connect-Timeout, damit Deploy nicht hängt
    connect_args = {}
    if DATABASE_URL.startswith("postgresql://"):
        connect_args["connect_timeout"] = 5
    _engine = create_engine(
        DATABASE_URL,
        pool_pre_ping=True,
        future=True,
        connect_args=connect_args
    )
    SessionLocal = sessionmaker(bind=_engine, autocommit=False, autoflush=False, future=True)
    return _engine

# Lazy init
_make_engine()

def get_db():
    if SessionLocal is None:
        raise RuntimeError("Database not configured. Set DATABASE_URL or skip endpoints that need DB.")
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ---------- Helpers aus deiner bisherigen Datei (unverändert) ----------
def _enum_exists(conn, enum_name: str) -> bool:
    row = conn.execute(text("SELECT 1 FROM pg_type WHERE typname = :n AND typtype = 'e' LIMIT 1"),
                       {"n": enum_name}).first()
    return row is not None

def _enum_has_value(conn, enum_name: str, value: str) -> bool:
    row = conn.execute(text("""
        SELECT 1
        FROM pg_enum e
        JOIN pg_type t ON e.enumtypid = t.oid
        WHERE t.typname = :n AND e.enumlabel = :v
        LIMIT 1
        """), {"n": enum_name, "v": value}).first()
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

# ... (deine bestehenden _add_* Helper unverändert hier lassen) ...

def ensure_min_schema():
    """Nur ausführen, wenn eine Engine existiert (sonst freundlich skippen)."""
    if _engine is None:
        print("[db] ensure_min_schema skipped (no DB).")
        return
    with _engine.begin() as conn:
        # --- dein bisheriger ensure_min_schema Body unverändert ---
        # Enums, Tables, Columns, Updates ...
        # (lasse hier deinen bestehenden Code stehen)
        pass  # <== WICHTIG: diesen pass entfernen und deinen bisherigen Body beibehalten
