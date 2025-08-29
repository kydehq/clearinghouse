# app/models.py
from __future__ import annotations
import enum
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, JSON, ForeignKey, Enum
from sqlalchemy.orm import relationship, Mapped, mapped_column
from sqlalchemy.sql import func
from .db import Base

class ParticipantRole(enum.Enum):
    PROSUMER = "prosumer"
    CONSUMER = "consumer"
    LANDLORD = "landlord"
    TENANT = "tenant"
    OPERATOR = "operator"
    COMMERCIAL = "commercial" 
    COMMUNITY_FEE_COLLECTOR = "community_fee_collector"

class EventType(enum.Enum):
    GENERATION = "generation"
    CONSUMPTION = "consumption"
    BASE_FEE = "base_fee"
    GRID_FEED = "grid_feed"

class Participant(Base):
    __tablename__ = "participants"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    external_id: Mapped[str | None] = mapped_column(String, nullable=True, index=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    role: Mapped[ParticipantRole] = mapped_column(Enum(ParticipantRole), nullable=False)

    events = relationship("UsageEvent", back_populates="participant", cascade="all, delete-orphan")
    settlement_lines = relationship("SettlementLine", back_populates="participant", cascade="all, delete-orphan")
    ledger_entries = relationship("LedgerEntry", back_populates="participant", cascade="all, delete-orphan")

class UsageEvent(Base):
    __tablename__ = "usage_events"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    participant_id: Mapped[int] = mapped_column(Integer, ForeignKey("participants.id"))
    event_type: Mapped[EventType] = mapped_column(Enum(EventType), nullable=False)
    quantity: Mapped[float] = mapped_column(Float, nullable=False)  # kWh oder EUR bei BASE_FEE
    unit: Mapped[str] = mapped_column(String, nullable=False, default="kWh")
    meta: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    timestamp: Mapped[str] = mapped_column(DateTime(timezone=True), server_default=func.now())
    participant = relationship("Participant", back_populates="events")

class Policy(Base):
    __tablename__ = "policies"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    use_case: Mapped[str] = mapped_column(String, index=True)  # 'energy_community' | 'mieterstrom'
    body: Mapped[dict] = mapped_column(JSON)
    created_at: Mapped[str] = mapped_column(DateTime(timezone=True), server_default=func.now())

class SettlementBatch(Base):
    __tablename__ = "settlement_batches"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    use_case: Mapped[str] = mapped_column(String, index=True)
    created_at: Mapped[str] = mapped_column(DateTime(timezone=True), server_default=func.now())
    lines = relationship("SettlementLine", back_populates="batch", cascade="all, delete-orphan")

class SettlementLine(Base):
    __tablename__ = "settlement_lines"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    batch_id: Mapped[int] = mapped_column(Integer, ForeignKey("settlement_batches.id"))
    participant_id: Mapped[int] = mapped_column(Integer, ForeignKey("participants.id"))
    amount_eur: Mapped[float] = mapped_column(Float, nullable=False)  # + wir zahlen an Teilnehmer; - Teilnehmer schuldet
    description: Mapped[str | None] = mapped_column(String, nullable=True)
    batch = relationship("SettlementBatch", back_populates="lines")
    participant = relationship("Participant", back_populates="settlement_lines")

class LedgerEntry(Base):
    __tablename__ = "ledger_entries"
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    settlement_line_id: Mapped[int | None] = mapped_column(Integer, ForeignKey("settlement_lines.id"), nullable=True)
    participant_id: Mapped[int] = mapped_column(Integer, ForeignKey("participants.id"))
    account_type: Mapped[str] = mapped_column(String, nullable=False)  # 'Asset','Liability','Revenue','Expense'
    amount: Mapped[float] = mapped_column(Float, nullable=False)
    is_debit: Mapped[bool] = mapped_column(Boolean, nullable=False)
    timestamp: Mapped[str] = mapped_column(DateTime(timezone=True), server_default=func.now())
    description: Mapped[str | None] = mapped_column(String, nullable=True)
    participant = relationship("Participant", back_populates="ledger_entries")
