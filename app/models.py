from __future__ import annotations
import enum
from sqlalchemy import (
    Column, Integer, String, DateTime, Enum, Float, JSON, ForeignKey, text
)
from sqlalchemy.orm import relationship
from .db import Base

class ParticipantRole(str, enum.Enum):
    prosumer = "prosumer"
    consumer = "consumer"
    landlord = "landlord"
    tenant = "tenant"
    operator = "operator"
    commercial = "commercial"
    community_fee_collector = "community_fee_collector"
    external_market = "external_market"

class EventType(str, enum.Enum):
    generation = "generation"
    consumption = "consumption"
    grid_feed = "grid_feed"
    base_fee = "base_fee"
    battery_charge = "battery_charge"
    production = "production"
    vpp_sale = "vpp_sale"
    battery_discharge = "battery_discharge"

class Participant(Base):
    __tablename__ = "participants"
    id = Column(Integer, primary_key=True)
    external_id = Column(String, unique=True, index=True, nullable=False)
    name = Column(String, default="")
    role = Column(
        Enum(ParticipantRole, native_enum=True, validate_strings=True),
        default=ParticipantRole.consumer,
        nullable=False
    )

class UsageEvent(Base):
    __tablename__ = "usage_events"
    id = Column(Integer, primary_key=True)
    participant_id = Column(Integer, ForeignKey("participants.id"), nullable=False)
    event_type = Column(Enum(EventType, native_enum=True, validate_strings=True), nullable=False)
    quantity = Column(Float, nullable=False, server_default=text("0.0"))
    unit = Column(String, nullable=False, server_default=text("'kWh'"))
    timestamp = Column(DateTime(timezone=True), nullable=False, server_default=text("NOW()"))
    meta = Column(JSON, nullable=False, server_default=text("'{}'::json"))

    participant = relationship("Participant")

class Policy(Base):
    __tablename__ = "policies"
    id = Column(Integer, primary_key=True)
    use_case = Column(String, nullable=False)
    body = Column(JSON, nullable=False, server_default=text("'{}'::json"))
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("NOW()"))

class SettlementBatch(Base):
    __tablename__ = "settlement_batches"
    id = Column(Integer, primary_key=True)
    use_case = Column(String, nullable=False, server_default=text("'mieterstrom'"))
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=text("NOW()"))
    start_time = Column(DateTime(timezone=True), nullable=False, server_default=text("NOW()"))
    end_time = Column(DateTime(timezone=True), nullable=False, server_default=text("NOW()"))

class SettlementLine(Base):
    __tablename__ = "settlement_lines"
    id = Column(Integer, primary_key=True)
    participant_id = Column(Integer, nullable=False)
    batch_id = Column(Integer, nullable=False)
    amount_eur = Column(Float, nullable=False, server_default=text("0.0"))
    description = Column(String, nullable=False, server_default=text("''"))
    proof_hash = Column(String, nullable=False, server_default=text("''"))

class LedgerEntry(Base):
    __tablename__ = "ledger_entries"
    id = Column(Integer, primary_key=True)
    sender_id = Column(Integer, ForeignKey("participants.id"))
    receiver_id = Column(Integer, ForeignKey("participants.id"))
    amount_eur = Column(Float, nullable=False, server_default=text("0.0"))
    batch_id = Column(Integer, ForeignKey("settlement_batches.id"))
    transaction_hash = Column(String, nullable=False, server_default=text("''"))
