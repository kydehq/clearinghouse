# models.py
import enum
from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    Enum,
    Float,
    JSON,
    ForeignKey,
    text,
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
    external_id = Column(String, unique=True, index=True)
    name = Column(String)
    role = Column(Enum(ParticipantRole), default=ParticipantRole.consumer)


class UsageEvent(Base):
    __tablename__ = "usage_events"
    id = Column(Integer, primary_key=True)
    participant_id = Column(Integer, ForeignKey("participants.id"))
    event_type = Column(Enum(EventType))
    quantity = Column(Float)
    unit = Column(String)
    timestamp = Column(DateTime(timezone=True))
    meta = Column(JSON, default=text("'{}'::json"))
    participant = relationship("Participant")


class Policy(Base):
    __tablename__ = "policies"
    id = Column(Integer, primary_key=True)
    use_case = Column(String)
    body = Column(JSON, default=text("'{}'::json"))
    created_at = Column(DateTime(timezone=True), default=text("NOW()"))


class SettlementBatch(Base):
    __tablename__ = "settlement_batches"
    id = Column(Integer, primary_key=True)
    use_case = Column(String, default="mieterstrom")
    created_at = Column(DateTime(timezone=True), default=text("NOW()"))
    # NEU: Start- und Endzeitpunkte zur Klasse hinzufügen
    start_time = Column(DateTime(timezone=True))
    end_time = Column(DateTime(timezone=True))


class SettlementLine(Base):
    __tablename__ = "settlement_lines"
    id = Column(Integer, primary_key=True)
    participant_id = Column(Integer)
    batch_id = Column(Integer)
    amount_eur = Column(Float)
    description = Column(String)
    proof_hash = Column(String)


class LedgerEntry(Base):
    __tablename__ = "ledger_entries"
    id = Column(Integer, primary_key=True)
    sender_id = Column(Integer, ForeignKey("participants.id"))
    receiver_id = Column(Integer, ForeignKey("participants.id"))
    amount_eur = Column(Float)
    batch_id = Column(Integer, ForeignKey("settlement_batches.id"))
    transaction_hash = Column(String)
    sender = relationship("Participant", foreign_keys=[sender_id])
    receiver = relationship("Participant", foreign_keys=[receiver_id])