from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, JSON, ForeignKey, Enum
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
import enum # Für Python Enum
from .db import Base # Importiere unsere Base-Klasse

# --- Enums für Rollen und Event-Typen ---

# Definieren der möglichen Rollen für einen Teilnehmer
class ParticipantRole(enum.Enum):
    PROSUMER = "prosumer" # Erzeugt und verbraucht Energie
    CONSUMER = "consumer" # Verbraucht Energie
    LANDLORD = "landlord" # Vermieter im Mieterstrom-Modell
    TENANT = "tenant"     # Mieter im Mieterstrom-Modell
    OPERATOR = "operator" # Betreiber / Dienstleister
    COMMUNITY_FEE_COLLECTOR = "community_fee_collector" # Spezielle Rolle für Community-Gebühren (oder ein Systemkonto)
    GRID_OPERATOR = "grid_operator" # Netzbetreiber (für Einspeisung/Bezug)

# Definieren der möglichen Event-Typen für UsageEvent
class EventType(enum.Enum):
    GENERATION = "generation"   # Energieerzeugung
    CONSUMPTION = "consumption" # Energieverbrauch
    BASE_FEE = "base_fee"       # Grundgebühr (z.B. monatlich)
    IMPORT_FROM_GRID = "import_from_grid" # Energiebezug aus dem Netz
    EXPORT_TO_GRID = "export_to_grid"     # Energieeinspeisung ins Netz

# --- Datenbank-Modelle (Tabellen) ---

class Participant(Base):
    """
    Repräsentiert einen Teilnehmer in der Clearinghouse-Plattform.
    Kann ein Prosumer, Consumer, Vermieter, Mieter etc. sein.
    """
    __tablename__ = "participants" # Name der Datenbanktabelle

    id = Column(Integer, primary_key=True, index=True) # Eindeutige ID, automatisch hochzählend
    name = Column(String, index=True) # Name des Teilnehmers
    role = Column(Enum(ParticipantRole), nullable=False) # Rolle des Teilnehmers (Prosumer, Consumer etc.)
    external_id = Column(String, unique=True, index=True, nullable=True) # Optionale ID aus einem externen System

    # Beziehungen zu anderen Tabellen (werden später gefüllt)
    usage_events = relationship("UsageEvent", back_populates="participant")
    ledger_entries = relationship("LedgerEntry", back_populates="participant")
    # ... weitere Beziehungen
    # Policy hat eine One-to-Many Beziehung zu Participant (ein Participant kann viele Policies haben oder eine Policy kann viele Participants haben?)
    # Für jetzt lassen wir es bei einfachen Beziehungen

class UsageEvent(Base):
    """
    Repräsentiert ein Ereignis des Energieverbrauchs oder der -erzeugung
    oder andere Gebühren, die einem Teilnehmer zugeordnet sind.
    """
    __tablename__ = "usage_events" # Name der Datenbanktabelle

    id = Column(Integer, primary_key=True, index=True)
    participant_id = Column(Integer, ForeignKey("participants.id")) # Verknüpfung zum Teilnehmer
    event_type = Column(Enum(EventType), nullable=False) # Typ des Ereignisses (Erzeugung, Verbrauch, Grundgebühr etc.)
    amount_kwh = Column(Float, nullable=True) # Menge an Energie in kWh (kann bei Gebühren null sein)
    amount_eur = Column(Float, nullable=True) # Betrag in Euro (kann bei Energieereignissen null sein, wenn nur kWh erfasst)
    timestamp = Column(DateTime(timezone=True), server_default=func.now()) # Zeitpunkt des Ereignisses
    description = Column(String, nullable=True) # Beschreibung des Ereignisses

    participant = relationship("Participant", back_populates="usage_events")

class Policy(Base):
    """
    Speichert die Konfiguration oder Regeln für ein bestimmtes Clearing-Batch.
    Die Richtlinie kann JSON-formatiert sein.
    """
    __tablename__ = "policies"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True) # Name der Richtlinie
    use_case = Column(String, nullable=False) # Der Anwendungsfall (z.B. "energy_community", "mieterstrom")
    definition = Column(JSON, nullable=False) # Die Policy-Definition als JSON-Objekt
    created_at = Column(DateTime(timezone=True), server_default=func.now()) # Erstellungszeitpunkt

    settlement_batches = relationship("SettlementBatch", back_populates="policy")


class SettlementBatch(Base):
    """
    Repräsentiert einen Satz von Ereignissen, die gemeinsam abgerechnet werden.
    Jedes Batch hat eine angewendete Policy und einen Status.
    """
    __tablename__ = "settlement_batches"

    id = Column(Integer, primary_key=True, index=True)
    policy_id = Column(Integer, ForeignKey("policies.id")) # Verknüpfung zur angewendeten Policy
    status = Column(String, default="pending") # Status des Batches (z.B. "pending", "processed", "completed")
    start_time = Column(DateTime(timezone=True)) # Startzeitraum des Batches
    end_time = Column(DateTime(timezone=True))   # Endzeitraum des Batches
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    policy = relationship("Policy", back_populates="settlement_batches")
    settlement_lines = relationship("SettlementLine", back_populates="settlement_batch")

class SettlementLine(Base):
    """
    Repräsentiert eine einzelne Abrechnungsposition innerhalb eines SettlementBatch.
    Kann eine Forderung oder eine Schuld eines Teilnehmers sein.
    """
    __tablename__ = "settlement_lines"

    id = Column(Integer, primary_key=True, index=True)
    settlement_batch_id = Column(Integer, ForeignKey("settlement_batches.id"))
    from_participant_id = Column(Integer, ForeignKey("participants.id"), nullable=True) # Wer zahlt (null, wenn System)
    to_participant_id = Column(Integer, ForeignKey("participants.id"), nullable=True) # Wer erhält (null, wenn System)
    amount = Column(Float, nullable=False) # Betrag der Transaktion
    currency = Column(String, default="EUR") # Währung
    description = Column(String, nullable=True) # Beschreibung der Position
    is_netted = Column(Boolean, default=False) # Zeigt an, ob dieser Posten bereits netto ist
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    settlement_batch = relationship("SettlementBatch", back_populates="settlement_lines")
    from_participant = relationship("Participant", foreign_keys=[from_participant_id])
    to_participant = relationship("Participant", foreign_keys=[to_participant_id])


class LedgerEntry(Base):
    """
    Repräsentiert einen Eintrag im Hauptbuch (doppelte Buchführung).
    """
    __tablename__ = "ledger_entries"

    id = Column(Integer, primary_key=True, index=True)
    settlement_line_id = Column(Integer, ForeignKey("settlement_lines.id"), nullable=True) # Optional: Verknüpfung zur SettlementLine
    participant_id = Column(Integer, ForeignKey("participants.id")) # Der betroffene Teilnehmer
    account_type = Column(String, nullable=False) # Art des Kontos (z.B. "Asset", "Liability", "Revenue", "Expense")
    amount = Column(Float, nullable=False) # Betrag des Eintrags
    is_debit = Column(Boolean, nullable=False) # True für Soll (Debit), False für Haben (Credit)
    timestamp = Column(DateTime(timezone=True), server_default=func.now())
    description = Column(String, nullable=True)

    participant = relationship("Participant", back_populates="ledger_entries")
    settlement_line = relationship("SettlementLine") # Beziehung zur SettlementLine

# Hinweis: BankTransfer wird später hinzugefügt, wenn wir uns mit Payouts beschäftigen.
# Für Tag 2 konzentrieren wir uns auf die oben genannten Kernmodelle.
