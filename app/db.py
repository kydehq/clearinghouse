from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
import os

# Die Datenbank-URL wird aus den Umgebungsvariablen gelesen.
# Railway setzt DATABASE_URL automatisch.
DATABASE_URL = os.getenv("DATABASE_URL")

# Überprüfen, ob die DATABASE_URL gesetzt ist. Wenn nicht, wird eine Fehlermeldung ausgegeben.
if not DATABASE_URL:
    raise ValueError("DATABASE_URL Umgebungsvariable ist nicht gesetzt.")

# SQLAlchemy Engine erstellen.
# Die Engine ist das Herzstück von SQLAlchemy und verwaltet die Datenbankverbindung.
# 'pool_pre_ping=True' hilft, verlorene Datenbankverbindungen zu erkennen und zu erneuern.
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

# Eine Basisklasse für unsere deklarativen Modelle erstellen.
# Alle unsere SQLAlchemy-Modelle werden von dieser Basisklasse erben.
Base = declarative_base()

# Eine Session-Fabrik erstellen.
# SessionLocal wird verwendet, um eine Datenbank-Sitzung zu erstellen.
# 'autocommit=False' bedeutet, dass Änderungen manuell commitet werden müssen.
# 'autoflush=False' bedeutet, dass die Session nicht automatisch in die DB schreibt.
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def create_db_and_tables():
    """
    Erstellt alle Datenbanktabellen, die von Base.metadata definiert sind.
    Diese Funktion sollte beim Start der Anwendung aufgerufen werden.
    """
    Base.metadata.create_all(engine)
    print("Datenbanktabellen wurden erfolgreich erstellt oder sind bereits vorhanden.")

# Optional: Eine Dependency für FastAPI, um eine Datenbank-Session bereitzustellen.
# Jede Anfrage erhält eine eigene Datenbank-Session.
def get_db():
    """
    Bietet eine unabhängige Datenbank-Session für jede Anfrage.
    Stellt sicher, dass die Session nach der Verwendung geschlossen wird.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

