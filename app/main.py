from fastapi import FastAPI, Request, Depends, HTTPException, status
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
import uvicorn

# Importiere unsere Datenbank-Tools und Modelle
from .db import create_db_and_tables, get_db, engine, Base
from . import models # Importiert models.py, um sicherzustellen, dass SQLAlchemy alle Modelle kennt

# Pfad zum Ordner 'templates' definieren.
BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

# FastAPI-Anwendung initialisieren
app = FastAPI(title="Clearinghouse POC")

# --- Event-Handler für den Start der Anwendung ---
@app.on_event("startup")
def on_startup():
    """
    Diese Funktion wird einmal beim Start der FastAPI-Anwendung ausgeführt.
    Hier erstellen wir unsere Datenbanktabellen.
    """
    create_db_and_tables()

# --- Routen der Anwendung (bleiben für Tag 2 gleich) ---

@app.get("/", response_class=HTMLResponse, summary="Startseite: Anwendungsfall-Auswahl")
async def read_root(request: Request):
    """
    Zeigt die Startseite an, auf der Benutzer zwischen verschiedenen Anwendungsfällen wählen können.
    """
    return templates.TemplateResponse(
        "case_selector.html",
        {"request": request, "title": "Anwendungsfall auswählen"}
    )

@app.get("/upload", response_class=HTMLResponse, summary="Upload-Seite (Platzhalter)")
async def upload_page(request: Request):
    """
    Platzhalter-Seite für den Upload von Daten.
    Wird in späteren Schritten implementiert.
    """
    return templates.TemplateResponse(
        "results.html",
        {"request": request, "title": "Daten hochladen", "message": "Dies ist die Upload-Seite. Sie wird noch implementiert."}
    )

@app.get("/results", response_class=HTMLResponse, summary="Ergebnisseite (Platzhalter)")
async def results_page(request: Request):
    """
    Platzhalter-Seite für die Anzeige der Berechnungsergebnisse.
    Wird in späteren Schritten implementiert.
    """
    return templates.TemplateResponse(
        "results.html",
        {"request": request, "title": "Ergebnisse", "message": "Dies ist die Ergebnisseite. Sie wird noch implementiert."}
    )

@app.get("/audit", response_class=HTMLResponse, summary="Audit-Seite (Platzhalter)")
async def audit_page(request: Request):
    """
    Platzhalter-Seite für die Anzeige von Audit-Informationen.
    Wird in späteren Schritten implementiert.
    """
    return templates.TemplateResponse(
        "results.html",
        {"request": request, "title": "Audit", "message": "Dies ist die Audit-Seite. Sie wird noch implementiert."}
    )

# Diese Zeile ist wichtig, damit Railway (oder ein lokaler Server) weiß, wie er die App startet.
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

