from fastapi import FastAPI, Request, Depends, HTTPException, status, UploadFile, File, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pathlib import Path
import uvicorn
import json # Benötigt, um JSON-Policies zu parsen
import io # Benötigt, um hochgeladene CSV-Dateien zu lesen
import pandas as pd # Hilft beim Lesen von CSV-Dateien (muss noch in requirements.txt)

# Importiere unsere Datenbank-Tools und Modelle
from .db import create_db_and_tables, get_db, SessionLocal
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

# --- Routen der Anwendung ---

@app.get("/", response_class=HTMLResponse, summary="Startseite: Anwendungsfall-Auswahl")
async def read_root(request: Request):
    """
    Zeigt die Startseite an, auf der Benutzer zwischen verschiedenen Anwendungsfällen wählen können.
    """
    return templates.TemplateResponse(
        "case_selector.html",
        {"request": request, "title": "Anwendungsfall auswählen"}
    )

@app.get("/upload", response_class=HTMLResponse, summary="Upload-Seite für Daten und Policy-Eingabe")
async def get_upload_page(request: Request, case: str = "energy_community"):
    """
    Zeigt die Upload-Seite an, auf der Benutzer eine CSV-Datei und eine JSON-Policy hochladen können.
    Der 'case'-Parameter wird aus der URL gelesen, um den gewählten Anwendungsfall anzuzeigen.
    """
    default_policy = {}
    if case == "energy_community":
        # Eine Standard-Policy für Energy Community
        default_policy = {
            "use_case": "energy_community",
            "prosumer_sell_price": 0.15,
            "consumer_buy_price": 0.12,
            "community_fee_rate": 0.02,
            "grid_feed_price": 0.08
        }
    elif case == "mieterstrom":
        # Eine Standard-Policy für Mieterstrom
        default_policy = {
            "use_case": "mieterstrom",
            "tenant_price_per_kwh": 0.18,
            "landlord_revenue_share": 0.60,
            "operator_fee_rate": 0.15,
            "grid_compensation": 0.08,
            "base_fee_per_unit": 5.00
        }

    return templates.TemplateResponse(
        "upload.html",
        {
            "request": request,
            "title": f"Daten & Policy für {case.replace('_', ' ').title()} hochladen",
            "case": case,
            "default_policy_json": json.dumps(default_policy, indent=2) # Policy als schön formatierten JSON-String
        }
    )

@app.post("/process_data", response_class=HTMLResponse, summary="Daten verarbeiten und Settlement starten")
async def process_data(
    request: Request,
    case: str = Form(...), # Der Anwendungsfall vom Formular
    csv_file: UploadFile = File(...), # Die hochgeladene CSV-Datei
    policy_json_str: str = Form(...) # Die JSON-Policy als String vom Formular
):
    """
    Empfängt die hochgeladene CSV-Datei und die JSON-Policy, verarbeitet sie
    und leitet den Settlement-Prozess ein.
    """
    try:
        # 1. Policy parsen
        policy_data = json.loads(policy_json_str)
        if policy_data.get("use_case") != case:
            return templates.TemplateResponse(
                "results.html",
                {"request": request, "title": "Fehler", "message": "Anwendungsfall in Policy stimmt nicht mit Auswahl überein."},
                status_code=status.HTTP_400_BAD_REQUEST
            )

        # 2. CSV-Datei lesen (hier nur als Beispiel, wird später verarbeitet)
        contents = await csv_file.read()
        csv_data = pd.read_csv(io.StringIO(contents.decode('utf-8')))

        # Debug-Ausgabe in den Logs (später entfernen)
        print(f"Verarbeite Case: {case}")
        print(f"Policy Daten: {policy_data}")
        print(f"CSV-Daten (erste 5 Zeilen):\n{csv_data.head()}")

        # Hier würden wir später die Daten in die DB speichern und den Settlement-Prozess starten
        # Für jetzt leiten wir einfach auf eine Erfolgsseite um.

        # Beispiel: Eine Policy in der DB speichern (später mit Fehlerbehandlung)
        db_session = SessionLocal()
        try:
            new_policy = models.Policy(
                name=f"Policy for {case} - {pd.Timestamp.now().strftime('%Y%m%d%H%M%S')}",
                use_case=case,
                definition=policy_data
            )
            db_session.add(new_policy)
            db_session.commit()
            db_session.refresh(new_policy)
            print(f"Policy erfolgreich in DB gespeichert: {new_policy.id}")
        except Exception as e:
            db_session.rollback()
            print(f"Fehler beim Speichern der Policy: {e}")
            return templates.TemplateResponse(
                "results.html",
                {"request": request, "title": "Fehler", "message": f"Fehler beim Speichern der Policy: {e}"},
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
        finally:
            db_session.close()


        # Erfolgreiche Verarbeitung -> Weiterleiten zur Ergebnisseite
        return templates.TemplateResponse(
            "results.html",
            {"request": request, "title": "Verarbeitung erfolgreich!", "message": "Daten wurden empfangen und die Policy gespeichert. Das Settlement wird demnächst gestartet."}
        )

    except json.JSONDecodeError:
        return templates.TemplateResponse(
            "results.html",
            {"request": request, "title": "Fehler", "message": "Ungültiges JSON-Format für die Policy."},
            status_code=status.HTTP_400_BAD_REQUEST
        )
    except Exception as e:
        return templates.TemplateResponse(
            "results.html",
            {"request": request, "title": "Fehler", "message": f"Ein unerwarteter Fehler ist aufgetreten: {e}"},
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        )

# Die anderen Platzhalter-Seiten bleiben vorerst wie gehabt
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

