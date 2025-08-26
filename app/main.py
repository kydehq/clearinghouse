from fastapi import FastAPI, Request, Depends, HTTPException, status, UploadFile, File, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates, RequestValidationError # RequestValidationError hinzugefügt (falls benötigt)
from fastapi.staticfiles import StaticFiles
from pathlib import Path
import uvicorn
import json
import io
import pandas as pd
import traceback # Neu für detailliertere Fehlermeldungen in Logs

# Importiere unsere Datenbank-Tools und Modelle
from .db import create_db_and_tables, get_db, SessionLocal
from . import models

# Pfad zum Ordner 'templates' definieren.
BASE_DIR = Path(__file__).resolve().parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

# FastAPI-Anwendung initialisieren
app = FastAPI(title="Clearinghouse POC")

# --- Statische Dateien einbinden ---
# Der Ordner 'demo_data' liegt im Hauptverzeichnis des Projekts.
# Wir müssen den Pfad so einrichten, dass FastAPI ihn findet.
# 'app/static' wird als '/static' URL bereitgestellt.
# 'demo_data' (im Wurzelverzeichnis) wird auch als '/static/demo_data' bereitgestellt.
# Da demo_data auf der gleichen Ebene wie app liegt, müssen wir den Pfad anders behandeln.
# Korrekter Weg, wenn `demo_data` auf der gleichen Ebene wie `app` ist:
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
# Für demo_data, wenn es im Projektwurzelverzeichnis liegt:
PROJECT_ROOT = BASE_DIR.parent # Gehe vom 'app'-Ordner zum Projektwurzel
app.mount("/static/demo_data", StaticFiles(directory=str(PROJECT_ROOT / "demo_data")), name="demo_data_static")


# --- Event-Handler für den Start der Anwendung ---
@app.on_event("startup")
def on_startup():
    """
    Diese Funktion wird einmal beim Start der FastAPI-Anwendung ausgeführt.
    Hier erstellen wir unsere Datenbanktabellen.
    """
    create_db_and_tables()

# --- Exception Handler für allgemeine Fehler ---
# Dieser Handler fängt unaufgeforderte Ausnahmen ab und loggt sie detailliert.
@app.exception_handler(Exception)
async def general_exception_handler(request: Request, exc: Exception):
    error_message = f"Ein unerwarteter Server-Fehler ist aufgetreten: {exc}\n{traceback.format_exc()}"
    print(error_message) # Wichtig: Den vollständigen Traceback im Log ausgeben
    return templates.TemplateResponse(
        "results.html",
        {"request": request, "title": "Interner Server-Fehler", "message": "Es gab ein Problem beim Verarbeiten Ihrer Anfrage. Bitte versuchen Sie es später erneut oder kontaktieren Sie den Support. Details finden Sie in den Server-Logs."},
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
    )


@app.get("/", response_class=HTMLResponse, summary="Startseite: Anwendungsfall-Auswahl")
async def read_root(request: Request):
    return templates.TemplateResponse(
        "case_selector.html",
        {"request": request, "title": "Anwendungsfall auswählen"}
    )

@app.get("/upload", response_class=HTMLResponse, summary="Upload-Seite für Daten und Policy-Eingabe")
async def get_upload_page(request: Request, case: str = "energy_community"):
    default_policy = {}
    if case == "energy_community":
        default_policy = {
            "use_case": "energy_community",
            "prosumer_sell_price": 0.15,
            "consumer_buy_price": 0.12,
            "community_fee_rate": 0.02,
            "grid_feed_price": 0.08
        }
    elif case == "mieterstrom":
        default_policy = {
            "use_case": "mieterstrom",
            "tenant_price_per_kwh": 0.18,
            "landlord_revenue_share": 0.60,
            "operator_fee_rate": 0.15,
            "grid_compensation": 0.08,
            "base_fee_per_unit": 5.00
        }

    try: # Zusätzlicher Try-Except Block zur Fehlerisolierung beim Template-Rendering
        return templates.TemplateResponse(
            "upload.html",
            {
                "request": request,
                "title": f"Daten & Policy für {case.replace('_', ' ').title()} hochladen",
                "case": case,
                "default_policy_json": json.dumps(default_policy, indent=2)
            }
        )
    except Exception as e:
        # Fängt spezifische Fehler beim Rendern des Templates ab und loggt sie
        error_message = f"Fehler beim Rendern von upload.html für Fall '{case}': {e}\n{traceback.format_exc()}"
        print(error_message)
        # Wirft eine HTTPException, die vom allgemeinen Exception-Handler abgefangen wird
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Fehler beim Laden der Upload-Seite: {e}"
        )


@app.post("/process_data", response_class=HTMLResponse, summary="Daten verarbeiten und Settlement starten")
async def process_data(
    request: Request,
    case: str = Form(...),
    csv_file: UploadFile = File(...),
    policy_json_str: str = Form(...)
):
    try:
        policy_data = json.loads(policy_json_str)
        if policy_data.get("use_case") != case:
            return templates.TemplateResponse(
                "results.html",
                {"request": request, "title": "Fehler", "message": "Anwendungsfall in Policy stimmt nicht mit Auswahl überein."},
                status_code=status.HTTP_400_BAD_REQUEST
            )

        contents = await csv_file.read()
        csv_data = pd.read_csv(io.StringIO(contents.decode('utf-8')))

        print(f"Verarbeite Case: {case}")
        print(f"Policy Daten: {policy_data}")
        print(f"CSV-Daten (erste 5 Zeilen):\n{csv_data.head()}")

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
        error_message = f"Ein unerwarteter Fehler in process_data ist aufgetreten: {e}\n{traceback.format_exc()}"
        print(error_message) # Auch hier detaillierte Logs
        return templates.TemplateResponse(
            "results.html",
            {"request": request, "title": "Fehler", "message": f"Ein unerwarteter Fehler ist aufgetreten: {e}"},
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR
        )


@app.get("/results", response_class=HTMLResponse, summary="Ergebnisseite (Platzhalter)")
async def results_page(request: Request):
    return templates.TemplateResponse(
        "results.html",
        {"request": request, "title": "Ergebnisse", "message": "Dies ist die Ergebnisseite. Sie wird noch implementiert."}
    )

@app.get("/audit", response_class=HTMLResponse, summary="Audit-Seite (Platzhalter)")
async def audit_page(request: Request):
    return templates.TemplateResponse(
        "results.html",
        {"request": request, "title": "Audit", "message": "Dies ist die Audit-Seite. Sie wird noch implementiert."}
    )

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

