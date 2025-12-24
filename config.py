# config.py
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env if present
load_dotenv()

BASE_DIR = Path(__file__).resolve().parent

class Config:
    # Flask
    SECRET_KEY = os.getenv("SECRET_KEY", "dev-change-me")

    # Database
    # Use Postgres in production (recommended), SQLite is fine for local dev.
    # Examples:
    #   Postgres: postgresql+psycopg2://user:pass@host:5432/dbname
    #   SQLite:   sqlite:///instance/invoices.db
    SQLALCHEMY_DATABASE_URI = os.getenv(
        "DATABASE_URL",
        f"sqlite:///{(BASE_DIR / 'instance' / 'invoices.db').as_posix()}"
    )
    SQLALCHEMY_ECHO = os.getenv("SQLALCHEMY_ECHO", "0") == "1"

    # PDF export storage (Option A)
    EXPORTS_DIR = os.getenv("EXPORTS_DIR", (BASE_DIR / "exports").as_posix())

    # Invoice numbering
    # Format: YYYY + 6-digit sequence (no dash)
    INVOICE_SEQ_WIDTH = int(os.getenv("INVOICE_SEQ_WIDTH", "6"))

    # App URL (useful later for absolute links/emails)
    APP_BASE_URL = os.getenv("APP_BASE_URL", "http://127.0.0.1:5000")

