# config.py
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env if present (local dev only)
load_dotenv()

BASE_DIR = Path(__file__).resolve().parent


def _normalize_database_url(raw_url: str) -> str:
    """
    Normalize DATABASE_URL so SQLAlchemy uses psycopg v3.
    Render may provide:
      - postgres://...
      - postgresql://...

    We convert both to:
      - postgresql+psycopg://...
    """
    if not raw_url:
        return ""

    if raw_url.startswith("postgres://"):
        raw_url = raw_url.replace("postgres://", "postgresql+psycopg://", 1)
    elif raw_url.startswith("postgresql://"):
        raw_url = raw_url.replace("postgresql://", "postgresql+psycopg://", 1)

    return raw_url


class Config:
    # -----------------------------
    # Flask
    # -----------------------------
    SECRET_KEY = os.getenv("SECRET_KEY", "dev-change-me")

    # -----------------------------
    # Database
    # -----------------------------
    _RAW_DATABASE_URL = os.getenv("DATABASE_URL", "")
    SQLALCHEMY_DATABASE_URI = (
        _normalize_database_url(_RAW_DATABASE_URL)
        or f"sqlite:///{(BASE_DIR / 'instance' / 'invoices.db').as_posix()}"
    )

    SQLALCHEMY_ECHO = os.getenv("SQLALCHEMY_ECHO", "0") == "1"

    # -----------------------------
    # PDF export storage (Option A)
    # -----------------------------
    EXPORTS_DIR = os.getenv(
        "EXPORTS_DIR",
        (BASE_DIR / "exports").as_posix()
    )

    # -----------------------------
    # Invoice numbering
    # -----------------------------
    # Format: YYYY + N-digit sequence (no dash)
    INVOICE_SEQ_WIDTH = int(os.getenv("INVOICE_SEQ_WIDTH", "6"))

    # -----------------------------
    # App URL (for absolute links)
    # -----------------------------
    APP_BASE_URL = os.getenv(
        "APP_BASE_URL",
        "http://127.0.0.1:5000"
    )

    # -----------------------------
    # Stripe
    # -----------------------------
    STRIPE_SECRET_KEY = os.getenv("STRIPE_SECRET_KEY", "")
    STRIPE_PUBLISHABLE_KEY = os.getenv("STRIPE_PUBLISHABLE_KEY", "")
    STRIPE_PRICE_ID = os.getenv("STRIPE_PRICE_ID", "")
    STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")

