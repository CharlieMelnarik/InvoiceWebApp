# =========================
# app.py
# =========================
import os
import re
import io
import zipfile
import smtplib
import base64
import json
import urllib.parse
import urllib.request
from email.message import EmailMessage
from datetime import datetime, timedelta
from pathlib import Path
from functools import wraps
from werkzeug.utils import secure_filename

import stripe
from flask import (
    Flask, render_template, request, redirect, url_for,
    flash, send_file, abort, current_app, jsonify
)
from flask_login import (
    LoginManager, UserMixin, login_user, logout_user,
    login_required, current_user
)
from sqlalchemy import text, or_
from sqlalchemy.orm import selectinload
from sqlalchemy.exc import IntegrityError
from werkzeug.security import generate_password_hash, check_password_hash
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired
from PIL import Image, UnidentifiedImageError, ImageFile
import pytesseract

from config import Config
from models import (
    Base, make_engine, make_session_factory,
    User, Customer, Invoice, InvoicePart, InvoiceLabor, next_invoice_number, next_display_number,
    ScheduleEvent, AuditLog,
)
from pdf_service import generate_and_store_pdf

login_manager = LoginManager()
login_manager.login_view = "login"
_IMG_RESAMPLE = getattr(Image, "Resampling", Image).LANCZOS


# -----------------------------
# Flask-Login user wrapper
# -----------------------------
class AppUser(UserMixin):
    def __init__(self, user_id: int, username: str):
        self.id = str(user_id)
        self.username = username


@login_manager.user_loader
def load_user(user_id: str):
    # NOTE: we load from DB inside create_app via SessionLocal closure
    # Flask-Login calls this after app is initialized, so we bind a function later.
    return None


# -----------------------------
# Helpers
# -----------------------------
def _to_float(s, default=0.0):
    try:
        s = (s or "").strip()
        return float(s) if s else float(default)
    except Exception:
        return float(default)


def _parse_repeating_fields(names, prices):
    out = []
    n = max(len(names), len(prices))
    for i in range(n):
        name = (names[i] if i < len(names) else "").strip()
        price = (prices[i] if i < len(prices) else "").strip()
        if not name and not price:
            continue
        out.append((name, _to_float(price, 0.0)))
    return out


def _ensure_dirs():
    Path("instance").mkdir(parents=True, exist_ok=True)
    Path(Config.EXPORTS_DIR).mkdir(parents=True, exist_ok=True)


def _logo_upload_dir() -> Path:
    d = Path("instance") / "uploads" / "logos"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _process_logo_upload_to_png_bytes(file_storage) -> bytes:
    """
    Resize/compress uploaded logo for DB storage.
    - Keeps dimensions reasonable for PDF header use.
    - Stores as optimized PNG bytes.
    """
    raw = file_storage.read()
    file_storage.stream.seek(0)
    if not raw:
        raise ValueError("Empty logo file.")
    if len(raw) > 8 * 1024 * 1024:
        raise ValueError("Logo file is too large. Please use an image under 8MB.")

    try:
        img = Image.open(io.BytesIO(raw))
        img.load()
    except UnidentifiedImageError:
        raise ValueError("Logo must be a valid .png, .jpg, or .jpeg image.")

    # Fit into a compact header-friendly box.
    max_w, max_h = 700, 260
    img.thumbnail((max_w, max_h), _IMG_RESAMPLE)

    # Normalize to RGBA so transparency works consistently.
    if img.mode not in ("RGB", "RGBA"):
        img = img.convert("RGBA")

    out = io.BytesIO()
    img.save(out, format="PNG", optimize=True, compress_level=9)
    return out.getvalue()


def _parse_receipt_text_to_items(text: str) -> list[dict]:
    """
    Parse OCR receipt text into line items.
    Heuristic: lines with a price near the end (e.g. $12.34 or 12.34).
    """
    items = []
    if not text:
        return items

    prev_item_name = ""
    last_item_index = -1
    in_items_section = False
    items_section_hits = 0
    noisy_chars_re = re.compile(r"[^\w\s\-\./#@]")

    for raw_line in text.splitlines():
        line = (raw_line or "").strip()
        if not line:
            continue

        # Drop obviously noisy/long lines unless they contain a price we can parse
        if len(line) > 80 and not re.search(r"\d{1,5}[.,]\d{2}\s*[A-Z]?\s*$", line):
            continue

        # Skip common non-item descriptor lines (often after item line on auto parts receipts)
        if any(k in line.lower() for k in ("manufacturer", "warranty", "returned")):
            continue

        # Ignore common totals/taxes
        lower = line.lower()
        if any(k in lower for k in ("subtotal", "tax", "total", "change", "balance", "visa", "mastercard", "amex", "approval", "ref #", "trans", "tend", "debit", "aid ", "terminal", "signature", "customer copy", "items sold", "st#", "op#", "te#", "tr#", "store", "manager", "philadelphia", "address", "invoice #", "drawer", "counter #", "reprint", "store hours", "receipt", "reg #", "csr", "rewards", "member", "auth", "chip", "payment", "autozone", "how did we do", "program terms", "www.")):
            continue
        if "wages" in lower:
            continue
        if re.match(r"^date[:\s]", lower):
            continue

        # Price patterns (prefer rightmost, allow optional trailing flags)
        m = re.search(r"(-?\$?\d{1,5}(?:[.,]\d{2})?)\s*[A-Z]?\s*$", line)
        if m:
            in_items_section = True
            items_section_hits += 1
            price_raw = m.group(1).replace(",", ".").replace("$", "").strip()
            try:
                price_val = float(price_raw)
            except Exception:
                continue
            if price_val == 0 or abs(price_val) > 5000:
                continue

            name_part = line[: m.start(1)].strip(" -\t")
            if not name_part:
                continue
            # Require some letters in the name to avoid phone/IDs
            if not re.search(r"[A-Za-z]", name_part):
                continue
            # Drop lines that are mostly digits/ids
            if re.fullmatch(r"[\d\s\-/().#]+", name_part):
                continue
            # Strip common UPC/ID blocks at end of line
            name_part = re.sub(r"\s+\d{6,}[A-Z0-9]*$", "", name_part).strip()
            # Strip leading IDs / account-like tokens
            name_part = re.sub(r"^[A-Z0-9]{6,}\s+", "", name_part).strip()
            # Remove trailing quantity markers like "X" or "F"
            name_part = re.sub(r"\b[XF]\b$", "", name_part).strip()
            if len(name_part) < 3:
                continue
            if len(name_part) > 60:
                continue
            if re.search(r"\b\d{1,2}/\d{1,2}/\d{2,4}\b", name_part):
                continue
            # Ignore lines that are clearly headers even if they have a price
            if re.search(r"\b(invoice|date|drawer|counter)\b", name_part.lower()):
                continue
            # Filter noisy lines (too many symbols vs letters)
            if noisy_chars_re.search(name_part):
                letters = len(re.findall(r"[A-Za-z]", name_part))
                if letters < 3:
                    continue
                non_alnum = len(re.findall(r"[^\w\s\-./#@]", name_part))
                if non_alnum / max(1, len(name_part)) > 0.15:
                    continue

            items.append({
                "name": name_part,
                "price": f"{price_val:.2f}",
                "raw": line,
            })
            prev_item_name = ""
            last_item_index = len(items) - 1
            continue

        # If the previous line was an item with price, append description-only lines to it.
        if last_item_index >= 0:
            if re.search(r"[A-Za-z]", line):
                if not re.search(r"\b(subtotal|tax|total|change|balance|invoice|date|drawer|counter|reprint|receipt|reg|csr|rewards|member|auth|chip|payment)\b", line.lower()):
                    if len(line) <= 40:
                        # Skip noisy descriptor lines
                        non_alnum = len(re.findall(r"[^\w\s\-./#@]", line))
                        if non_alnum / max(1, len(line)) <= 0.15:
                            items[last_item_index]["name"] = f"{items[last_item_index]['name']} {line}".strip()
                        continue

        # Handle receipt lines split across two lines (e.g. name line followed by price line)
        if prev_item_name:
            m_price_only = re.search(r"^\$?\d{1,4}(?:[.,]\d{2})\s*[A-Z]?\s*$", line)
            if m_price_only:
                price_raw = m_price_only.group(0).replace(",", ".").replace("$", "").strip()
                try:
                    price_val = float(price_raw)
                except Exception:
                    continue
                if price_val <= 0 or price_val > 5000:
                    continue
                items.append({
                    "name": prev_item_name,
                    "price": f"{price_val:.2f}",
                    "raw": f"{prev_item_name} {price_val:.2f}",
                })
                prev_item_name = ""
                last_item_index = len(items) - 1
                continue

        # Capture multi-line item descriptions: keep building name until price line appears.
        if re.search(r"[A-Za-z]", line) and len(line) >= 3:
            if not in_items_section and items_section_hits < 1:
                continue
            if prev_item_name:
                prev_item_name = f"{prev_item_name} {line}".strip()
            else:
                prev_item_name = line

    return items


def _current_user_id_int() -> int:
    try:
        return int(current_user.get_id())
    except Exception:
        return -1


def _invoice_owned_or_404(session, invoice_id: int) -> Invoice:
    inv = (
        session.query(Invoice)
        .options(selectinload(Invoice.parts), selectinload(Invoice.labor_items))
        .filter(Invoice.id == invoice_id, Invoice.user_id == _current_user_id_int())
        .filter(or_(Invoice.is_estimate.is_(False), Invoice.is_estimate.is_(None)))
        .first()
    )
    if not inv:
        abort(404)
    return inv


def _estimate_owned_or_404(session, estimate_id: int) -> Invoice:
    inv = (
        session.query(Invoice)
        .options(selectinload(Invoice.parts), selectinload(Invoice.labor_items))
        .filter(Invoice.id == estimate_id, Invoice.user_id == _current_user_id_int())
        .filter(Invoice.is_estimate.is_(True))
        .first()
    )
    if not inv:
        abort(404)
    return inv


def _customer_owned_or_404(session, customer_id: int) -> Customer:
    c = (
        session.query(Customer)
        .filter(Customer.id == customer_id, Customer.user_id == _current_user_id_int())
        .first()
    )
    if not c:
        abort(404)
    return c


def _normalize_email(email: str) -> str:
    return (email or "").strip().lower()


def _looks_like_email(email: str) -> bool:
    e = _normalize_email(email)
    return bool(e) and ("@" in e) and ("." in e.split("@")[-1])


def _format_phone_display(phone: str | None) -> str:
    raw = (phone or "").strip()
    if not raw:
        return ""
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+1 ({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
    return re.sub(r"\)\s+", ") ", raw)


def _parse_summary_time(value: str) -> str | None:
    raw = (value or "").strip()
    if not raw:
        return None
    if not re.match(r"^\d{2}:\d{2}$", raw):
        return None
    hh, mm = raw.split(":")
    try:
        h = int(hh)
        m = int(mm)
    except ValueError:
        return None
    if not (0 <= h <= 23 and 0 <= m <= 59):
        return None
    return f"{h:02d}:{m:02d}"


def _parse_iso_dt(s: str) -> datetime:
    """
    Accepts:
      - 'YYYY-MM-DDTHH:MM'
      - 'YYYY-MM-DD HH:MM'
      - with optional seconds
    """
    s = (s or "").strip()
    if not s:
        raise ValueError("Missing datetime")
    s = s.replace(" ", "T")
    return datetime.fromisoformat(s)


def _parse_dt_local(s: str) -> datetime | None:
    """
    For <input type="datetime-local"> values (YYYY-MM-DDTHH:MM).
    Returns naive datetime or None.
    """
    s = (s or "").strip()
    if not s:
        return None
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _clamp_tz_offset_minutes(value: int) -> int:
    return max(-720, min(840, int(value)))


def _request_tz_offset_minutes(user: User | None = None) -> int:
    """
    Resolve user's timezone offset in minutes from request first, then user profile.
    Positive values are east of UTC (same sign as JS: -getTimezoneOffset()).
    """
    fallback = _clamp_tz_offset_minutes(int(getattr(user, "schedule_summary_tz_offset_minutes", 0) or 0))
    raw = (
        (request.form.get("client_tz_offset_minutes") if request.form is not None else None)
        or request.args.get("client_tz_offset_minutes")
        or ""
    ).strip()
    if not raw:
        return fallback
    try:
        return _clamp_tz_offset_minutes(int(raw))
    except Exception:
        return fallback


def _user_local_now(user: User | None = None) -> datetime:
    return datetime.utcnow() + timedelta(minutes=_request_tz_offset_minutes(user))


def _format_customer_address(
    line1: str | None,
    line2: str | None,
    city: str | None,
    state: str | None,
    postal_code: str | None,
) -> str | None:
    parts = [p for p in [(line1 or "").strip(), (line2 or "").strip()] if p]
    city_state = " ".join([p for p in [(city or "").strip(), (state or "").strip()] if p])
    if city_state:
        parts.append(city_state)
    if (postal_code or "").strip():
        if parts:
            parts[-1] = f"{parts[-1]} {postal_code.strip()}"
        else:
            parts.append(postal_code.strip())
    return ", ".join(parts) if parts else None


def _summary_period_key(freq: str, window_start: datetime) -> str:
    if freq == "day":
        return window_start.date().isoformat()
    if freq == "week":
        iso = window_start.isocalendar()
        return f"{iso.year}-W{iso.week:02d}"
    if freq == "month":
        return f"{window_start.year}-{window_start.month:02d}"
    return ""


def _summary_window(now_local: datetime, freq: str, start_time: str) -> tuple[datetime, datetime]:
    hh, mm = [int(part) for part in start_time.split(":")]
    window_start = datetime(now_local.year, now_local.month, now_local.day, hh, mm)
    if freq == "day":
        return window_start, window_start + timedelta(days=1)
    if freq == "week":
        return window_start, window_start + timedelta(days=7)
    if freq == "month":
        return window_start, window_start + timedelta(days=30)
    return window_start, window_start + timedelta(days=1)


def _format_offset_label(offset_minutes: int) -> str:
    sign = "+" if offset_minutes >= 0 else "-"
    abs_val = abs(offset_minutes)
    hh = abs_val // 60
    mm = abs_val % 60
    return f"UTC{sign}{hh:02d}:{mm:02d}"


def _summary_window_for_user(user: User, now_utc: datetime) -> tuple[datetime, datetime, str, datetime]:
    offset_minutes = int(getattr(user, "schedule_summary_tz_offset_minutes", 0) or 0)
    now_local = now_utc + timedelta(minutes=offset_minutes)
    start_time = getattr(user, "schedule_summary_time", None) or "00:00"
    start, end = _summary_window(now_local, user.schedule_summary_frequency or "day", start_time)
    return start, end, _format_offset_label(offset_minutes), now_local


def _format_event_line(event: ScheduleEvent, customer: Customer | None) -> str:
    title = (event.title or "").strip() or (customer.name if customer else "Appointment")
    if customer and customer.name and title.lower() != customer.name.lower():
        label = f"{title} - {customer.name}"
    else:
        label = title

    start_label = event.start_dt.strftime("%b %d, %Y %I:%M %p").lstrip("0")
    end_label = event.end_dt.strftime("%b %d, %Y %I:%M %p").lstrip("0")
    return f"- {start_label} → {end_label}: {label}"


def _should_send_summary(user: User, now_utc: datetime) -> bool:
    freq = (getattr(user, "schedule_summary_frequency", None) or "none").lower().strip()
    if freq == "none":
        return False

    time_value = _parse_summary_time(getattr(user, "schedule_summary_time", None) or "")
    if not time_value:
        return False

    offset_minutes = int(getattr(user, "schedule_summary_tz_offset_minutes", 0) or 0)
    now_local = now_utc + timedelta(minutes=offset_minutes)
    hh, mm = [int(part) for part in time_value.split(":")]
    window_start = datetime(now_local.year, now_local.month, now_local.day, hh, mm)
    if now_local < window_start:
        return False

    last_sent = getattr(user, "schedule_summary_last_sent", None)
    if not last_sent:
        return True

    last_sent_local = last_sent + timedelta(minutes=offset_minutes)
    return _summary_period_key(freq, last_sent_local) != _summary_period_key(freq, window_start)


# -----------------------------
# Invoice template / profession config
# -----------------------------
INVOICE_TEMPLATES = {
    "auto_repair": {
        "label": "Auto Repair",
        "job_label": "Vehicle",
        "labor_title": "Labor",
        "labor_desc_label": "Labor Description",
        "parts_title": "Parts",
        "parts_name_label": "Part Name",
        "shop_supplies_label": "Shop Supplies",
    },
    "general_service": {
        "label": "General Service",
        "job_label": "Job / Project",
        "labor_title": "Services",
        "labor_desc_label": "Service Description",
        "parts_title": "Materials",
        "parts_name_label": "Material",
        "shop_supplies_label": "Supplies / Fees",
    },
    "accountant": {
        "label": "Accountant",
        "job_label": "Engagement",
        "labor_title": "Services",
        "labor_desc_label": "Service Description",
        "parts_title": "Expenses",
        "parts_name_label": "Expense",
        "shop_supplies_label": "Admin / Filing Fees",
    },
    "computer_repair": {
        "label": "Computer Repair",
        "job_label": "Device",
        "labor_title": "Services",
        "labor_desc_label": "Service Description",
        "parts_title": "Parts",
        "parts_name_label": "Part Name",
        "shop_supplies_label": "Shop Supplies",
    },
    "lawn_care": {
        "label": "Lawn Care / Landscaping",
        "job_label": "Service Address",
        "labor_title": "Services",
        "labor_desc_label": "Service Description",
        "parts_title": "Materials",
        "parts_name_label": "Material",
        "shop_supplies_label": "Disposal / Trip Fees",
    },
    "flipping_items": {
        "label": "Flipping Items",
        "job_label": "Item",
        "labor_title": "Sales",
        "labor_desc_label": "Sale Description",
        "parts_title": "Costs",
        "parts_name_label": "Cost Item",
        "shop_supplies_label": "Other Expenses",
    },
}


def _template_key_fallback(key: str | None) -> str:
    key = (key or "").strip()
    return key if key in INVOICE_TEMPLATES else "auto_repair"


def _template_config_for(key: str | None) -> dict:
    return INVOICE_TEMPLATES[_template_key_fallback(key)]


# -----------------------------
# PDF layout templates
# -----------------------------
PDF_TEMPLATES = {
    "classic": {
        "label": "Classic",
        "desc": "Clean, low-ink layout with traditional sections.",
        "preview": "images/pdf_template_classic.svg",
    },
    "modern": {
        "label": "Modern",
        "desc": "Bold header band, refined table styling, and a polished summary.",
        "preview": "images/pdf_template_modern.svg",
    },
    "split_panel": {
        "label": "Split Panel",
        "desc": "Left summary rail with a wide, airy content layout.",
        "preview": "images/pdf_template_split.svg",
    },
    "strip": {
        "label": "Invoice Strip",
        "desc": "Header-led layout with a bold total strip and clean rows.",
        "preview": "images/pdf_template_strip.svg",
    },
}


def _pdf_template_key_fallback(key: str | None) -> str:
    key = (key or "").strip()
    return key if key in PDF_TEMPLATES else "classic"


# -----------------------------
# Password reset token helpers
# -----------------------------
def _reset_serializer():
    secret = current_app.config.get("SECRET_KEY")
    salt = current_app.config.get("PASSWORD_RESET_SALT", "password-reset")
    return URLSafeTimedSerializer(secret, salt=salt)


def make_password_reset_token(user_id: int) -> str:
    return _reset_serializer().dumps({"uid": int(user_id)})


def read_password_reset_token(token: str, max_age_seconds: int) -> int | None:
    try:
        data = _reset_serializer().loads(token, max_age=max_age_seconds)
        uid = data.get("uid")
        return int(uid)
    except (SignatureExpired, BadSignature, TypeError, ValueError):
        return None


def _share_serializer():
    secret = current_app.config.get("SECRET_KEY")
    salt = current_app.config.get("PDF_SHARE_SALT", "pdf-share")
    return URLSafeTimedSerializer(secret, salt=salt)


def make_pdf_share_token(user_id: int, invoice_id: int) -> str:
    return _share_serializer().dumps({"uid": int(user_id), "iid": int(invoice_id)})


def read_pdf_share_token(token: str, max_age_seconds: int) -> tuple[int, int] | None:
    try:
        data = _share_serializer().loads(token, max_age=max_age_seconds)
        uid = int(data.get("uid"))
        iid = int(data.get("iid"))
        return uid, iid
    except (SignatureExpired, BadSignature, TypeError, ValueError):
        return None


def _normalize_phone(phone: str | None) -> str:
    return (phone or "").strip()


def _to_e164_phone(phone: str | None) -> str | None:
    raw = _normalize_phone(phone)
    if not raw:
        return None
    if raw.startswith("+"):
        digits = "+" + re.sub(r"\D", "", raw)
        if len(digits) >= 8:
            return digits
        return None
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 10:
        return f"+1{digits}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+{digits}"
    return None


def _public_base_url() -> str:
    return (
        current_app.config.get("APP_BASE_URL")
        or os.getenv("APP_BASE_URL")
        or current_app.config.get("PUBLIC_APP_URL")
        or os.getenv("PUBLIC_APP_URL")
        or ""
    ).strip().rstrip("/")


def _public_url(path: str) -> str:
    base = _public_base_url()
    if base:
        return f"{base}{path}"
    return url_for("landing", _external=True).rstrip("/") + path


def _client_ip() -> str:
    forwarded = (request.headers.get("X-Forwarded-For") or "").strip()
    if forwarded:
        return forwarded.split(",")[0].strip()[:64]
    cf_ip = (request.headers.get("CF-Connecting-IP") or "").strip()
    if cf_ip:
        return cf_ip[:64]
    return (request.remote_addr or "")[:64]


def _audit_log(
    session,
    *,
    event: str,
    result: str,
    user_id: int | None = None,
    username: str | None = None,
    email: str | None = None,
    details: str | None = None,
) -> None:
    try:
        row = AuditLog(
            user_id=user_id,
            event=(event or "")[:80],
            result=(result or "")[:20],
            method=(request.method or "")[:10],
            path=(request.path or "")[:255],
            ip_address=_client_ip(),
            user_agent=(request.headers.get("User-Agent") or "")[:300],
            username=((username or "").strip() or None),
            email=((email or "").strip().lower() or None),
            details=((details or "").strip()[:1000] or None),
        )
        session.add(row)
    except Exception:
        pass


def _turnstile_enabled() -> bool:
    site = (current_app.config.get("TURNSTILE_SITE_KEY") or os.getenv("TURNSTILE_SITE_KEY") or "").strip()
    secret = (current_app.config.get("TURNSTILE_SECRET_KEY") or os.getenv("TURNSTILE_SECRET_KEY") or "").strip()
    return bool(site and secret)


def _turnstile_site_key() -> str:
    return (current_app.config.get("TURNSTILE_SITE_KEY") or os.getenv("TURNSTILE_SITE_KEY") or "").strip()


def _verify_turnstile(token: str) -> tuple[bool, str]:
    secret = (current_app.config.get("TURNSTILE_SECRET_KEY") or os.getenv("TURNSTILE_SECRET_KEY") or "").strip()
    if not secret:
        return False, "Turnstile secret is missing."
    if not token:
        return False, "Captcha token missing."

    payload = urllib.parse.urlencode({
        "secret": secret,
        "response": token,
        "remoteip": _client_ip(),
    }).encode("utf-8")

    req = urllib.request.Request(
        "https://challenges.cloudflare.com/turnstile/v0/siteverify",
        data=payload,
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=12) as resp:
            body = json.loads(resp.read().decode("utf-8", errors="replace") or "{}")
    except Exception:
        return False, "Captcha verification failed."

    if bool(body.get("success")):
        return True, ""
    codes = body.get("error-codes") or []
    return False, f"Captcha rejected ({', '.join(codes) if codes else 'unknown'})."


def _send_sms_via_twilio(to_phone_e164: str, body_text: str) -> None:
    account_sid = (current_app.config.get("TWILIO_ACCOUNT_SID") or os.getenv("TWILIO_ACCOUNT_SID") or "").strip()
    auth_token = (current_app.config.get("TWILIO_AUTH_TOKEN") or os.getenv("TWILIO_AUTH_TOKEN") or "").strip()
    from_phone = (current_app.config.get("TWILIO_FROM_NUMBER") or os.getenv("TWILIO_FROM_NUMBER") or "").strip()

    if not all([account_sid, auth_token, from_phone]):
        raise RuntimeError("Twilio is not configured (TWILIO_ACCOUNT_SID/TWILIO_AUTH_TOKEN/TWILIO_FROM_NUMBER).")

    endpoint = f"https://api.twilio.com/2010-04-01/Accounts/{account_sid}/Messages.json"
    payload = urllib.parse.urlencode({
        "From": from_phone,
        "To": to_phone_e164,
        "Body": body_text,
    }).encode("utf-8")

    auth = base64.b64encode(f"{account_sid}:{auth_token}".encode("utf-8")).decode("ascii")
    req = urllib.request.Request(
        endpoint,
        data=payload,
        headers={
            "Authorization": f"Basic {auth}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
        method="POST",
    )

    with urllib.request.urlopen(req, timeout=20) as resp:
        raw = resp.read().decode("utf-8", errors="replace")
        if resp.status < 200 or resp.status >= 300:
            raise RuntimeError(f"Twilio send failed ({resp.status}): {raw[:300]}")
        data = json.loads(raw or "{}")
        if data.get("error_code") or data.get("status") == "failed":
            raise RuntimeError(f"Twilio error: {data.get('error_message') or data.get('message') or 'Unknown error'}")


def _stripe_err_msg(exc: Exception) -> str:
    text_msg = str(exc or "")
    if "No such customer" in text_msg:
        return "Stripe customer record is invalid for the current mode. Please retry checkout."
    if "No such price" in text_msg:
        return "Stripe price is invalid for the current mode. Update STRIPE_PRICE_ID."
    if "Invalid API Key" in text_msg or "api key" in text_msg.lower():
        return "Stripe API key is invalid."
    return text_msg or "Stripe request failed."


def _send_reset_email(to_email: str, reset_url: str) -> None:
    host = current_app.config.get("SMTP_HOST") or os.getenv("SMTP_HOST")
    port = int(current_app.config.get("SMTP_PORT") or os.getenv("SMTP_PORT", "587"))
    user = current_app.config.get("SMTP_USER") or os.getenv("SMTP_USER")
    password = current_app.config.get("SMTP_PASS") or os.getenv("SMTP_PASS")
    mail_from = current_app.config.get("MAIL_FROM") or os.getenv("MAIL_FROM") or user

    if not all([host, port, user, password, mail_from]):
        raise RuntimeError("SMTP is not configured (SMTP_HOST/PORT/USER/PASS/MAIL_FROM).")

    minutes = int((current_app.config.get("PASSWORD_RESET_MAX_AGE_SECONDS", 3600)) / 60)

    subject = "Reset your password"
    html = f"""
    <p>You requested a password reset.</p>
    <p><a href="{reset_url}">Click here to reset your password</a></p>
    <p>This link expires in {minutes} minutes.</p>
    <p>If you didn’t request this, you can ignore this email.</p>
    """

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = mail_from
    msg["To"] = to_email
    msg.set_content("This email requires HTML support.")
    msg.add_alternative(html, subtype="html")

    with smtplib.SMTP(host, port) as smtp:
        smtp.starttls()
        smtp.login(user, password)
        smtp.send_message(msg)


def _send_invoice_pdf_email(to_email: str, subject: str, body_text: str, pdf_path: str) -> None:
    host = current_app.config.get("SMTP_HOST") or os.getenv("SMTP_HOST")
    port = int(current_app.config.get("SMTP_PORT") or os.getenv("SMTP_PORT", "587"))
    user = current_app.config.get("SMTP_USER") or os.getenv("SMTP_USER")
    password = current_app.config.get("SMTP_PASS") or os.getenv("SMTP_PASS")
    mail_from = current_app.config.get("MAIL_FROM") or os.getenv("MAIL_FROM") or user

    if not all([host, port, user, password, mail_from]):
        raise RuntimeError("SMTP is not configured (SMTP_HOST/PORT/USER/PASS/MAIL_FROM).")

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = mail_from
    msg["To"] = to_email
    msg.set_content(body_text)

    with open(pdf_path, "rb") as f:
        data = f.read()

    filename = os.path.basename(pdf_path) or "invoice.pdf"
    msg.add_attachment(data, maintype="application", subtype="pdf", filename=filename)

    with smtplib.SMTP(host, port) as smtp:
        smtp.starttls()
        smtp.login(user, password)
        smtp.send_message(msg)


def _send_schedule_summary_email(to_email: str, subject: str, body_text: str) -> None:
    host = current_app.config.get("SMTP_HOST") or os.getenv("SMTP_HOST")
    port = int(current_app.config.get("SMTP_PORT") or os.getenv("SMTP_PORT", "587"))
    user = current_app.config.get("SMTP_USER") or os.getenv("SMTP_USER")
    password = current_app.config.get("SMTP_PASS") or os.getenv("SMTP_PASS")
    mail_from = current_app.config.get("MAIL_FROM") or os.getenv("MAIL_FROM") or user

    if not all([host, port, user, password, mail_from]):
        raise RuntimeError("SMTP is not configured (SMTP_HOST/PORT/USER/PASS/MAIL_FROM).")

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = mail_from
    msg["To"] = to_email
    msg.set_content(body_text)

    with smtplib.SMTP(host, port) as smtp:
        smtp.starttls()
        smtp.login(user, password)
        smtp.send_message(msg)
    print(f"[SCHEDULE SUMMARY] Sent email to {to_email}", flush=True)


# -----------------------------
# DB migration (lightweight)
# -----------------------------
def _table_exists(engine, table_name: str) -> bool:
    # Postgres
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT 1 FROM information_schema.tables "
                    "WHERE table_schema='public' AND table_name=:t "
                    "LIMIT 1"
                ),
                {"t": table_name},
            ).fetchone()
            if row is not None:
                return True
    except Exception:
        pass

    # SQLite fallback
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text(
                    "SELECT name FROM sqlite_master "
                    "WHERE type='table' AND name=:t "
                    "LIMIT 1"
                ),
                {"t": table_name},
            ).fetchone()
            return row is not None
    except Exception:
        return False


def _column_exists(engine, table_name: str, column_name: str) -> bool:
    # Postgres / general SQL
    try:
        with engine.connect() as conn:
            res = conn.execute(
                text("""
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND table_name = :t
                      AND column_name = :c
                    LIMIT 1
                """),
                {"t": table_name, "c": column_name}
            ).scalar()
            if res:
                return True
    except Exception:
        pass

    # SQLite fallback
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(f"PRAGMA table_info({table_name})")).fetchall()
        return any(r[1] == column_name for r in rows)
    except Exception:
        return False


def _migrate_add_user_id(engine):
    if not _table_exists(engine, "invoices"):
        return

    if not _column_exists(engine, "invoices", "user_id"):
        with engine.begin() as conn:
            try:
                conn.execute(text("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS user_id INTEGER"))
            except Exception:
                conn.execute(text("ALTER TABLE invoices ADD COLUMN user_id INTEGER"))


def _migrate_user_profile_fields(engine):
    if not _table_exists(engine, "users"):
        return

    stmts = []
    if not _column_exists(engine, "users", "business_name"):
        stmts.append("ALTER TABLE users ADD COLUMN business_name VARCHAR(200)")
    if not _column_exists(engine, "users", "phone"):
        stmts.append("ALTER TABLE users ADD COLUMN phone VARCHAR(50)")
    if not _column_exists(engine, "users", "address"):
        stmts.append("ALTER TABLE users ADD COLUMN address VARCHAR(300)")

    if stmts:
        with engine.begin() as conn:
            for st in stmts:
                conn.execute(text(st))


def _migrate_user_email(engine):
    if not _table_exists(engine, "users"):
        return

    if not _column_exists(engine, "users", "email"):
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE users ADD COLUMN email VARCHAR(255)"))

    try:
        with engine.begin() as conn:
            conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS users_email_unique ON users (LOWER(email))"))
    except Exception:
        pass


def _migrate_user_schedule_summary(engine):
    if not _table_exists(engine, "users"):
        return

    stmts = []
    if not _column_exists(engine, "users", "schedule_summary_frequency"):
        stmts.append("ALTER TABLE users ADD COLUMN schedule_summary_frequency VARCHAR(20)")
    if not _column_exists(engine, "users", "schedule_summary_time"):
        stmts.append("ALTER TABLE users ADD COLUMN schedule_summary_time VARCHAR(5)")
    if not _column_exists(engine, "users", "schedule_summary_last_sent"):
        stmts.append("ALTER TABLE users ADD COLUMN schedule_summary_last_sent TIMESTAMP NULL")
    if not _column_exists(engine, "users", "schedule_summary_tz_offset_minutes"):
        stmts.append("ALTER TABLE users ADD COLUMN schedule_summary_tz_offset_minutes INTEGER")

    if stmts:
        with engine.begin() as conn:
            for st in stmts:
                conn.execute(text(st))

    try:
        with engine.begin() as conn:
            conn.execute(text(
                "UPDATE users SET schedule_summary_frequency = 'none' "
                "WHERE schedule_summary_frequency IS NULL"
            ))
            conn.execute(text(
                "UPDATE users SET schedule_summary_tz_offset_minutes = 0 "
                "WHERE schedule_summary_tz_offset_minutes IS NULL"
            ))
    except Exception:
        pass


def _migrate_invoice_contact_fields(engine):
    if not _table_exists(engine, "invoices"):
        return

    stmts = []
    if not _column_exists(engine, "invoices", "customer_email"):
        stmts.append("ALTER TABLE invoices ADD COLUMN customer_email VARCHAR(255)")
    if not _column_exists(engine, "invoices", "customer_phone"):
        stmts.append("ALTER TABLE invoices ADD COLUMN customer_phone VARCHAR(50)")

    if stmts:
        with engine.begin() as conn:
            for st in stmts:
                conn.execute(text(st))


def _migrate_invoice_useful_info(engine):
    if not _table_exists(engine, "invoices"):
        return
    if not _column_exists(engine, "invoices", "useful_info"):
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE invoices ADD COLUMN useful_info TEXT"))


def _migrate_invoice_converted_flag(engine):
    if not _table_exists(engine, "invoices"):
        return
    if not _column_exists(engine, "invoices", "converted_from_estimate"):
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE invoices ADD COLUMN converted_from_estimate BOOLEAN"))
        try:
            with engine.begin() as conn:
                conn.execute(text(
                    "UPDATE invoices SET converted_from_estimate = FALSE "
                    "WHERE converted_from_estimate IS NULL"
                ))
        except Exception:
            pass


def _migrate_estimate_converted_flag(engine):
    if not _table_exists(engine, "invoices"):
        return
    if not _column_exists(engine, "invoices", "converted_to_invoice"):
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE invoices ADD COLUMN converted_to_invoice BOOLEAN"))
        try:
            with engine.begin() as conn:
                conn.execute(text(
                    "UPDATE invoices SET converted_to_invoice = FALSE "
                    "WHERE converted_to_invoice IS NULL"
                ))
        except Exception:
            pass


def _migrate_customer_address_fields(engine):
    if not _table_exists(engine, "customers"):
        return

    stmts = []
    if not _column_exists(engine, "customers", "address_line1"):
        stmts.append("ALTER TABLE customers ADD COLUMN address_line1 VARCHAR(200)")
    if not _column_exists(engine, "customers", "address_line2"):
        stmts.append("ALTER TABLE customers ADD COLUMN address_line2 VARCHAR(200)")
    if not _column_exists(engine, "customers", "city"):
        stmts.append("ALTER TABLE customers ADD COLUMN city VARCHAR(100)")
    if not _column_exists(engine, "customers", "state"):
        stmts.append("ALTER TABLE customers ADD COLUMN state VARCHAR(50)")
    if not _column_exists(engine, "customers", "postal_code"):
        stmts.append("ALTER TABLE customers ADD COLUMN postal_code VARCHAR(20)")

    if stmts:
        with engine.begin() as conn:
            for st in stmts:
                conn.execute(text(st))


def _migrate_user_invoice_template(engine):
    if not _table_exists(engine, "users"):
        return
    if not _column_exists(engine, "users", "invoice_template"):
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE users ADD COLUMN invoice_template VARCHAR(50)"))
        with engine.begin() as conn:
            conn.execute(text("UPDATE users SET invoice_template='auto_repair' WHERE invoice_template IS NULL"))


def _migrate_invoice_template(engine):
    if not _table_exists(engine, "invoices"):
        return
    if not _column_exists(engine, "invoices", "invoice_template"):
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE invoices ADD COLUMN invoice_template VARCHAR(50)"))


def _migrate_user_pdf_template(engine):
    if not _table_exists(engine, "users"):
        return
    if not _column_exists(engine, "users", "pdf_template"):
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE users ADD COLUMN pdf_template VARCHAR(50)"))
        with engine.begin() as conn:
            conn.execute(text("UPDATE users SET pdf_template='classic' WHERE pdf_template IS NULL"))


def _migrate_invoice_pdf_template(engine):
    if not _table_exists(engine, "invoices"):
        return
    if not _column_exists(engine, "invoices", "pdf_template"):
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE invoices ADD COLUMN pdf_template VARCHAR(50)"))
    try:
        with engine.begin() as conn:
            conn.execute(text(
                "UPDATE invoices "
                "SET pdf_template = COALESCE("
                "pdf_template, "
                "(SELECT pdf_template FROM users WHERE users.id = invoices.user_id), "
                "'classic'"
                ") "
                "WHERE pdf_template IS NULL"
            ))
    except Exception:
        pass


def _migrate_user_tax_rate(engine):
    if not _table_exists(engine, "users"):
        return
    if not _column_exists(engine, "users", "tax_rate"):
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE users ADD COLUMN tax_rate FLOAT"))
        with engine.begin() as conn:
            conn.execute(text("UPDATE users SET tax_rate=0 WHERE tax_rate IS NULL"))


def _migrate_user_default_rates(engine):
    if not _table_exists(engine, "users"):
        return
    if not _column_exists(engine, "users", "default_hourly_rate"):
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE users ADD COLUMN default_hourly_rate FLOAT"))
        with engine.begin() as conn:
            conn.execute(text("UPDATE users SET default_hourly_rate=0 WHERE default_hourly_rate IS NULL"))
    if not _column_exists(engine, "users", "default_parts_markup"):
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE users ADD COLUMN default_parts_markup FLOAT"))
        with engine.begin() as conn:
            conn.execute(text("UPDATE users SET default_parts_markup=0 WHERE default_parts_markup IS NULL"))


def _migrate_invoice_tax_fields(engine):
    if not _table_exists(engine, "invoices"):
        return
    if not _column_exists(engine, "invoices", "tax_rate"):
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE invoices ADD COLUMN tax_rate FLOAT"))
    if not _column_exists(engine, "invoices", "tax_override"):
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE invoices ADD COLUMN tax_override FLOAT"))
    try:
        with engine.begin() as conn:
            conn.execute(text(
                "UPDATE invoices "
                "SET tax_rate = COALESCE("
                "tax_rate, "
                "(SELECT tax_rate FROM users WHERE users.id = invoices.user_id), "
                "0"
                ") "
                "WHERE tax_rate IS NULL"
            ))
    except Exception:
        pass


def _migrate_invoice_is_estimate(engine):
    if not _table_exists(engine, "invoices"):
        return
    if not _column_exists(engine, "invoices", "is_estimate"):
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE invoices ADD COLUMN is_estimate BOOLEAN"))
    try:
        with engine.begin() as conn:
            conn.execute(text("UPDATE invoices SET is_estimate = FALSE WHERE is_estimate IS NULL"))
    except Exception:
        pass


def _migrate_invoice_parts_markup_percent(engine):
    if not _table_exists(engine, "invoices"):
        return
    if not _column_exists(engine, "invoices", "parts_markup_percent"):
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE invoices ADD COLUMN parts_markup_percent FLOAT"))
        try:
            with engine.begin() as conn:
                conn.execute(text("UPDATE invoices SET parts_markup_percent = 0.0 WHERE parts_markup_percent IS NULL"))
        except Exception:
            pass


def _migrate_invoice_display_number(engine):
    if not _table_exists(engine, "invoices"):
        return
    if not _column_exists(engine, "invoices", "display_number"):
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE invoices ADD COLUMN display_number VARCHAR(32)"))
        try:
            with engine.begin() as conn:
                conn.execute(text("UPDATE invoices SET display_number = invoice_number WHERE display_number IS NULL"))
        except Exception:
            pass


def _migrate_invoice_display_sequences(engine):
    if _table_exists(engine, "invoice_display_sequences"):
        return
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS invoice_display_sequences (
                    id INTEGER PRIMARY KEY,
                    user_id INTEGER NOT NULL,
                    year INTEGER NOT NULL,
                    doc_type VARCHAR(20) NOT NULL,
                    last_seq INTEGER NOT NULL DEFAULT 0,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
            """))
            conn.execute(text("""
                CREATE UNIQUE INDEX IF NOT EXISTS uq_invoice_display_seq_user_year_type
                ON invoice_display_sequences (user_id, year, doc_type)
            """))
    except Exception:
        pass


def _migrate_user_billing_fields(engine):
    if not _table_exists(engine, "users"):
        return

    stmts = []
    if not _column_exists(engine, "users", "stripe_customer_id"):
        stmts.append("ALTER TABLE users ADD COLUMN stripe_customer_id VARCHAR(255)")
    if not _column_exists(engine, "users", "stripe_subscription_id"):
        stmts.append("ALTER TABLE users ADD COLUMN stripe_subscription_id VARCHAR(255)")
    if not _column_exists(engine, "users", "subscription_status"):
        stmts.append("ALTER TABLE users ADD COLUMN subscription_status VARCHAR(50)")
    if not _column_exists(engine, "users", "trial_ends_at"):
        stmts.append("ALTER TABLE users ADD COLUMN trial_ends_at TIMESTAMP NULL")
    if not _column_exists(engine, "users", "current_period_end"):
        stmts.append("ALTER TABLE users ADD COLUMN current_period_end TIMESTAMP NULL")
    if not _column_exists(engine, "users", "trial_used_at"):
        stmts.append("ALTER TABLE users ADD COLUMN trial_used_at TIMESTAMP NULL")

    if stmts:
        with engine.begin() as conn:
            for st in stmts:
                conn.execute(text(st))

    try:
        with engine.begin() as conn:
            conn.execute(text("CREATE INDEX IF NOT EXISTS users_stripe_customer_idx ON users (stripe_customer_id)"))
            conn.execute(text("CREATE INDEX IF NOT EXISTS users_stripe_sub_idx ON users (stripe_subscription_id)"))
    except Exception:
        pass


def _migrate_user_security_fields(engine):
    if not _table_exists(engine, "users"):
        return

    stmts = []
    if not _column_exists(engine, "users", "failed_login_attempts"):
        stmts.append("ALTER TABLE users ADD COLUMN failed_login_attempts INTEGER")
    if not _column_exists(engine, "users", "password_reset_required"):
        stmts.append("ALTER TABLE users ADD COLUMN password_reset_required BOOLEAN")
    if not _column_exists(engine, "users", "last_failed_login"):
        stmts.append("ALTER TABLE users ADD COLUMN last_failed_login TIMESTAMP NULL")

    if stmts:
        with engine.begin() as conn:
            for st in stmts:
                conn.execute(text(st))

        with engine.begin() as conn:
            conn.execute(text("UPDATE users SET failed_login_attempts = 0 WHERE failed_login_attempts IS NULL"))
            conn.execute(text("UPDATE users SET password_reset_required = FALSE WHERE password_reset_required IS NULL"))


def _migrate_customers(engine):
    if not _table_exists(engine, "invoices"):
        return

    if not _column_exists(engine, "invoices", "customer_id"):
        with engine.begin() as conn:
            try:
                conn.execute(text("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS customer_id INTEGER"))
            except Exception:
                conn.execute(text("ALTER TABLE invoices ADD COLUMN customer_id INTEGER"))


def _migrate_invoice_customer_id(engine):
    if not _table_exists(engine, "invoices"):
        return
    if not _column_exists(engine, "invoices", "customer_id"):
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE invoices ADD COLUMN customer_id INTEGER"))


def _migrate_customers_unique_name_ci(engine):
    """
    Recommended: enforce per-user customer uniqueness case-insensitively on Postgres:
      UNIQUE (user_id, LOWER(name))
    Safe to no-op on sqlite.
    """
    if not _table_exists(engine, "customers"):
        return
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                CREATE UNIQUE INDEX IF NOT EXISTS customers_user_lower_name_unique
                ON customers (user_id, LOWER(name));
            """))
    except Exception:
        pass


def _migrate_user_logo(engine):
    if not _table_exists(engine, "users"):
        return
    if not _column_exists(engine, "users", "logo_path"):
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE users ADD COLUMN logo_path VARCHAR(300)"))
    if not _column_exists(engine, "users", "logo_blob"):
        with engine.begin() as conn:
            try:
                # Postgres
                conn.execute(text("ALTER TABLE users ADD COLUMN logo_blob BYTEA"))
            except Exception:
                # SQLite fallback
                conn.execute(text("ALTER TABLE users ADD COLUMN logo_blob BLOB"))
    if not _column_exists(engine, "users", "logo_blob_mime"):
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE users ADD COLUMN logo_blob_mime VARCHAR(50)"))


def _migrate_user_logo_backfill_blob(engine):
    """
    Best-effort backfill of existing file-based logos into DB blob storage.
    Safe to skip on any error.
    """
    if not _table_exists(engine, "users"):
        return
    if not _column_exists(engine, "users", "logo_path") or not _column_exists(engine, "users", "logo_blob"):
        return
    try:
        with engine.begin() as conn:
            rows = conn.execute(text("""
                SELECT id, logo_path
                FROM users
                WHERE logo_path IS NOT NULL
                  AND logo_path != ''
                  AND logo_blob IS NULL
            """)).fetchall()

            for uid, rel in rows:
                try:
                    abs_path = (Path("instance") / str(rel)).resolve()
                    if not abs_path.exists():
                        continue
                    with open(abs_path, "rb") as f:
                        raw = f.read()
                    img = Image.open(io.BytesIO(raw))
                    img.load()
                    img.thumbnail((700, 260), _IMG_RESAMPLE)
                    if img.mode not in ("RGB", "RGBA"):
                        img = img.convert("RGBA")
                    out = io.BytesIO()
                    img.save(out, format="PNG", optimize=True, compress_level=9)
                    conn.execute(
                        text("UPDATE users SET logo_blob = :blob, logo_blob_mime = 'image/png' WHERE id = :uid"),
                        {"blob": out.getvalue(), "uid": uid},
                    )
                except Exception:
                    continue
    except Exception:
        pass


def _migrate_schedule_events(engine):
    # create_all handles it once ScheduleEvent exists in models.py
    return


def _migrate_schedule_event_auto_fields(engine):
    """
    Adds fields so we can distinguish auto-generated recurring events from manual appointments.
    """
    if not _table_exists(engine, "schedule_events"):
        return

    stmts = []
    if not _column_exists(engine, "schedule_events", "is_auto"):
        stmts.append("ALTER TABLE schedule_events ADD COLUMN is_auto BOOLEAN")
    if not _column_exists(engine, "schedule_events", "recurring_token"):
        stmts.append("ALTER TABLE schedule_events ADD COLUMN recurring_token VARCHAR(100)")

    if stmts:
        with engine.begin() as conn:
            for st in stmts:
                conn.execute(text(st))

    # backfill default
    try:
        with engine.begin() as conn:
            conn.execute(text("UPDATE schedule_events SET is_auto = FALSE WHERE is_auto IS NULL"))
    except Exception:
        pass

    # best-effort indexes
    try:
        with engine.begin() as conn:
            conn.execute(text(
                "CREATE INDEX IF NOT EXISTS schedule_events_is_auto_idx ON schedule_events (is_auto)"
            ))
            conn.execute(text(
                "CREATE INDEX IF NOT EXISTS schedule_events_recurring_token_idx ON schedule_events (recurring_token)"
            ))
    except Exception:
        pass


def _migrate_schedule_event_type(engine):
    """
    Adds event_type so we can store appointment vs. block-off time entries.
    """
    if not _table_exists(engine, "schedule_events"):
        return

    if not _column_exists(engine, "schedule_events", "event_type"):
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE schedule_events ADD COLUMN event_type VARCHAR(20)"))

    try:
        with engine.begin() as conn:
            conn.execute(text(
                "UPDATE schedule_events SET event_type = 'appointment' WHERE event_type IS NULL"
            ))
    except Exception:
        pass


def _migrate_schedule_event_invoice_id(engine):
    if not _table_exists(engine, "schedule_events"):
        return
    if not _column_exists(engine, "schedule_events", "invoice_id"):
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE schedule_events ADD COLUMN invoice_id INTEGER"))


def _migrate_customer_schedule_fields(engine):
    """
    Adds recurring service fields onto customers.
    """
    if not _table_exists(engine, "customers"):
        return

    stmts = []
    if not _column_exists(engine, "customers", "next_service_dt"):
        stmts.append("ALTER TABLE customers ADD COLUMN next_service_dt TIMESTAMP NULL")
    if not _column_exists(engine, "customers", "service_interval_days"):
        stmts.append("ALTER TABLE customers ADD COLUMN service_interval_days INTEGER")
    if not _column_exists(engine, "customers", "default_service_minutes"):
        stmts.append("ALTER TABLE customers ADD COLUMN default_service_minutes INTEGER")
    if not _column_exists(engine, "customers", "service_title"):
        stmts.append("ALTER TABLE customers ADD COLUMN service_title VARCHAR(200)")
    if not _column_exists(engine, "customers", "service_notes"):
        stmts.append("ALTER TABLE customers ADD COLUMN service_notes VARCHAR(1000)")
    if not _column_exists(engine, "customers", "recurring_horizon_dt"):
        stmts.append("ALTER TABLE customers ADD COLUMN recurring_horizon_dt TIMESTAMP NULL")

    if stmts:
        with engine.begin() as conn:
            for st in stmts:
                conn.execute(text(st))

    # backfill default duration
    try:
        with engine.begin() as conn:
            conn.execute(text(
                "UPDATE customers SET default_service_minutes = 60 WHERE default_service_minutes IS NULL"
            ))
    except Exception:
        pass


# -----------------------------
# App factory
# -----------------------------
def create_app():
    _ensure_dirs()

    app = Flask(__name__)
    app.config.from_object(Config)

    app.config.setdefault("APP_BASE_URL", os.getenv("APP_BASE_URL", "").rstrip("/"))
    app.config.setdefault("PASSWORD_RESET_MAX_AGE_SECONDS", int(os.getenv("PASSWORD_RESET_MAX_AGE_SECONDS", "3600")))
    app.config.setdefault("PASSWORD_RESET_SALT", os.getenv("PASSWORD_RESET_SALT", "password-reset"))

    app.config.setdefault("SMTP_HOST", os.getenv("SMTP_HOST"))
    app.config.setdefault("SMTP_PORT", int(os.getenv("SMTP_PORT", "587")))
    app.config.setdefault("SMTP_USER", os.getenv("SMTP_USER"))
    app.config.setdefault("SMTP_PASS", os.getenv("SMTP_PASS"))
    app.config.setdefault("MAIL_FROM", os.getenv("MAIL_FROM", os.getenv("SMTP_USER", "no-reply@example.com")))
    app.config.setdefault("TURNSTILE_SITE_KEY", os.getenv("TURNSTILE_SITE_KEY", ""))
    app.config.setdefault("TURNSTILE_SECRET_KEY", os.getenv("TURNSTILE_SECRET_KEY", ""))

    login_manager.init_app(app)

    engine = make_engine(Config.SQLALCHEMY_DATABASE_URI, echo=Config.SQLALCHEMY_ECHO)

    Base.metadata.create_all(bind=engine)

    _migrate_add_user_id(engine)
    _migrate_user_profile_fields(engine)
    _migrate_user_email(engine)
    _migrate_user_schedule_summary(engine)
    _migrate_invoice_contact_fields(engine)
    _migrate_invoice_useful_info(engine)
    _migrate_invoice_converted_flag(engine)
    _migrate_estimate_converted_flag(engine)
    _migrate_user_invoice_template(engine)
    _migrate_invoice_template(engine)
    _migrate_user_pdf_template(engine)
    _migrate_invoice_pdf_template(engine)
    _migrate_user_tax_rate(engine)
    _migrate_user_default_rates(engine)
    _migrate_invoice_tax_fields(engine)
    _migrate_invoice_is_estimate(engine)
    _migrate_invoice_parts_markup_percent(engine)
    _migrate_invoice_display_number(engine)
    _migrate_invoice_display_sequences(engine)
    _migrate_user_billing_fields(engine)
    _migrate_user_security_fields(engine)
    _migrate_customers(engine)
    _migrate_customers_unique_name_ci(engine)
    _migrate_invoice_customer_id(engine)
    _migrate_user_logo(engine)
    _migrate_user_logo_backfill_blob(engine)
    _migrate_schedule_events(engine)
    _migrate_schedule_event_auto_fields(engine)   # <-- schedule is_auto/recurring_token
    _migrate_schedule_event_type(engine)
    _migrate_schedule_event_invoice_id(engine)
    _migrate_customer_schedule_fields(engine)
    _migrate_customer_address_fields(engine)

    SessionLocal = make_session_factory(engine)

    def db_session():
        return SessionLocal()

    @app.context_processor
    def inject_billing():
        if not current_user.is_authenticated:
            return {"format_phone": _format_phone_display}

        with db_session() as s:
            u = s.get(User, _current_user_id_int())
            status = (getattr(u, "subscription_status", None) or "").strip().lower()
            is_sub = status in ("trialing", "active")
            trial_used = bool(getattr(u, "trial_used_at", None)) if u else False

        return {
            "billing_status": status or None,
            "is_subscribed": is_sub,
            "trial_used": trial_used,
            "format_phone": _format_phone_display,
        }

    @login_manager.user_loader
    def load_user(user_id: str):
        try:
            uid = int(user_id)
        except Exception:
            return None
        with db_session() as s:
            u = s.get(User, uid)
            if not u:
                return None
            return AppUser(u.id, u.username)

    def _bootstrap_first_user():
        username = os.getenv("INITIAL_ADMIN_USERNAME", "admin")
        password = os.getenv("INITIAL_ADMIN_PASSWORD", os.getenv("ADMIN_PASSWORD", "changeme"))
        email = _normalize_email(os.getenv("INITIAL_ADMIN_EMAIL", ""))

        with db_session() as s:
            first = s.query(User).order_by(User.id.asc()).first()

            if first:
                if not (getattr(first, "email", None) or "").strip() and _looks_like_email(email):
                    already = (
                        s.query(User)
                        .filter(text("lower(email) = :e"))
                        .params(e=email)
                        .first()
                    )
                    if not already:
                        first.email = email
                        s.commit()
                return

            if not _looks_like_email(email):
                email = "no-reply@placeholder.local"

            u = User(username=username, email=email, password_hash=generate_password_hash(password))
            s.add(u)
            s.commit()

            s.query(Invoice).filter(Invoice.user_id.is_(None)).update({"user_id": u.id})
            s.commit()

    _bootstrap_first_user()

    def _backfill_customers_from_invoices():
        """
        One-time-ish backfill:
        For any invoice with customer_id NULL, create/find a Customer per user using old Invoice.name,
        link invoice.customer_id to that customer.
        """
        with db_session() as s:
            invs = (
                s.query(Invoice)
                .filter(Invoice.customer_id.is_(None))
                .filter(Invoice.user_id.isnot(None))
                .all()
            )
            if not invs:
                return

            cache = {}
            changed = 0

            for inv in invs:
                user_id = inv.user_id
                cname = (inv.name or "").strip()
                if not cname:
                    continue

                key = (user_id, cname.lower())
                cust = cache.get(key)
                if not cust:
                    cust = (
                        s.query(Customer)
                        .filter(Customer.user_id == user_id)
                        .filter(text("lower(name) = :n")).params(n=cname.lower())
                        .first()
                    )
                    if not cust:
                        cust = Customer(user_id=user_id, name=cname)
                        s.add(cust)
                        s.flush()
                    cache[key] = cust

                inv.customer_id = cust.id
                changed += 1

            s.commit()
            if changed:
                print(f"[MIGRATE] Backfilled customers for {changed} invoices", flush=True)

    _backfill_customers_from_invoices()

    # -----------------------------
    # Customer merge helper
    # -----------------------------
    def _merge_customers(session, source: Customer, target: Customer) -> None:
        """
        Merge source customer into target customer (same user).
        - Reassign invoices.customer_id from source -> target
        - Optionally fill missing target contact fields from source
        - Update Invoice.name to target.name for moved invoices
        - Delete the source customer
        """
        if not source or not target:
            raise ValueError("source/target missing")
        if source.id == target.id:
            return
        if source.user_id != target.user_id:
            raise ValueError("Cannot merge across users")

        for field in ("email", "phone", "address"):
            tv = (getattr(target, field, None) or "").strip()
            sv = (getattr(source, field, None) or "").strip()
            if (not tv) and sv:
                setattr(target, field, sv)

        moved = (
            session.query(Invoice)
            .filter(Invoice.user_id == target.user_id)
            .filter(Invoice.customer_id == source.id)
            .all()
        )
        for inv in moved:
            inv.customer_id = target.id
            inv.name = (target.name or "").strip()

        session.delete(source)

    # -----------------------------
    # Subscription gating
    # -----------------------------
    def _is_subscribed(u: User) -> bool:
        status = (getattr(u, "subscription_status", None) or "").strip().lower()
        return status in ("trialing", "active")

    def subscription_required(view_fn):
        @wraps(view_fn)
        def wrapper(*args, **kwargs):
            with db_session() as s:
                u = s.get(User, _current_user_id_int())
                if not u:
                    abort(403)
                if not _is_subscribed(u):
                    return redirect(url_for("billing"))
            return view_fn(*args, **kwargs)
        return wrapper

    # -----------------------------
    # Recurring scheduler helpers
    # -----------------------------
    def _delete_future_recurring_events(session, customer: Customer, from_dt: datetime | None = None) -> int:
        """
        Delete future auto-generated recurring events for a specific customer.
        Manual events are not touched (is_auto=False).
        """
        if not customer:
            return 0
        if from_dt is None:
            from_dt = datetime.utcnow()

        token = f"cust:{customer.id}"

        q = (
            session.query(ScheduleEvent)
            .filter(ScheduleEvent.user_id == customer.user_id)
            .filter(ScheduleEvent.customer_id == customer.id)
            .filter(ScheduleEvent.is_auto.is_(True))
            .filter(ScheduleEvent.recurring_token == token)
            .filter(ScheduleEvent.start_dt >= from_dt)
        )

        count = q.count()
        q.delete(synchronize_session=False)
        return count

    def _delete_all_recurring_events(session, customer: Customer) -> int:
        """
        Delete ALL auto-generated recurring events for this customer (past + future).
        This is used when user clicks the "Discontinue Recurring" button.
        """
        if not customer:
            return 0

        token = f"cust:{customer.id}"

        q = (
            session.query(ScheduleEvent)
            .filter(ScheduleEvent.user_id == customer.user_id)
            .filter(ScheduleEvent.customer_id == customer.id)
            .filter(ScheduleEvent.is_auto.is_(True))
            .filter(ScheduleEvent.recurring_token == token)
        )

        count = q.count()
        q.delete(synchronize_session=False)
        return count

    def _ensure_recurring_events(session, customer: Customer, horizon_days: int = 90) -> int:
        """
        Creates future ScheduleEvent rows up to horizon_days.
        Uses customer.next_service_dt as the next occurrence to generate.
        Advances next_service_dt forward as events are created.

        NOTE: Only auto-generated events are marked is_auto=True + recurring_token.
        """
        if not customer:
            return 0

        next_dt = getattr(customer, "next_service_dt", None)
        interval = getattr(customer, "service_interval_days", None)

        if not next_dt or not interval or int(interval) < 1:
            return 0

        minutes = int(getattr(customer, "default_service_minutes", 60) or 60)
        title_default = (getattr(customer, "service_title", None) or "").strip() or None
        notes_default = (getattr(customer, "service_notes", None) or "").strip() or None

        created = 0
        horizon_base = datetime.utcnow()
        if next_dt and next_dt > horizon_base:
            horizon_base = next_dt
        horizon_end = horizon_base + timedelta(days=horizon_days)
        recurring_horizon = getattr(customer, "recurring_horizon_dt", None)
        if recurring_horizon:
            horizon_end = min(horizon_end, recurring_horizon)

        token = f"cust:{customer.id}"

        # generate until beyond horizon
        while next_dt <= horizon_end:
            exists = (
                session.query(ScheduleEvent)
                .filter(ScheduleEvent.user_id == customer.user_id)
                .filter(ScheduleEvent.customer_id == customer.id)
                .filter(ScheduleEvent.start_dt == next_dt)
                .filter(ScheduleEvent.is_auto.is_(True))
                .filter(ScheduleEvent.recurring_token == token)
                .first()
            )
            if not exists:
                end_dt = next_dt + timedelta(minutes=minutes)
                ev = ScheduleEvent(
                    user_id=customer.user_id,
                    customer_id=customer.id,
                    title=title_default or customer.name,
                    notes=notes_default or None,
                    start_dt=next_dt,
                    end_dt=end_dt,

                    is_auto=True,
                    recurring_token=token,
                )
                session.add(ev)
                created += 1

            # advance pointer
            next_dt = next_dt + timedelta(days=int(interval))
            customer.next_service_dt = next_dt

        return created

    # -----------------------------
    # Stripe Billing
    # -----------------------------
    stripe.api_key = app.config.get("STRIPE_SECRET_KEY") or os.getenv("STRIPE_SECRET_KEY")
    STRIPE_PRICE_ID = app.config.get("STRIPE_PRICE_ID") or os.getenv("STRIPE_PRICE_ID")
    STRIPE_PUBLISHABLE_KEY = app.config.get("STRIPE_PUBLISHABLE_KEY") or os.getenv("STRIPE_PUBLISHABLE_KEY")
    STRIPE_WEBHOOK_SECRET = app.config.get("STRIPE_WEBHOOK_SECRET") or os.getenv("STRIPE_WEBHOOK_SECRET")

    def _base_url():
        base = (current_app.config.get("APP_BASE_URL") or "").rstrip("/")
        return base or request.host_url.rstrip("/")

    # -----------------------------
    # Auth routes
    # -----------------------------
    @app.route("/login", methods=["GET", "POST"])
    def login():
        if current_user.is_authenticated:
            return redirect(url_for("customers_list"))

        if request.method == "POST":
            username = (request.form.get("username") or "").strip()
            password = request.form.get("password") or ""

            with db_session() as s:
                u = s.query(User).filter(User.username == username).first()

                generic_fail_msg = "Invalid username or password."

                if not u:
                    _audit_log(
                        s,
                        event="auth.login",
                        result="fail",
                        username=username,
                        details="user_not_found",
                    )
                    flash(generic_fail_msg, "error")
                    return render_template("login.html")

                if bool(getattr(u, "password_reset_required", False)):
                    _audit_log(
                        s,
                        event="auth.login",
                        result="blocked",
                        user_id=u.id,
                        username=u.username,
                        email=u.email,
                        details="password_reset_required",
                    )
                    flash("Too many failed login attempts. Please reset your password.", "error")
                    return redirect(url_for("forgot_password"))

                if check_password_hash(u.password_hash, password):
                    u.failed_login_attempts = 0
                    u.password_reset_required = False
                    u.last_failed_login = None
                    _audit_log(
                        s,
                        event="auth.login",
                        result="success",
                        user_id=u.id,
                        username=u.username,
                        email=u.email,
                    )
                    s.commit()

                    login_user(AppUser(u.id, u.username))
                    return redirect(url_for("customers_list"))

                attempts = int(getattr(u, "failed_login_attempts", 0) or 0) + 1
                u.failed_login_attempts = attempts
                u.last_failed_login = datetime.utcnow()

                if attempts >= 6:
                    u.password_reset_required = True
                    _audit_log(
                        s,
                        event="auth.login",
                        result="blocked",
                        user_id=u.id,
                        username=u.username,
                        email=u.email,
                        details=f"failed_attempts={attempts}",
                    )
                    s.commit()
                    flash("Too many failed login attempts. Please reset your password.", "error")
                    return redirect(url_for("forgot_password"))

                _audit_log(
                    s,
                    event="auth.login",
                    result="fail",
                    user_id=u.id,
                    username=u.username,
                    email=u.email,
                    details=f"wrong_password_attempt={attempts}",
                )
                s.commit()
                flash(generic_fail_msg, "error")
                return render_template("login.html")

        return render_template("login.html")

    @app.route("/register", methods=["GET", "POST"])
    def register():
        if current_user.is_authenticated:
            return redirect(url_for("customers_list"))
        turnstile_site_key = _turnstile_site_key()

        if request.method == "POST":
            username = (request.form.get("username") or "").strip()
            email = _normalize_email(request.form.get("email") or "")
            password = request.form.get("password") or ""
            confirm = request.form.get("confirm") or ""
            if _turnstile_enabled():
                token = (request.form.get("cf-turnstile-response") or "").strip()
                ok, reason = _verify_turnstile(token)
                if not ok:
                    with db_session() as s:
                        _audit_log(
                            s,
                            event="auth.register",
                            result="blocked",
                            username=username,
                            email=email,
                            details=f"captcha_failed:{reason}",
                        )
                        s.commit()
                    flash("Captcha verification failed. Please try again.", "error")
                    return render_template("register.html", turnstile_site_key=turnstile_site_key)

            if not username or len(username) < 3:
                with db_session() as s:
                    _audit_log(s, event="auth.register", result="fail", username=username, email=email, details="invalid_username")
                    s.commit()
                flash("Username must be at least 3 characters.", "error")
                return render_template("register.html", turnstile_site_key=turnstile_site_key)

            if not _looks_like_email(email):
                with db_session() as s:
                    _audit_log(s, event="auth.register", result="fail", username=username, email=email, details="invalid_email")
                    s.commit()
                flash("A valid email address is required.", "error")
                return render_template("register.html", turnstile_site_key=turnstile_site_key)

            if not password or len(password) < 6:
                with db_session() as s:
                    _audit_log(s, event="auth.register", result="fail", username=username, email=email, details="short_password")
                    s.commit()
                flash("Password must be at least 6 characters.", "error")
                return render_template("register.html", turnstile_site_key=turnstile_site_key)

            if password != confirm:
                with db_session() as s:
                    _audit_log(s, event="auth.register", result="fail", username=username, email=email, details="password_mismatch")
                    s.commit()
                flash("Passwords do not match.", "error")
                return render_template("register.html", turnstile_site_key=turnstile_site_key)

            with db_session() as s:
                taken_user = s.query(User).filter(User.username == username).first()
                if taken_user:
                    _audit_log(s, event="auth.register", result="fail", username=username, email=email, details="username_taken")
                    s.commit()
                    flash("That username is already taken.", "error")
                    return render_template("register.html", turnstile_site_key=turnstile_site_key)

                taken_email = (
                    s.query(User)
                    .filter(text("lower(email) = :e"))
                    .params(e=email)
                    .first()
                )
                if taken_email:
                    _audit_log(s, event="auth.register", result="fail", username=username, email=email, details="email_taken")
                    s.commit()
                    flash("That email is already registered.", "error")
                    return render_template("register.html", turnstile_site_key=turnstile_site_key)

                u = User(username=username, email=email, password_hash=generate_password_hash(password))
                s.add(u)
                s.flush()
                _audit_log(s, event="auth.register", result="success", user_id=u.id, username=u.username, email=u.email)
                s.commit()

                login_user(AppUser(u.id, u.username))
                return redirect(url_for("customers_list"))

        return render_template("register.html", turnstile_site_key=turnstile_site_key)

    @app.route("/forgot-password", methods=["GET", "POST"])
    def forgot_password():
        if current_user.is_authenticated:
            return redirect(url_for("customers_list"))

        if request.method == "POST":
            email = _normalize_email(request.form.get("email") or "")
            flash("If that email exists, we sent a password reset link.", "info")

            if _looks_like_email(email):
                with db_session() as s:
                    u = (
                        s.query(User)
                        .filter(text("lower(email) = :e"))
                        .params(e=email)
                        .first()
                    )
                    if u:
                        token = make_password_reset_token(u.id)

                        base = (current_app.config.get("APP_BASE_URL") or "").rstrip("/")
                        if not base:
                            base = request.host_url.rstrip("/")

                        reset_url = f"{base}{url_for('reset_password', token=token)}"

                        try:
                            _send_reset_email(email, reset_url)
                            print(f"[RESET] Sent reset email to {email}", flush=True)
                        except Exception as e:
                            print(f"[RESET] SMTP ERROR for {email}: {repr(e)}", flush=True)

            return redirect(url_for("login"))

        return render_template("forgot_password.html")

    @app.route("/reset-password/<token>", methods=["GET", "POST"])
    def reset_password(token: str):
        if current_user.is_authenticated:
            return redirect(url_for("customers_list"))

        max_age = int(current_app.config.get("PASSWORD_RESET_MAX_AGE_SECONDS", 3600))
        user_id = read_password_reset_token(token, max_age_seconds=max_age)
        if not user_id:
            flash("Reset link is invalid or expired. Please request a new one.", "error")
            return redirect(url_for("forgot_password"))

        if request.method == "POST":
            pw1 = request.form.get("password") or ""
            pw2 = request.form.get("confirm_password") or ""

            if not pw1 or len(pw1) < 6:
                flash("Password must be at least 6 characters.", "error")
                return render_template("reset_password.html", token=token)

            if pw1 != pw2:
                flash("Passwords do not match.", "error")
                return render_template("reset_password.html", token=token)

            with db_session() as s:
                u = s.get(User, int(user_id))
                if not u:
                    flash("Reset link is invalid or expired. Please request a new one.", "error")
                    return redirect(url_for("forgot_password"))

                u.password_hash = generate_password_hash(pw1)
                u.failed_login_attempts = 0
                u.password_reset_required = False
                u.last_failed_login = None
                s.commit()

            flash("Password updated. Please log in.", "success")
            return redirect(url_for("login"))

        return render_template("reset_password.html", token=token)

    @app.route("/logout")
    @login_required
    def logout():
        logout_user()
        return redirect(url_for("login"))

    # -----------------------------
    # User Settings
    # -----------------------------
    @app.route("/settings", methods=["GET", "POST"])
    @login_required
    def settings():
        with db_session() as s:
            u = s.get(User, _current_user_id_int())
            if not u:
                abort(404)

            if request.method == "POST":
                new_email = _normalize_email(request.form.get("email") or "")
                if not _looks_like_email(new_email):
                    flash("Please enter a valid email address.", "error")
                    return render_template("settings.html", u=u, templates=INVOICE_TEMPLATES, pdf_templates=PDF_TEMPLATES)

                if (u.email or "").strip().lower() != new_email:
                    taken_email = (
                        s.query(User)
                        .filter(text("lower(email) = :e AND id != :id"))
                        .params(e=new_email, id=u.id)
                        .first()
                    )
                    if taken_email:
                        flash("That email is already in use.", "error")
                        return render_template("settings.html", u=u, templates=INVOICE_TEMPLATES, pdf_templates=PDF_TEMPLATES)

                    u.email = new_email

                # Logo upload / remove
                remove_logo = (request.form.get("remove_logo") or "").strip() == "1"
                logo_file = request.files.get("logo")

                if remove_logo:
                    old_rel = (getattr(u, "logo_path", None) or "").strip()
                    if old_rel:
                        old_abs = (Path("instance") / old_rel).resolve()
                        try:
                            if old_abs.exists():
                                old_abs.unlink()
                        except Exception:
                            pass
                    u.logo_path = None
                    u.logo_blob = None
                    u.logo_blob_mime = None

                elif logo_file and getattr(logo_file, "filename", ""):
                    filename = secure_filename(logo_file.filename or "")
                    ext = (os.path.splitext(filename)[1] or "").lower()

                    if ext not in (".png", ".jpg", ".jpeg"):
                        flash("Logo must be a .png, .jpg, or .jpeg file.", "error")
                        return render_template("settings.html", u=u, templates=INVOICE_TEMPLATES, pdf_templates=PDF_TEMPLATES)

                    try:
                        png_bytes = _process_logo_upload_to_png_bytes(logo_file)
                    except ValueError as exc:
                        flash(str(exc), "error")
                        return render_template("settings.html", u=u, templates=INVOICE_TEMPLATES, pdf_templates=PDF_TEMPLATES)

                    d = _logo_upload_dir()
                    out_name = f"user_{u.id}{ext}"
                    out_path = (d / out_name).resolve()

                    old_rel = (getattr(u, "logo_path", None) or "").strip()
                    if old_rel:
                        old_abs = (Path("instance") / old_rel).resolve()
                        try:
                            if old_abs.exists() and old_abs != out_path:
                                old_abs.unlink()
                        except Exception:
                            pass

                    # Keep writing a file as a backward-compatible fallback for old flows.
                    try:
                        logo_file.stream.seek(0)
                        logo_file.save(out_path)
                        u.logo_path = str(Path("uploads") / "logos" / out_name)
                    except Exception:
                        u.logo_path = None

                    # Primary persistence: DB-stored compressed PNG.
                    u.logo_blob = png_bytes
                    u.logo_blob_mime = "image/png"

                tmpl = (request.form.get("invoice_template") or "").strip()
                tmpl = _template_key_fallback(tmpl)
                u.invoice_template = tmpl

                pdf_tmpl = _pdf_template_key_fallback(request.form.get("pdf_template"))
                u.pdf_template = pdf_tmpl

                u.tax_rate = _to_float(request.form.get("tax_rate"), 0.0)
                u.default_hourly_rate = _to_float(request.form.get("default_hourly_rate"), 0.0)
                u.default_parts_markup = _to_float(request.form.get("default_parts_markup"), 0.0)

                u.business_name = (request.form.get("business_name") or "").strip() or None
                u.phone = (request.form.get("phone") or "").strip() or None
                u.address = (request.form.get("address") or "").strip() or None

                summary_freq = (request.form.get("schedule_summary_frequency") or "none").strip().lower()
                summary_time_raw = request.form.get("schedule_summary_time") or ""
                summary_time = _parse_summary_time(summary_time_raw)
                summary_tz_offset_raw = (request.form.get("schedule_summary_tz_offset_minutes") or "").strip()
                summary_tz_offset = 0
                if summary_tz_offset_raw:
                    try:
                        summary_tz_offset = int(summary_tz_offset_raw)
                    except ValueError:
                        flash("Invalid schedule summary time zone offset.", "error")
                        return render_template("settings.html", u=u, templates=INVOICE_TEMPLATES, pdf_templates=PDF_TEMPLATES)
                if summary_tz_offset < -720 or summary_tz_offset > 840:
                    flash("Invalid schedule summary time zone offset.", "error")
                    return render_template("settings.html", u=u, templates=INVOICE_TEMPLATES, pdf_templates=PDF_TEMPLATES)

                if summary_freq not in ("none", "day", "week", "month"):
                    flash("Invalid schedule summary frequency.", "error")
                    return render_template("settings.html", u=u, templates=INVOICE_TEMPLATES, pdf_templates=PDF_TEMPLATES)

                if summary_freq != "none" and not summary_time:
                    flash("Please choose a time for schedule summaries.", "error")
                    return render_template("settings.html", u=u, templates=INVOICE_TEMPLATES, pdf_templates=PDF_TEMPLATES)

                u.schedule_summary_frequency = summary_freq
                u.schedule_summary_time = summary_time if summary_freq != "none" else None
                u.schedule_summary_tz_offset_minutes = summary_tz_offset

                s.commit()
                flash("Settings saved.", "success")
                return redirect(url_for("settings"))

            return render_template("settings.html", u=u, templates=INVOICE_TEMPLATES, pdf_templates=PDF_TEMPLATES)

    @app.post("/settings/schedule-summary/test")
    @login_required
    def schedule_summary_test():
        now = datetime.utcnow()
        with db_session() as s:
            u = s.get(User, _current_user_id_int())
            if not u:
                abort(404)

            freq = (u.schedule_summary_frequency or "none").lower().strip()
            if freq == "none":
                flash("Schedule summary frequency is set to None.", "info")
                return redirect(url_for("settings"))

            summary_time = _parse_summary_time(u.schedule_summary_time or "")
            if not summary_time:
                flash("Please set a summary start time first.", "error")
                return redirect(url_for("settings"))

            to_email = _normalize_email(u.email or "")
            if not _looks_like_email(to_email):
                flash("Your account email is missing or invalid.", "error")
                return redirect(url_for("settings"))

            start, end, tz_label, _now_local = _summary_window_for_user(u, now)
            events = (
                s.query(ScheduleEvent)
                .filter(ScheduleEvent.user_id == u.id)
                .filter(ScheduleEvent.status == "scheduled")
                .filter(or_(ScheduleEvent.event_type.is_(None), ScheduleEvent.event_type != "block"))
                .filter(ScheduleEvent.start_dt < end)
                .filter(ScheduleEvent.end_dt > start)
                .order_by(ScheduleEvent.start_dt.asc())
                .all()
            )

            print(
                f"[SCHEDULE SUMMARY] test user={u.id} freq={freq} window={start}..{end} events={len(events)}",
                flush=True,
            )

            if not events:
                flash("No scheduled appointments in the summary window.", "info")
                return redirect(url_for("settings"))

            lines = []
            for event in events:
                customer = s.get(Customer, event.customer_id) if event.customer_id else None
                lines.append(_format_event_line(event, customer))

            end_display = end - timedelta(seconds=1)
            subject = f"Upcoming appointments ({freq})"
            body = (
                f"Here is your upcoming appointment summary (local time, {tz_label}):\n"
                f"{start:%b %d, %Y %I:%M %p} through {end_display:%b %d, %Y %I:%M %p}\n\n"
                + "\n".join(lines)
            )

            try:
                _send_schedule_summary_email(to_email, subject, body)
            except Exception as exc:
                print(f"[SCHEDULE SUMMARY] test send failed user={u.id}: {exc!r}", flush=True)
                flash("Could not send summary email. Check server logs for SMTP errors.", "error")
                return redirect(url_for("settings"))

            u.schedule_summary_last_sent = now
            s.commit()
            flash("Test summary email sent.", "success")
            return redirect(url_for("settings"))

    # -----------------------------
    # Billing pages
    # -----------------------------
    @app.route("/billing")
    @login_required
    def billing():
        with db_session() as s:
            u = s.get(User, _current_user_id_int())
            status = (getattr(u, "subscription_status", None) or "none") if u else "none"
        return render_template("billing.html", status=status, publishable_key=STRIPE_PUBLISHABLE_KEY)

    @app.route("/billing/checkout", methods=["POST"])
    @login_required
    def billing_checkout():
        if not stripe.api_key or not STRIPE_PRICE_ID:
            abort(500)

        base = _base_url()
        uid = _current_user_id_int()

        with db_session() as s:
            u = s.get(User, uid)
            if not u:
                _audit_log(s, event="billing.checkout", result="blocked", user_id=uid, details="user_not_found")
                s.commit()
                abort(403)

            status = (getattr(u, "subscription_status", None) or "").lower().strip()

            if status in ("trialing", "active", "past_due"):
                _audit_log(
                    s,
                    event="billing.checkout",
                    result="blocked",
                    user_id=u.id,
                    username=u.username,
                    email=u.email,
                    details=f"already_{status}",
                )
                s.commit()
                flash("You already have an active subscription. Manage billing below.", "info")
                return redirect(url_for("billing"))

            cust = (getattr(u, "stripe_customer_id", None) or "").strip()
            if cust:
                try:
                    stripe.Customer.retrieve(cust)
                except Exception as e:
                    if "No such customer" in str(e or ""):
                        cust = ""
                        u.stripe_customer_id = None
                        u.stripe_subscription_id = None
                        s.commit()
                    else:
                        _audit_log(
                            s,
                            event="billing.checkout",
                            result="fail",
                            user_id=u.id,
                            username=u.username,
                            email=u.email,
                            details=f"stripe_customer_retrieve_failed:{type(e).__name__}",
                        )
                        s.commit()
                        flash(f"Stripe error: {_stripe_err_msg(e)}", "error")
                        return redirect(url_for("billing"))

            if not cust:
                try:
                    customer = stripe.Customer.create(
                        email=(u.email or None),
                        metadata={"app_user_id": str(uid)},
                    )
                    cust = customer["id"]
                    u.stripe_customer_id = cust
                    s.commit()
                except Exception as e:
                    _audit_log(
                        s,
                        event="billing.checkout",
                        result="fail",
                        user_id=u.id,
                        username=u.username,
                        email=u.email,
                        details=f"stripe_customer_create_failed:{type(e).__name__}",
                    )
                    s.commit()
                    flash(f"Stripe error: {_stripe_err_msg(e)}", "error")
                    return redirect(url_for("billing"))

            subscription_data = {}
            if getattr(u, "trial_used_at", None) is None:
                subscription_data["trial_period_days"] = 7

            try:
                cs = stripe.checkout.Session.create(
                    mode="subscription",
                    customer=cust,
                    line_items=[{"price": STRIPE_PRICE_ID, "quantity": 1}],
                    client_reference_id=str(uid),
                    subscription_data=subscription_data,
                    success_url=f"{base}{url_for('billing_success')}?session_id={{CHECKOUT_SESSION_ID}}",
                    cancel_url=f"{base}{url_for('billing')}",
                )
            except Exception as e:
                _audit_log(
                    s,
                    event="billing.checkout",
                    result="fail",
                    user_id=u.id,
                    username=u.username,
                    email=u.email,
                    details=f"stripe_checkout_failed:{type(e).__name__}",
                )
                s.commit()
                flash(f"Stripe error: {_stripe_err_msg(e)}", "error")
                return redirect(url_for("billing"))
            _audit_log(
                s,
                event="billing.checkout",
                result="success",
                user_id=u.id,
                username=u.username,
                email=u.email,
                details=f"stripe_checkout_session={cs.get('id', '')}",
            )
            s.commit()

        return redirect(cs.url, code=303)

    @app.route("/billing/success")
    @login_required
    def billing_success():
        return render_template("billing_success.html")

    @app.route("/billing/portal", methods=["POST"])
    @login_required
    def billing_portal():
        if not stripe.api_key:
            abort(500)

        base = _base_url()
        with db_session() as s:
            u = s.get(User, _current_user_id_int())
            cust = (getattr(u, "stripe_customer_id", None) or "").strip() if u else ""
            if not cust:
                _audit_log(
                    s,
                    event="billing.portal",
                    result="blocked",
                    user_id=(u.id if u else None),
                    username=(u.username if u else None),
                    email=(u.email if u else None),
                    details="missing_stripe_customer_id",
                )
                s.commit()
                flash("No billing profile yet. Start your trial first.", "error")
                return redirect(url_for("billing"))

        try:
            ps = stripe.billing_portal.Session.create(
                customer=cust,
                return_url=f"{base}{url_for('billing')}",
            )
        except Exception as e:
            with db_session() as s:
                u = s.get(User, _current_user_id_int())
                if u and "No such customer" in str(e or ""):
                    u.stripe_customer_id = None
                    u.stripe_subscription_id = None
                _audit_log(
                    s,
                    event="billing.portal",
                    result="fail",
                    user_id=(u.id if u else None),
                    username=(u.username if u else None),
                    email=(u.email if u else None),
                    details=f"stripe_portal_failed:{type(e).__name__}",
                )
                s.commit()
            flash(f"Stripe error: {_stripe_err_msg(e)}", "error")
            return redirect(url_for("billing"))
        with db_session() as s:
            u = s.get(User, _current_user_id_int())
            _audit_log(
                s,
                event="billing.portal",
                result="success",
                user_id=(u.id if u else None),
                username=(u.username if u else None),
                email=(u.email if u else None),
                details=f"stripe_portal_session={ps.get('id', '')}",
            )
            s.commit()
        return redirect(ps.url, code=303)

    # -----------------------------
    # Stripe Webhook
    # -----------------------------
    @app.route("/stripe/webhook", methods=["POST"])
    def stripe_webhook():
        webhook_secret = app.config.get("STRIPE_WEBHOOK_SECRET") or os.getenv("STRIPE_WEBHOOK_SECRET")
        if not webhook_secret:
            abort(500)

        payload = request.get_data(as_text=False)
        sig_header = request.headers.get("Stripe-Signature", "")

        try:
            event = stripe.Webhook.construct_event(
                payload=payload,
                sig_header=sig_header,
                secret=webhook_secret,
            )
        except Exception as e:
            print(f"[STRIPE] Webhook signature verification failed: {repr(e)}", flush=True)
            with db_session() as s:
                _audit_log(
                    s,
                    event="billing.webhook",
                    result="fail",
                    details=f"signature_verification_failed:{type(e).__name__}",
                )
                s.commit()
            return ("bad signature", 400)

        etype = event["type"]
        obj = event["data"]["object"]

        with db_session() as s:
            _audit_log(
                s,
                event="billing.webhook",
                result="success",
                details=f"type={etype}",
            )
            if etype == "checkout.session.completed":
                uid = int(obj.get("client_reference_id") or 0)
                customer_id = obj.get("customer")
                subscription_id = obj.get("subscription")

                u = s.get(User, uid) if uid else None
                if u:
                    if customer_id:
                        u.stripe_customer_id = customer_id
                    if subscription_id:
                        u.stripe_subscription_id = subscription_id

                    try:
                        if subscription_id:
                            sub = stripe.Subscription.retrieve(subscription_id)
                            u.subscription_status = (sub.get("status") or "").lower() or None

                            trial_end = sub.get("trial_end")
                            cpe = sub.get("current_period_end")
                            u.trial_ends_at = datetime.utcfromtimestamp(trial_end) if trial_end else None
                            u.current_period_end = datetime.utcfromtimestamp(cpe) if cpe else None

                            if (u.subscription_status == "trialing") and (getattr(u, "trial_used_at", None) is None):
                                u.trial_used_at = datetime.utcnow()
                    except Exception as e:
                        print("[STRIPE] retrieve subscription failed:", repr(e), flush=True)

                    s.commit()

            elif etype in (
                "customer.subscription.created",
                "customer.subscription.updated",
                "customer.subscription.deleted",
            ):
                sub_id = obj.get("id")
                customer_id = obj.get("customer")

                status = (obj.get("status") or "").lower()
                if etype == "customer.subscription.deleted":
                    status = "canceled"

                u = None
                if sub_id:
                    u = s.query(User).filter(User.stripe_subscription_id == sub_id).first()

                if not u and customer_id:
                    u = s.query(User).filter(User.stripe_customer_id == customer_id).first()
                    if u and not (getattr(u, "stripe_subscription_id", None) or "").strip():
                        u.stripe_subscription_id = sub_id

                if u:
                    u.subscription_status = status

                    trial_end = obj.get("trial_end")
                    cpe = obj.get("current_period_end")
                    u.trial_ends_at = datetime.utcfromtimestamp(trial_end) if trial_end else None
                    u.current_period_end = datetime.utcfromtimestamp(cpe) if cpe else None

                    if status == "trialing" and getattr(u, "trial_used_at", None) is None:
                        u.trial_used_at = datetime.utcnow()

                    s.commit()

            elif etype == "invoice.payment_failed":
                cust = obj.get("customer")
                u = s.query(User).filter(User.stripe_customer_id == cust).first()
                if u:
                    u.subscription_status = "past_due"
                    s.commit()

            s.commit()

        return ("ok", 200)

    # -----------------------------
    # Index
    # -----------------------------
    @app.route("/")
    def index():
        if current_user.is_authenticated:
            return redirect(url_for("customers_list"))
        return render_template("landing.html", title="InvoiceRunner")

    @app.route("/privacy")
    def privacy():
        return render_template("privacy.html", title="Privacy Policy", last_updated=datetime.utcnow().strftime("%B %d, %Y"))

    @app.route("/terms")
    def terms():
        return render_template("terms.html", title="Terms and Conditions", last_updated=datetime.utcnow().strftime("%B %d, %Y"))

    # -----------------------------
    # Scheduler
    # -----------------------------
    @app.route("/schedule")
    @login_required
    @subscription_required
    def schedule():
        # optional deep-link for preselecting a customer on the front-end (JS can read URL param)
        return render_template("schedule.html", title="Scheduler")

    @app.get("/api/customers/search")
    @login_required
    @subscription_required
    def api_customers_search():
        uid = _current_user_id_int()
        q = (request.args.get("q") or "").strip()
        if not q:
            return jsonify([])

        with db_session() as s:
            like = f"%{q}%"
            rows = (
                s.query(Customer)
                .filter(Customer.user_id == uid)
                .filter(Customer.name.ilike(like))
                .order_by(Customer.name.asc())
                .limit(20)
                .all()
            )
            return jsonify([
                {"id": c.id, "name": c.name, "phone": c.phone or "", "email": c.email or "", "address": c.address or ""}
                for c in rows
            ])

    @app.get("/api/customers/<int:customer_id>/documents")
    @login_required
    @subscription_required
    def api_customer_documents(customer_id: int):
        uid = _current_user_id_int()
        with db_session() as s:
            customer = _customer_owned_or_404(s, customer_id)
            invoices = (
                s.query(Invoice)
                .filter(Invoice.user_id == uid)
                .filter(Invoice.customer_id == customer.id)
                .order_by(Invoice.created_at.desc())
                .all()
            )
            return jsonify([
                {
                    "id": inv.id,
                    "invoice_number": (inv.display_number or inv.invoice_number),
                    "is_estimate": bool(inv.is_estimate),
                    "date_in": inv.date_in,
                    "vehicle": inv.vehicle,
                }
                for inv in invoices
            ])

    @app.get("/api/schedule/events")
    @login_required
    @subscription_required
    def api_schedule_events():
        uid = _current_user_id_int()
        start = (request.args.get("start") or "").strip()  # YYYY-MM-DD
        end = (request.args.get("end") or "").strip()      # YYYY-MM-DD

        if not start or not end:
            return jsonify({"error": "start and end required"}), 400

        # If you pass YYYY-MM-DD only, treat as date boundaries
        if len(start) == 10:
            start_dt = datetime.fromisoformat(start + "T00:00:00")
        else:
            try:
                start_dt = datetime.fromisoformat(start)
            except Exception:
                return jsonify({"error": "Invalid date format. Use YYYY-MM-DD (or ISO)."}), 400

        if len(end) == 10:
            end_dt = datetime.fromisoformat(end + "T00:00:00")
        else:
            try:
                end_dt = datetime.fromisoformat(end)
            except Exception:
                return jsonify({"error": "Invalid date format. Use YYYY-MM-DD (or ISO)."}), 400

        with db_session() as s:
            # auto-generate recurring events up to 90 days out
            try:
                customers = (
                    s.query(Customer)
                    .filter(Customer.user_id == uid)
                    .filter(Customer.next_service_dt.isnot(None))
                    .filter(Customer.service_interval_days.isnot(None))
                    .all()
                )
                any_created = 0
                for cust in customers:
                    any_created += _ensure_recurring_events(s, cust, horizon_days=90)
                if any_created:
                    s.commit()
            except Exception as e:
                # don't break calendar if recurring generation fails
                print("[SCHEDULE] recurring generation error:", repr(e), flush=True)
                s.rollback()

            evs = (
                s.query(ScheduleEvent)
                .filter(ScheduleEvent.user_id == uid)
                .filter(ScheduleEvent.start_dt < end_dt)
                .filter(ScheduleEvent.end_dt > start_dt)
                .order_by(ScheduleEvent.start_dt.asc())
                .all()
            )

            out = []
            for e in evs:
                cust = s.get(Customer, e.customer_id) if getattr(e, "customer_id", None) else None
                inv = None
                if getattr(e, "invoice_id", None):
                    inv = s.get(Invoice, e.invoice_id)
                    if inv and inv.user_id != uid:
                        inv = None
                event_type = (getattr(e, "event_type", None) or "appointment")
                if event_type == "block":
                    title = (e.title or "").strip() or "Blocked time"
                    customer_name = ""
                else:
                    title = (e.title or "").strip() or (cust.name if cust else "Appointment")
                    customer_name = (cust.name if cust else "")
                out.append({
                    "id": e.id,
                    "customer_id": e.customer_id,
                    "customer_name": customer_name,
                    "invoice_id": inv.id if inv else None,
                    "invoice_number": (inv.display_number or inv.invoice_number) if inv else "",
                    "invoice_is_estimate": bool(inv.is_estimate) if inv else False,
                    "invoice_url": (
                        url_for("estimate_view", estimate_id=inv.id)
                        if (inv and inv.is_estimate)
                        else (url_for("invoice_view", invoice_id=inv.id) if inv else "")
                    ),
                    "customer_recurring": {
                        "next_service_dt": cust.next_service_dt.isoformat(timespec="minutes")
                        if (cust and cust.next_service_dt) else None,
                        "interval_days": (cust.service_interval_days if cust else None),
                        "default_minutes": (cust.default_service_minutes if cust else None),
                        "title": (cust.service_title or "").strip() if cust else "",
                        "notes": (cust.service_notes or "").strip() if cust else "",
                    } if cust else None,
                    "title": title,
                    "start": e.start_dt.isoformat(timespec="minutes"),
                    "end": e.end_dt.isoformat(timespec="minutes"),
                    "notes": (e.notes or "").strip(),
                    "status": (getattr(e, "status", None) or "scheduled"),
                    "event_type": event_type,
                })

            return jsonify(out)

    @app.post("/api/schedule/events")
    @login_required
    @subscription_required
    def api_schedule_create():
        uid = _current_user_id_int()
        data = request.get_json(silent=True) or {}

        try:
            start_dt = _parse_iso_dt(data.get("start") or "")
            end_dt = _parse_iso_dt(data.get("end") or "")
        except Exception:
            return jsonify({"error": "Invalid start/end datetime"}), 400

        if end_dt <= start_dt:
            return jsonify({"error": "End must be after start"}), 400

        customer_id = data.get("customer_id")
        title = (data.get("title") or "").strip()
        notes = (data.get("notes") or "").strip()
        recurring_enabled = bool(data.get("recurring_enabled"))
        recurring_interval_days = (data.get("recurring_interval_days") or "").strip()
        recurring_duration_minutes = (data.get("recurring_duration_minutes") or "").strip()
        recurring_horizon_months = (data.get("recurring_horizon_months") or "").strip()
        recurring_title = (data.get("recurring_title") or "").strip()
        recurring_notes = (data.get("recurring_notes") or "").strip()
        event_type = (data.get("event_type") or "appointment").strip().lower()
        invoice_id_raw = (data.get("invoice_id") or "").strip()

        if event_type not in ("appointment", "block"):
            return jsonify({"error": "Invalid event type"}), 400

        with db_session() as s:
            cust_id_int = None
            invoice_id_int = None
            if event_type == "block":
                cust_id_int = None
            elif customer_id is not None and str(customer_id).strip() != "":
                try:
                    cust_id_int = int(customer_id)
                except Exception:
                    return jsonify({"error": "customer_id must be an integer"}), 400
                _customer_owned_or_404(s, cust_id_int)

            if event_type == "block":
                invoice_id_int = None
            elif invoice_id_raw:
                try:
                    invoice_id_int = int(invoice_id_raw)
                except Exception:
                    return jsonify({"error": "invoice_id must be an integer"}), 400
                if not cust_id_int:
                    return jsonify({"error": "Select a customer before choosing an invoice."}), 400
                inv = _invoice_owned_or_404(s, invoice_id_int)
                if cust_id_int and inv.customer_id != cust_id_int:
                    return jsonify({"error": "Invoice must belong to the selected customer."}), 400

            ev = ScheduleEvent(
                user_id=uid,
                customer_id=cust_id_int,
                invoice_id=invoice_id_int,
                title=(title or None) if event_type != "block" else (title or "Blocked time"),
                start_dt=start_dt,
                end_dt=end_dt,
                notes=notes or None,
                event_type=event_type,
                # is_auto defaults False for manual events
            )
            s.add(ev)

            if recurring_enabled:
                if event_type == "block":
                    return jsonify({"error": "Recurring schedule is not available for blocked time."}), 400
                if not cust_id_int:
                    return jsonify({"error": "Recurring schedule requires a customer."}), 400
                if not recurring_interval_days.isdigit():
                    return jsonify({"error": "Recurring interval (days) is required."}), 400

                interval_days = int(recurring_interval_days)
                if interval_days < 1:
                    return jsonify({"error": "Recurring interval must be at least 1 day."}), 400

                duration_minutes = int(recurring_duration_minutes) if recurring_duration_minutes.isdigit() else None
                if not duration_minutes or duration_minutes < 15:
                    duration_minutes = max(15, int((end_dt - start_dt).total_seconds() / 60))

                horizon_months = int(recurring_horizon_months) if recurring_horizon_months.isdigit() else 1
                horizon_months = max(1, horizon_months)
                horizon_days = horizon_months * 30

                cust = _customer_owned_or_404(s, cust_id_int)
                _delete_future_recurring_events(s, cust, from_dt=datetime.utcnow())

                cust.next_service_dt = start_dt + timedelta(days=interval_days)
                cust.service_interval_days = interval_days
                cust.default_service_minutes = duration_minutes
                cust.service_title = recurring_title or (title or cust.name)
                cust.service_notes = recurring_notes or (notes or None)
                cust.recurring_horizon_dt = start_dt + timedelta(days=horizon_days)

                _ensure_recurring_events(s, cust, horizon_days=horizon_days)

            s.commit()

            return jsonify({"ok": True, "id": ev.id})

    @app.post("/api/receipts/scan")
    @login_required
    @subscription_required
    def api_receipts_scan():
        """
        Accepts a receipt image and returns parsed line items.
        """
        img_file = request.files.get("image")
        if not img_file or not getattr(img_file, "filename", ""):
            return jsonify({"error": "Image is required."}), 400

        try:
            raw = img_file.read()
            img_file.stream.seek(0)
            if not raw:
                return jsonify({"error": "Empty image file."}), 400
            ImageFile.LOAD_TRUNCATED_IMAGES = True
            img = Image.open(io.BytesIO(raw))
            img.load()
        except UnidentifiedImageError:
            return jsonify({
                "error": "Invalid image file. If this is HEIC, please convert to JPG/PNG first."
            }), 400
        except Exception:
            return jsonify({"error": "Invalid image file."}), 400

        try:
            # Preprocess for better receipt OCR
            img = img.convert("L")
            img = img.resize((img.width * 2, img.height * 2))
            # contrast boost
            img = Image.eval(img, lambda x: 0 if x < 40 else 255 if x > 220 else x)
            # adaptive-ish threshold: use a slightly lower cutoff to preserve faint text
            img = img.point(lambda x: 0 if x < 150 else 255, mode="1")

            text_psm4 = pytesseract.image_to_string(img, config="--oem 1 --psm 4")
            text_psm6 = pytesseract.image_to_string(img, config="--oem 1 --psm 6")
            text_psm11 = pytesseract.image_to_string(img, config="--oem 1 --psm 11")
        except Exception:
            return jsonify({"error": "OCR failed. Check Tesseract installation."}), 500

        items4 = _parse_receipt_text_to_items(text_psm4)
        items6 = _parse_receipt_text_to_items(text_psm6)
        items11 = _parse_receipt_text_to_items(text_psm11)

        # Merge passes: keep duplicates based on counts
        def _count_items(items):
            counts = {}
            for it in items:
                key = (it.get("name", "").lower(), it.get("price", ""))
                counts[key] = counts.get(key, 0) + 1
            return counts

        out_items = list(items4)
        count_out = _count_items(items4)
        count6 = _count_items(items6)
        count11 = _count_items(items11)

        for it in items6:
            key = (it.get("name", "").lower(), it.get("price", ""))
            if count6.get(key, 0) > count_out.get(key, 0):
                out_items.append(it)
                count_out[key] = count_out.get(key, 0) + 1

        for it in items11:
            key = (it.get("name", "").lower(), it.get("price", ""))
            if count11.get(key, 0) > count_out.get(key, 0):
                out_items.append(it)
                count_out[key] = count_out.get(key, 0) + 1

        combined_text = (
            "=== OCR (PSM 4) ===\n" + (text_psm4 or "").strip() +
            "\n\n=== OCR (PSM 6) ===\n" + (text_psm6 or "").strip() +
            "\n\n=== OCR (PSM 11) ===\n" + (text_psm11 or "").strip()
        ).strip()

        return jsonify({"items": out_items, "text": combined_text})

    @app.put("/api/schedule/events/<int:event_id>")
    @login_required
    @subscription_required
    def api_schedule_update(event_id: int):
        uid = _current_user_id_int()
        data = request.get_json(silent=True) or {}

        with db_session() as s:
            ev = (
                s.query(ScheduleEvent)
                .filter(ScheduleEvent.id == event_id, ScheduleEvent.user_id == uid)
                .first()
            )
            if not ev:
                abort(404)

            if "event_type" in data:
                event_type = (data.get("event_type") or "appointment").strip().lower()
                if event_type not in ("appointment", "block"):
                    return jsonify({"error": "Invalid event type"}), 400
                ev.event_type = event_type
                if event_type == "block":
                    ev.customer_id = None

            if "start" in data:
                ev.start_dt = _parse_iso_dt(data.get("start") or "")
            if "end" in data:
                ev.end_dt = _parse_iso_dt(data.get("end") or "")

            if getattr(ev, "end_dt", None) and getattr(ev, "start_dt", None) and ev.end_dt <= ev.start_dt:
                return jsonify({"error": "End must be after start"}), 400

            if "customer_id" in data:
                raw = data.get("customer_id")
                if getattr(ev, "event_type", None) == "block":
                    ev.customer_id = None
                elif raw is None or str(raw).strip() == "":
                    ev.customer_id = None
                else:
                    try:
                        cust_id_int = int(raw)
                    except Exception:
                        return jsonify({"error": "customer_id must be an integer"}), 400
                    _customer_owned_or_404(s, cust_id_int)
                    ev.customer_id = cust_id_int
                if not ev.customer_id:
                    ev.invoice_id = None

            if "invoice_id" in data:
                invoice_raw = (data.get("invoice_id") or "").strip()
                if getattr(ev, "event_type", None) == "block":
                    ev.invoice_id = None
                elif not invoice_raw:
                    ev.invoice_id = None
                else:
                    try:
                        invoice_id_int = int(invoice_raw)
                    except Exception:
                        return jsonify({"error": "invoice_id must be an integer"}), 400
                    if not ev.customer_id:
                        return jsonify({"error": "Select a customer before choosing an invoice."}), 400
                    inv = _invoice_owned_or_404(s, invoice_id_int)
                    if ev.customer_id and inv.customer_id != ev.customer_id:
                        return jsonify({"error": "Invoice must belong to the selected customer."}), 400
                    ev.invoice_id = invoice_id_int

            if "title" in data:
                t = (data.get("title") or "").strip()
                if getattr(ev, "event_type", None) == "block":
                    ev.title = t or "Blocked time"
                else:
                    ev.title = t or None

            if "notes" in data:
                n = (data.get("notes") or "").strip()
                ev.notes = n or None

            if "status" in data:
                if getattr(ev, "event_type", None) == "block":
                    return jsonify({"error": "Blocked time cannot change status."}), 400
                st = (data.get("status") or "").strip().lower()
                if st in ("scheduled", "completed", "cancelled"):
                    ev.status = st

            if data.get("recurring_enabled"):
                if getattr(ev, "event_type", None) == "block":
                    return jsonify({"error": "Recurring schedule is not available for blocked time."}), 400
                if not ev.customer_id:
                    return jsonify({"error": "Recurring schedule requires a customer."}), 400

                interval_days_raw = (data.get("recurring_interval_days") or "").strip()
                if not interval_days_raw.isdigit():
                    return jsonify({"error": "Recurring interval (days) is required."}), 400

                interval_days = int(interval_days_raw)
                if interval_days < 1:
                    return jsonify({"error": "Recurring interval must be at least 1 day."}), 400

                duration_raw = (data.get("recurring_duration_minutes") or "").strip()
                duration_minutes = int(duration_raw) if duration_raw.isdigit() else None
                if not duration_minutes or duration_minutes < 15:
                    duration_minutes = max(15, int((ev.end_dt - ev.start_dt).total_seconds() / 60))

                horizon_raw = (data.get("recurring_horizon_months") or "").strip()
                horizon_months = int(horizon_raw) if horizon_raw.isdigit() else 1
                horizon_months = max(1, horizon_months)
                horizon_days = horizon_months * 30

                recurring_title = (data.get("recurring_title") or "").strip()
                recurring_notes = (data.get("recurring_notes") or "").strip()

                cust = _customer_owned_or_404(s, ev.customer_id)
                _delete_future_recurring_events(s, cust, from_dt=datetime.utcnow())

                cust.next_service_dt = ev.start_dt + timedelta(days=interval_days)
                cust.service_interval_days = interval_days
                cust.default_service_minutes = duration_minutes
                cust.service_title = recurring_title or (ev.title or cust.name)
                cust.service_notes = recurring_notes or (ev.notes or None)
                cust.recurring_horizon_dt = ev.start_dt + timedelta(days=horizon_days)

                _ensure_recurring_events(s, cust, horizon_days=horizon_days)

            s.commit()
            return jsonify({"ok": True})

    @app.delete("/api/schedule/events/<int:event_id>")
    @login_required
    @subscription_required
    def api_schedule_delete(event_id: int):
        uid = _current_user_id_int()
        with db_session() as s:
            ev = (
                s.query(ScheduleEvent)
                .filter(ScheduleEvent.id == event_id, ScheduleEvent.user_id == uid)
                .first()
            )
            if not ev:
                abort(404)
            s.delete(ev)
            s.commit()
            return jsonify({"ok": True})

    # -----------------------------
    # Customers
    # -----------------------------
    @app.route("/customers")
    @login_required
    @subscription_required
    def customers_list():
        uid = _current_user_id_int()
        q = (request.args.get("q") or "").strip()

        with db_session() as s:
            cq = s.query(Customer).filter(Customer.user_id == uid)
            if q:
                like = f"%{q}%"
                cq = cq.filter(
                    Customer.name.ilike(like) |
                    Customer.email.ilike(like) |
                    Customer.phone.ilike(like)
                )
            customers = cq.order_by(Customer.name.asc()).all()

        return render_template("customers_list.html", customers=customers, q=q)

    @app.route("/customers/new", methods=["GET", "POST"])
    @login_required
    @subscription_required
    def customer_new():
        if request.method == "POST":
            name = (request.form.get("name") or "").strip()
            email = _normalize_email(request.form.get("email") or "").strip() or None
            phone = (request.form.get("phone") or "").strip() or None
            address_line1 = (request.form.get("address_line1") or "").strip() or None
            address_line2 = (request.form.get("address_line2") or "").strip() or None
            city = (request.form.get("city") or "").strip() or None
            state = (request.form.get("state") or "").strip() or None
            postal_code = (request.form.get("postal_code") or "").strip() or None
            address = _format_customer_address(address_line1, address_line2, city, state, postal_code)

            # Recurring fields (safe if form doesn't have them yet)
            next_service_dt = _parse_dt_local(request.form.get("next_service_dt"))
            interval_days_raw = (request.form.get("service_interval_days") or "").strip()
            default_minutes_raw = (request.form.get("default_service_minutes") or "").strip()
            service_title = (request.form.get("service_title") or "").strip() or None
            service_notes = (request.form.get("service_notes") or "").strip() or None

            service_interval_days = int(interval_days_raw) if interval_days_raw.isdigit() else None
            default_service_minutes = int(default_minutes_raw) if default_minutes_raw.isdigit() else 60

            if not name:
                flash("Customer name is required.", "error")
                return render_template("customer_form.html", mode="new", form=request.form)

            with db_session() as s:
                existing = (
                    s.query(Customer)
                    .filter(Customer.user_id == _current_user_id_int())
                    .filter(text("lower(name) = :n")).params(n=name.lower())
                    .first()
                )
                if existing:
                    flash("That customer already exists.", "info")
                    return redirect(url_for("customer_view", customer_id=existing.id))

                c = Customer(
                    user_id=_current_user_id_int(),
                    name=name,
                    email=(email if (email and _looks_like_email(email)) else (email or None)),
                    phone=phone,
                    address=address,
                    address_line1=address_line1,
                    address_line2=address_line2,
                    city=city,
                    state=state,
                    postal_code=postal_code,

                    next_service_dt=next_service_dt,
                    service_interval_days=service_interval_days,
                    default_service_minutes=default_service_minutes,
                    service_title=service_title,
                    service_notes=service_notes,
                )
                s.add(c)
                try:
                    s.commit()
                except IntegrityError:
                    s.rollback()
                    flash("That customer name is already in use.", "error")
                    return render_template("customer_form.html", mode="new", form=request.form)

                # If recurrence is enabled, generate initial future events
                try:
                    created = _ensure_recurring_events(s, c, horizon_days=90)
                    if created:
                        s.commit()
                except Exception as e:
                    s.rollback()
                    print("[SCHEDULE] initial recurrence gen error:", repr(e), flush=True)

                return redirect(url_for("customer_view", customer_id=c.id))

        return render_template("customer_form.html", mode="new")

    @app.route("/customers/<int:customer_id>/edit", methods=["GET", "POST"])
    @login_required
    @subscription_required
    def customer_edit(customer_id: int):
        with db_session() as s:
            c = _customer_owned_or_404(s, customer_id)

            # capture old recurring rule so we can detect changes
            old_rule = (
                c.next_service_dt,
                c.service_interval_days,
                c.default_service_minutes,
                (c.service_title or "").strip(),
                (c.service_notes or "").strip(),
            )

            if request.method == "POST":
                name = (request.form.get("name") or "").strip()
                email = _normalize_email(request.form.get("email") or "").strip() or None
                phone = (request.form.get("phone") or "").strip() or None
                address_line1 = (request.form.get("address_line1") or "").strip() or None
                address_line2 = (request.form.get("address_line2") or "").strip() or None
                city = (request.form.get("city") or "").strip() or None
                state = (request.form.get("state") or "").strip() or None
                postal_code = (request.form.get("postal_code") or "").strip() or None
                address = _format_customer_address(address_line1, address_line2, city, state, postal_code)

                if not name:
                    flash("Customer name is required.", "error")
                    return render_template("customer_form.html", mode="edit", c=c)

                dup = (
                    s.query(Customer)
                    .filter(Customer.user_id == _current_user_id_int())
                    .filter(text("lower(name) = :n")).params(n=name.lower())
                    .filter(Customer.id != c.id)
                    .first()
                )
                if dup:
                    try:
                        _merge_customers(s, source=c, target=dup)
                        s.commit()
                        flash(f"Merged into existing customer: {dup.name}", "success")
                        return redirect(url_for("customer_view", customer_id=dup.id))
                    except Exception as e:
                        s.rollback()
                        print("[CUSTOMER MERGE] ERROR:", repr(e), flush=True)
                        flash("Could not merge customers (server error).", "error")
                        return render_template("customer_form.html", mode="edit", c=c)

                c.name = name
                c.email = (email if (email and _looks_like_email(email)) else (email or None))
                c.phone = phone
                c.address = address
                c.address_line1 = address_line1
                c.address_line2 = address_line2
                c.city = city
                c.state = state
                c.postal_code = postal_code

                # Recurring fields
                c.next_service_dt = _parse_dt_local(request.form.get("next_service_dt"))
                interval_days_raw = (request.form.get("service_interval_days") or "").strip()
                default_minutes_raw = (request.form.get("default_service_minutes") or "").strip()
                c.service_interval_days = int(interval_days_raw) if interval_days_raw.isdigit() else None
                c.default_service_minutes = int(default_minutes_raw) if default_minutes_raw.isdigit() else 60
                c.service_title = (request.form.get("service_title") or "").strip() or None
                c.service_notes = (request.form.get("service_notes") or "").strip() or None

                new_rule = (
                    c.next_service_dt,
                    c.service_interval_days,
                    c.default_service_minutes,
                    (c.service_title or "").strip(),
                    (c.service_notes or "").strip(),
                )
                rule_changed = (old_rule != new_rule)

                # If rule changed, delete future old recurring events and regenerate from the new rule
                if rule_changed:
                    try:
                        _delete_future_recurring_events(s, c, from_dt=datetime.utcnow())
                        _ensure_recurring_events(s, c, horizon_days=90)
                    except Exception as e:
                        s.rollback()
                        print("[SCHEDULE] recurring reset error:", repr(e), flush=True)
                        flash("Could not update recurring schedule (server error).", "error")
                        return render_template("customer_form.html", mode="edit", c=c)

                try:
                    s.commit()
                except IntegrityError:
                    s.rollback()
                    flash("That customer name is already in use. Try a different name, or merge customers.", "error")
                    return render_template("customer_form.html", mode="edit", c=c)

                flash("Customer updated.", "success")
                return redirect(url_for("customer_view", customer_id=c.id))

        return render_template("customer_form.html", mode="edit", c=c)

    @app.post("/customers/<int:customer_id>/delete")
    @login_required
    @subscription_required
    def customer_delete(customer_id: int):
        uid = _current_user_id_int()
        pdf_paths = []
        deleted_docs = 0

        with db_session() as s:
            c = _customer_owned_or_404(s, customer_id)

            docs = (
                s.query(Invoice)
                .filter(Invoice.user_id == uid)
                .filter(Invoice.customer_id == c.id)
                .all()
            )
            deleted_docs = len(docs)

            for inv in docs:
                if inv.pdf_path:
                    pdf_paths.append(inv.pdf_path)
                s.delete(inv)

            # Also remove calendar items attached to this customer.
            s.query(ScheduleEvent).filter(
                ScheduleEvent.user_id == uid,
                ScheduleEvent.customer_id == c.id,
            ).delete(synchronize_session=False)

            s.delete(c)
            s.commit()

        for p in pdf_paths:
            try:
                if p and os.path.exists(p):
                    os.remove(p)
            except Exception:
                pass

        flash(
            f"Customer deleted. Removed {deleted_docs} associated invoice/estimate record(s).",
            "success",
        )
        return redirect(url_for("customers_list"))

    # -----------------------------
    # NEW: Discontinue recurring schedule for a customer (button)
    # -----------------------------
    @app.post("/customers/<int:customer_id>/recurring/disable")
    @login_required
    @subscription_required
    def customer_disable_recurring(customer_id: int):
        """
        Disables recurring scheduling for this customer WITHOUT requiring the user
        to manually delete fields. Also deletes all existing auto-generated recurring
        appointments for this customer from the calendar.
        """
        with db_session() as s:
            c = _customer_owned_or_404(s, customer_id)

            # delete all auto events (past + future) tied to this customer's recurring token
            deleted = 0
            try:
                deleted = _delete_all_recurring_events(s, c)
            except Exception as e:
                s.rollback()
                print("[SCHEDULE] disable recurring delete error:", repr(e), flush=True)
                flash("Could not delete recurring appointments (server error).", "error")
                return redirect(url_for("customer_edit", customer_id=customer_id))

            # clear recurring fields (this disables future generation)
            c.next_service_dt = None
            c.service_interval_days = None
            c.service_title = None
            c.service_notes = None
            c.recurring_horizon_dt = None
            # keep default_service_minutes as-is (harmless / preference)

            try:
                s.commit()
            except Exception as e:
                s.rollback()
                print("[SCHEDULE] disable recurring commit error:", repr(e), flush=True)
                flash("Could not disable recurring schedule (server error).", "error")
                return redirect(url_for("customer_edit", customer_id=customer_id))

        flash(f"Recurring schedule disabled. Removed {deleted} recurring appointment(s).", "success")
        return redirect(url_for("customer_edit", customer_id=customer_id))

    @app.route("/customers/<int:customer_id>/merge", methods=["GET", "POST"])
    @login_required
    @subscription_required
    def customer_merge(customer_id: int):
        uid = _current_user_id_int()
        with db_session() as s:
            source = _customer_owned_or_404(s, customer_id)

            customers = (
                s.query(Customer)
                .filter(Customer.user_id == uid)
                .filter(Customer.id != source.id)
                .order_by(Customer.name.asc())
                .all()
            )

            if request.method == "POST":
                target_id_raw = (request.form.get("target_customer_id") or "").strip()
                if not target_id_raw.isdigit():
                    flash("Pick a customer to merge into.", "error")
                    return render_template("customer_merge.html", source=source, customers=customers)

                target = _customer_owned_or_404(s, int(target_id_raw))

                try:
                    _merge_customers(s, source=source, target=target)
                    s.commit()
                    flash(f"Merged '{source.name}' into '{target.name}'.", "success")
                    return redirect(url_for("customer_view", customer_id=target.id))
                except Exception as e:
                    s.rollback()
                    print("[CUSTOMER MERGE] ERROR:", repr(e), flush=True)
                    flash("Merge failed (server error).", "error")
                    return render_template("customer_merge.html", source=source, customers=customers)

        return render_template("customer_merge.html", source=source, customers=customers)

    @app.route("/customers/<int:customer_id>")
    @login_required
    @subscription_required
    def customer_view(customer_id: int):
        uid = _current_user_id_int()
        year = (request.args.get("year") or "").strip()
        status = (request.args.get("status") or "").strip()

        with db_session() as s:
            c = _customer_owned_or_404(s, customer_id)
            now = datetime.utcnow()
            next_event = (
                s.query(ScheduleEvent)
                .filter(ScheduleEvent.user_id == uid)
                .filter(ScheduleEvent.customer_id == c.id)
                .filter(ScheduleEvent.end_dt >= now)
                .filter(ScheduleEvent.status == "scheduled")
                .order_by(ScheduleEvent.start_dt.asc())
                .first()
            )

            inv_q = (
                s.query(Invoice)
                .options(selectinload(Invoice.parts), selectinload(Invoice.labor_items))
                .filter(Invoice.user_id == uid)
                .filter(Invoice.customer_id == c.id)
                .filter(or_(Invoice.is_estimate.is_(False), Invoice.is_estimate.is_(None)))
                .order_by(Invoice.created_at.desc())
            )

            if year.isdigit() and len(year) == 4:
                inv_q = inv_q.filter(Invoice.display_number.startswith(year))

            invoices_list = inv_q.all()

            estimates_q = (
                s.query(Invoice)
                .options(selectinload(Invoice.parts), selectinload(Invoice.labor_items))
                .filter(Invoice.user_id == uid)
                .filter(Invoice.customer_id == c.id)
                .filter(Invoice.is_estimate.is_(True))
                .order_by(Invoice.created_at.desc())
            )

            if year.isdigit() and len(year) == 4:
                estimates_q = estimates_q.filter(Invoice.display_number.startswith(year))

            estimates_list = estimates_q.all()

            if status in ("paid", "unpaid"):
                EPS = 0.01
                filtered = []
                for inv in invoices_list:
                    fully_paid = (inv.paid or 0) + EPS >= inv.invoice_total()
                    if status == "paid" and fully_paid:
                        filtered.append(inv)
                    if status == "unpaid" and not fully_paid:
                        filtered.append(inv)
                invoices_list = filtered

            total_business = 0.0
            total_paid = 0.0
            total_unpaid = 0.0

            for inv in invoices_list:
                try:
                    total = float(inv.invoice_total() or 0.0)
                    paid = float(inv.paid or 0.0)
                    total_business += total
                    total_paid += paid
                    total_unpaid += max(total - paid, 0.0)
                except Exception:
                    pass

        return render_template(
            "customer_view.html",
            c=c,
            invoices=invoices_list,
            estimates=estimates_list,
            year=year,
            status=status or "all",
            total_business=total_business,
            total_paid=total_paid,
            total_unpaid=total_unpaid,
            next_event=next_event,
        )

    # -----------------------------
    # All estimates list
    # -----------------------------
    @app.route("/estimates")
    @login_required
    def estimates():
        q = (request.args.get("q") or "").strip()
        year = (request.args.get("year") or "").strip()

        uid = _current_user_id_int()

        with db_session() as s:
            estimates_q = (
                s.query(Invoice)
                .options(selectinload(Invoice.parts), selectinload(Invoice.labor_items))
                .filter(Invoice.user_id == uid)
                .filter(Invoice.is_estimate.is_(True))
                .order_by(Invoice.created_at.desc())
            )

            if q:
                like = f"%{q}%"
                estimates_q = estimates_q.filter(
                    (Invoice.name.ilike(like)) |
                    (Invoice.vehicle.ilike(like))
                )

            if year.isdigit() and len(year) == 4:
                estimates_q = estimates_q.filter(Invoice.display_number.startswith(year))

            estimates_list = estimates_q.all()

            customers = s.query(Customer.id, Customer.name).filter(Customer.user_id == uid).all()
            customer_map = {cid: (name or "").strip() for cid, name in customers}

        return render_template(
            "estimates_list.html",
            estimates=estimates_list,
            customer_map=customer_map,
            q=q,
            year=year,
        )

    # -----------------------------
    # All invoices list (optional / legacy)
    # -----------------------------
    @app.route("/invoices")
    @login_required
    def invoices():
        q = (request.args.get("q") or "").strip()
        year = (request.args.get("year") or "").strip()
        status = (request.args.get("status") or "").strip()

        uid = _current_user_id_int()

        with db_session() as s:
            invoices_q = (
                s.query(Invoice)
                .options(selectinload(Invoice.parts), selectinload(Invoice.labor_items))
                .filter(Invoice.user_id == uid)
                .filter(or_(Invoice.is_estimate.is_(False), Invoice.is_estimate.is_(None)))
                .order_by(Invoice.created_at.desc())
            )

            if q:
                like = f"%{q}%"
                invoices_q = invoices_q.filter(
                    (Invoice.name.ilike(like)) |
                    (Invoice.vehicle.ilike(like))
                )

            if year.isdigit() and len(year) == 4:
                invoices_q = invoices_q.filter(Invoice.display_number.startswith(year))

            invoices_list = invoices_q.all()

            customers = s.query(Customer.id, Customer.name).filter(Customer.user_id == uid).all()
            customer_map = {cid: (name or "").strip() for cid, name in customers}

            if status in ("paid", "unpaid"):
                filtered = []
                for inv in invoices_list:
                    fully_paid = (inv.paid or 0) + 0.01 >= inv.invoice_total()
                    if status == "paid" and fully_paid:
                        filtered.append(inv)
                    if status == "unpaid" and not fully_paid:
                        filtered.append(inv)
                invoices_list = filtered

        return render_template(
            "invoices_list.html",
            invoices=invoices_list,
            customer_map=customer_map,
            q=q,
            year=year,
            status=status or "all"
        )

    # -----------------------------
    # Create estimate — GATED
    # -----------------------------
    @app.route("/estimates/new", methods=["GET", "POST"])
    @login_required
    @subscription_required
    def estimate_new():
        uid = _current_user_id_int()
        pre_customer_id = (request.args.get("customer_id") or "").strip()
        default_local_now = datetime.utcnow()

        with db_session() as s:
            u = s.get(User, uid)
            default_local_now = _user_local_now(u)
            user_template_key = _template_key_fallback(getattr(u, "invoice_template", None) if u else None)
            user_pdf_template = _pdf_template_key_fallback(getattr(u, "pdf_template", None) if u else None)
            user_tax_rate = float(getattr(u, "tax_rate", 0.0) or 0.0) if u else 0.0
            user_default_hourly_rate = float(getattr(u, "default_hourly_rate", 0.0) or 0.0) if u else 0.0
            user_default_parts_markup = float(getattr(u, "default_parts_markup", 0.0) or 0.0) if u else 0.0
            tmpl = _template_config_for(user_template_key)

            customers = (
                s.query(Customer)
                .filter(Customer.user_id == uid)
                .order_by(Customer.name.asc())
                .all()
            )

            customers_for_js = [{
                "id": c.id,
                "name": (c.name or "").strip(),
                "email": (c.email or "").strip(),
                "phone": (c.phone or "").strip(),
            } for c in customers]

            pre_customer = None
            if pre_customer_id.isdigit():
                try:
                    pre_customer = _customer_owned_or_404(s, int(pre_customer_id))
                except Exception:
                    pre_customer = None

        if request.method == "POST":
            customer_id_raw = (request.form.get("customer_id") or "").strip()
            vehicle = (request.form.get("vehicle") or "").strip()

            if not customer_id_raw.isdigit():
                flash("Please select a customer from the list.", "error")
                return render_template(
                    "invoice_form.html",
                    mode="new",
                    doc_type="estimate",
                    form=request.form,
                    tmpl=tmpl,
                    tmpl_key=user_template_key,
                    user_tax_rate=user_tax_rate,
                    user_default_hourly_rate=user_default_hourly_rate,
                    user_default_parts_markup=user_default_parts_markup,
                    customers=customers,
                    customers_for_js=customers_for_js,
                    pre_customer=pre_customer,
                )

            customer_id = int(customer_id_raw)

            if user_template_key in ("auto_repair", "lawn_care") and not vehicle:
                flash("Vehicle is required for Auto Repair estimates.", "error")
                return render_template(
                    "invoice_form.html",
                    mode="new",
                    doc_type="estimate",
                    form=request.form,
                    tmpl=tmpl,
                    tmpl_key=user_template_key,
                    user_tax_rate=user_tax_rate,
                    user_default_hourly_rate=user_default_hourly_rate,
                    user_default_parts_markup=user_default_parts_markup,
                    customers=customers,
                    customers_for_js=customers_for_js,
                    pre_customer=pre_customer,
                )

            with db_session() as s:
                c = _customer_owned_or_404(s, customer_id)
                local_now = _user_local_now(s.get(User, uid))

                year = int(local_now.strftime("%Y"))
                inv_no = next_invoice_number(s, year, Config.INVOICE_SEQ_WIDTH)
                display_no = next_display_number(s, uid, year, "estimate", Config.INVOICE_SEQ_WIDTH)

                cust_email_override = (request.form.get("customer_email") or "").strip() or None
                cust_phone_override = (request.form.get("customer_phone") or "").strip() or None

                parts_data = _parse_repeating_fields(
                    request.form.getlist("part_name"),
                    request.form.getlist("part_price")
                )
                labor_data = _parse_repeating_fields(
                    request.form.getlist("labor_desc"),
                    request.form.getlist("labor_time_hours")
                )

                price_per_hour = _to_float(request.form.get("price_per_hour"), user_default_hourly_rate)
                hours = _to_float(request.form.get("hours"))
                shop_supplies = _to_float(request.form.get("shop_supplies"))
                parts_markup_percent = _to_float(request.form.get("parts_markup_percent"), user_default_parts_markup)
                tax_rate = _to_float(request.form.get("tax_rate"), user_tax_rate)
                tax_override_raw = (request.form.get("tax_override") or "").strip()
                tax_override = _to_float(tax_override_raw, 0.0) if tax_override_raw else None
                if user_template_key == "flipping_items":
                    price_per_hour = 1.0
                    hours = 0.0
                else:
                    hours = sum(t for _, t in labor_data)

                inv = Invoice(
                    user_id=uid,
                    customer_id=c.id,

                    invoice_number=inv_no,
                    display_number=display_no,
                    invoice_template=user_template_key,
                    pdf_template=user_pdf_template,
                    is_estimate=True,

                    customer_email=(cust_email_override or (c.email or None)),
                    customer_phone=(cust_phone_override or (c.phone or None)),

                    name=(c.name or "").strip(),
                    vehicle=vehicle,

                    hours=hours,
                    price_per_hour=price_per_hour,
                    shop_supplies=shop_supplies,
                    parts_markup_percent=parts_markup_percent,
                    tax_rate=tax_rate,
                    tax_override=tax_override,
                    paid=0.0,
                    date_in=(request.form.get("date_in", "").strip() or local_now.strftime("%B %d, %Y")),
                    notes=request.form.get("notes", "").rstrip(),
                    useful_info=(request.form.get("useful_info") or "").rstrip() or None,
                )

                for pn, pp in parts_data:
                    inv.parts.append(InvoicePart(part_name=pn, part_price=pp))

                if user_template_key != "flipping_items":
                    for desc, t in labor_data:
                        inv.labor_items.append(InvoiceLabor(labor_desc=desc, labor_time_hours=t))

                s.add(inv)
                s.flush()
                try:
                    generate_and_store_pdf(s, inv.id)
                except Exception as exc:
                    flash(f"Estimate saved, but PDF generation failed: {exc}", "warning")
                s.commit()

                return redirect(url_for("estimate_view", estimate_id=inv.id))

        return render_template(
            "invoice_form.html",
            mode="new",
            doc_type="estimate",
            default_date=default_local_now.strftime("%B %d, %Y"),
            tmpl=tmpl,
            tmpl_key=user_template_key,
            user_tax_rate=user_tax_rate,
            user_default_hourly_rate=user_default_hourly_rate,
            user_default_parts_markup=user_default_parts_markup,
            customers=customers,
            customers_for_js=customers_for_js,
            pre_customer=pre_customer,
        )

    # -----------------------------
    # Create invoice — GATED
    # -----------------------------
    @app.route("/invoices/new", methods=["GET", "POST"])
    @login_required
    @subscription_required
    def invoice_new():
        uid = _current_user_id_int()
        pre_customer_id = (request.args.get("customer_id") or "").strip()
        default_local_now = datetime.utcnow()

        with db_session() as s:
            u = s.get(User, uid)
            default_local_now = _user_local_now(u)
            user_template_key = _template_key_fallback(getattr(u, "invoice_template", None) if u else None)
            user_pdf_template = _pdf_template_key_fallback(getattr(u, "pdf_template", None) if u else None)
            user_tax_rate = float(getattr(u, "tax_rate", 0.0) or 0.0) if u else 0.0
            user_default_hourly_rate = float(getattr(u, "default_hourly_rate", 0.0) or 0.0) if u else 0.0
            user_default_parts_markup = float(getattr(u, "default_parts_markup", 0.0) or 0.0) if u else 0.0
            tmpl = _template_config_for(user_template_key)

            customers = (
                s.query(Customer)
                .filter(Customer.user_id == uid)
                .order_by(Customer.name.asc())
                .all()
            )

            customers_for_js = [{
                "id": c.id,
                "name": (c.name or "").strip(),
                "email": (c.email or "").strip(),
                "phone": (c.phone or "").strip(),
            } for c in customers]

            pre_customer = None
            if pre_customer_id.isdigit():
                try:
                    pre_customer = _customer_owned_or_404(s, int(pre_customer_id))
                except Exception:
                    pre_customer = None

        if request.method == "POST":
            customer_id_raw = (request.form.get("customer_id") or "").strip()
            vehicle = (request.form.get("vehicle") or "").strip()

            if not customer_id_raw.isdigit():
                flash("Please select a customer from the list.", "error")
                return render_template(
                    "invoice_form.html",
                    mode="new",
                    doc_type="invoice",
                    form=request.form,
                    tmpl=tmpl,
                    tmpl_key=user_template_key,
                    user_tax_rate=user_tax_rate,
                    user_default_hourly_rate=user_default_hourly_rate,
                    user_default_parts_markup=user_default_parts_markup,
                    customers=customers,
                    customers_for_js=customers_for_js,
                    pre_customer=pre_customer,
                )

            customer_id = int(customer_id_raw)

            if user_template_key in ("auto_repair", "lawn_care") and not vehicle:
                flash("Vehicle is required for Auto Repair invoices.", "error")
                return render_template(
                    "invoice_form.html",
                    mode="new",
                    doc_type="invoice",
                    form=request.form,
                    tmpl=tmpl,
                    tmpl_key=user_template_key,
                    user_tax_rate=user_tax_rate,
                    user_default_hourly_rate=user_default_hourly_rate,
                    user_default_parts_markup=user_default_parts_markup,
                    customers=customers,
                    customers_for_js=customers_for_js,
                    pre_customer=pre_customer,
                )

            with db_session() as s:
                c = _customer_owned_or_404(s, customer_id)
                local_now = _user_local_now(s.get(User, uid))

                year = int(local_now.strftime("%Y"))
                inv_no = next_invoice_number(s, year, Config.INVOICE_SEQ_WIDTH)
                display_no = next_display_number(s, uid, year, "invoice", Config.INVOICE_SEQ_WIDTH)

                cust_email_override = (request.form.get("customer_email") or "").strip() or None
                cust_phone_override = (request.form.get("customer_phone") or "").strip() or None

                parts_data = _parse_repeating_fields(
                    request.form.getlist("part_name"),
                    request.form.getlist("part_price")
                )
                labor_data = _parse_repeating_fields(
                    request.form.getlist("labor_desc"),
                    request.form.getlist("labor_time_hours")
                )

                price_per_hour = _to_float(request.form.get("price_per_hour"), user_default_hourly_rate)
                hours = _to_float(request.form.get("hours"))
                shop_supplies = _to_float(request.form.get("shop_supplies"))
                paid_val = _to_float(request.form.get("paid"))
                parts_markup_percent = _to_float(request.form.get("parts_markup_percent"), user_default_parts_markup)
                tax_rate = _to_float(request.form.get("tax_rate"), user_tax_rate)
                tax_override_raw = (request.form.get("tax_override") or "").strip()
                tax_override = _to_float(tax_override_raw, 0.0) if tax_override_raw else None
                if user_template_key == "flipping_items":
                    price_per_hour = 1.0
                    parts_total = sum(pp for _, pp in parts_data)
                    markup_multiplier = 1 + (parts_markup_percent or 0.0) / 100.0
                    parts_total_with_markup = parts_total * markup_multiplier
                    hours = paid_val - parts_total_with_markup - shop_supplies
                else:
                    hours = sum(t for _, t in labor_data)

                inv = Invoice(
                    user_id=uid,
                    customer_id=c.id,

                    invoice_number=inv_no,
                    display_number=display_no,
                    invoice_template=user_template_key,
                    pdf_template=user_pdf_template,

                    customer_email=(cust_email_override or (c.email or None)),
                    customer_phone=(cust_phone_override or (c.phone or None)),

                    name=(c.name or "").strip(),
                    vehicle=vehicle,

                    hours=hours,
                    price_per_hour=price_per_hour,
                    shop_supplies=shop_supplies,
                    parts_markup_percent=parts_markup_percent,
                    tax_rate=tax_rate,
                    tax_override=tax_override,
                    paid=paid_val,
                    date_in=(request.form.get("date_in", "").strip() or local_now.strftime("%B %d, %Y")),
                    notes=request.form.get("notes", "").rstrip(),
                    useful_info=(request.form.get("useful_info") or "").rstrip() or None,
                )

                for pn, pp in parts_data:
                    inv.parts.append(InvoicePart(part_name=pn, part_price=pp))

                if user_template_key != "flipping_items":
                    for desc, t in labor_data:
                        inv.labor_items.append(InvoiceLabor(labor_desc=desc, labor_time_hours=t))

                s.add(inv)
                s.flush()
                try:
                    generate_and_store_pdf(s, inv.id)
                except Exception as exc:
                    flash(f"Invoice saved, but PDF generation failed: {exc}", "warning")
                s.commit()

                return redirect(url_for("invoice_view", invoice_id=inv.id))

        return render_template(
            "invoice_form.html",
            mode="new",
            default_date=default_local_now.strftime("%B %d, %Y"),
            doc_type="invoice",
            tmpl=tmpl,
            tmpl_key=user_template_key,
            user_tax_rate=user_tax_rate,
            user_default_hourly_rate=user_default_hourly_rate,
            user_default_parts_markup=user_default_parts_markup,
            customers=customers,
            customers_for_js=customers_for_js,
            pre_customer=pre_customer,
        )

    # -----------------------------
    # View estimate
    # -----------------------------
    @app.route("/estimates/<int:estimate_id>")
    @login_required
    def estimate_view(estimate_id):
        with db_session() as s:
            inv = _estimate_owned_or_404(s, estimate_id)
            tmpl = _template_config_for(inv.invoice_template)
            c = s.get(Customer, inv.customer_id) if getattr(inv, "customer_id", None) else None
        return render_template("estimate_view.html", inv=inv, tmpl=tmpl, customer=c)

    # -----------------------------
    # Edit estimate — GATED
    # -----------------------------
    @app.route("/estimates/<int:estimate_id>/edit", methods=["GET", "POST"])
    @login_required
    @subscription_required
    def estimate_edit(estimate_id):
        uid = _current_user_id_int()

        with db_session() as s:
            inv = _estimate_owned_or_404(s, estimate_id)

            customers = (
                s.query(Customer)
                .filter(Customer.user_id == uid)
                .order_by(Customer.name.asc())
                .all()
            )

            customers_for_js = [{
                "id": c.id,
                "name": (c.name or "").strip(),
                "email": (c.email or "").strip(),
                "phone": (c.phone or "").strip(),
            } for c in customers]

            tmpl_key = _template_key_fallback(inv.invoice_template)
            tmpl = _template_config_for(tmpl_key)

            if request.method == "POST":
                customer_id_raw = (request.form.get("customer_id") or "").strip()
                if not customer_id_raw.isdigit():
                    flash("Please select a customer from the list.", "error")
                    return render_template(
                        "invoice_form.html",
                        mode="edit",
                        inv=inv,
                        doc_type="estimate",
                        form=request.form,
                        tmpl=tmpl,
                        tmpl_key=tmpl_key,
                        user_tax_rate=inv.tax_rate or 0.0,
                        customers=customers,
                        customers_for_js=customers_for_js,
                    )

                customer_id = int(customer_id_raw)
                c = _customer_owned_or_404(s, customer_id)

                inv.customer_id = customer_id
                inv.name = (c.name or "").strip()

                parts_data = _parse_repeating_fields(
                    request.form.getlist("part_name"),
                    request.form.getlist("part_price")
                )
                labor_data = _parse_repeating_fields(
                    request.form.getlist("labor_desc"),
                    request.form.getlist("labor_time_hours")
                )

                inv.vehicle = (request.form.get("vehicle") or "").strip()
                inv.hours = _to_float(request.form.get("hours"))
                inv.price_per_hour = _to_float(request.form.get("price_per_hour"))
                inv.shop_supplies = _to_float(request.form.get("shop_supplies"))
                inv.parts_markup_percent = _to_float(request.form.get("parts_markup_percent"))
                inv.tax_rate = _to_float(request.form.get("tax_rate"), inv.tax_rate or 0.0)
                tax_override_raw = (request.form.get("tax_override") or "").strip()
                inv.tax_override = _to_float(tax_override_raw, 0.0) if tax_override_raw else None
                if tmpl_key == "flipping_items":
                    inv.price_per_hour = 1.0
                    inv.hours = 0.0
                else:
                    inv.hours = sum(t for _, t in labor_data)
                inv.paid = 0.0
                inv.date_in = request.form.get("date_in", "").strip()
                inv.notes = (request.form.get("notes") or "").rstrip()
                inv.useful_info = (request.form.get("useful_info") or "").rstrip() or None

                cust_email_override = (request.form.get("customer_email") or "").strip() or None
                cust_phone_override = (request.form.get("customer_phone") or "").strip() or None

                inv.customer_email = cust_email_override or (c.email or None)
                inv.customer_phone = cust_phone_override or (c.phone or None)
                inv.is_estimate = True

                if tmpl_key in ("auto_repair", "lawn_care") and not inv.vehicle:
                    flash("Vehicle is required for Auto Repair estimates.", "error")
                    return render_template(
                        "invoice_form.html",
                        mode="edit",
                        inv=inv,
                        doc_type="estimate",
                        form=request.form,
                        tmpl=tmpl,
                        tmpl_key=tmpl_key,
                        user_tax_rate=inv.tax_rate or 0.0,
                        customers=customers,
                        customers_for_js=customers_for_js,
                    )

                inv.parts.clear()
                inv.labor_items.clear()

                for pn, pp in parts_data:
                    inv.parts.append(InvoicePart(part_name=pn, part_price=pp))

                if tmpl_key != "flipping_items":
                    for desc, t in labor_data:
                        inv.labor_items.append(InvoiceLabor(labor_desc=desc, labor_time_hours=t))

                try:
                    generate_and_store_pdf(s, inv.id)
                except Exception as exc:
                    flash(f"Estimate saved, but PDF generation failed: {exc}", "warning")
                s.commit()
                return redirect(url_for("estimate_view", estimate_id=inv.id))

        return render_template(
            "invoice_form.html",
            mode="edit",
            inv=inv,
            doc_type="estimate",
            tmpl=tmpl,
            tmpl_key=tmpl_key,
            user_tax_rate=inv.tax_rate or 0.0,
            customers=customers,
            customers_for_js=customers_for_js,
        )

    # -----------------------------
    # Convert estimate to invoice — GATED
    # -----------------------------
    @app.route("/estimates/<int:estimate_id>/convert", methods=["POST"])
    @login_required
    @subscription_required
    def estimate_convert(estimate_id: int):
        uid = _current_user_id_int()
        with db_session() as s:
            inv = _estimate_owned_or_404(s, estimate_id)
            local_now = _user_local_now(s.get(User, uid))

            year = int(local_now.strftime("%Y"))
            new_no = next_invoice_number(s, year, Config.INVOICE_SEQ_WIDTH)
            display_no = next_display_number(s, uid, year, "invoice", Config.INVOICE_SEQ_WIDTH)

            new_inv = Invoice(
                user_id=uid,
                customer_id=inv.customer_id,
                invoice_number=new_no,
                display_number=display_no,
                invoice_template=inv.invoice_template,
                pdf_template=inv.pdf_template,
                is_estimate=False,
                converted_from_estimate=True,

                name=inv.name,
                vehicle=inv.vehicle,

                hours=inv.hours,
                price_per_hour=inv.price_per_hour,
                shop_supplies=inv.shop_supplies,
                parts_markup_percent=inv.parts_markup_percent,
                tax_rate=inv.tax_rate,
                tax_override=inv.tax_override,

                notes=inv.notes,
                useful_info=inv.useful_info,
                paid=0.0,
                date_in=inv.date_in or local_now.strftime("%m/%d/%Y"),

                customer_email=inv.customer_email,
                customer_phone=inv.customer_phone,

                pdf_path=None,
                pdf_generated_at=None,
            )

            inv.converted_to_invoice = True

            for p in inv.parts:
                new_inv.parts.append(InvoicePart(part_name=p.part_name, part_price=p.part_price))

            for li in inv.labor_items:
                new_inv.labor_items.append(InvoiceLabor(labor_desc=li.labor_desc, labor_time_hours=li.labor_time_hours))

            s.add(new_inv)
            s.commit()

            flash(f"Estimate converted to invoice {display_no}.", "success")
            return redirect(url_for("invoice_edit", invoice_id=new_inv.id))

    @app.route("/invoices/<int:invoice_id>/convert", methods=["POST"])
    @login_required
    @subscription_required
    def invoice_convert(invoice_id: int):
        uid = _current_user_id_int()
        with db_session() as s:
            inv = _invoice_owned_or_404(s, invoice_id)
            local_now = _user_local_now(s.get(User, uid))

            year = int(local_now.strftime("%Y"))
            new_no = next_invoice_number(s, year, Config.INVOICE_SEQ_WIDTH)
            display_no = next_display_number(s, uid, year, "estimate", Config.INVOICE_SEQ_WIDTH)

            new_inv = Invoice(
                user_id=uid,
                customer_id=inv.customer_id,
                invoice_number=new_no,
                display_number=display_no,
                invoice_template=inv.invoice_template,
                pdf_template=inv.pdf_template,
                is_estimate=True,

                name=inv.name,
                vehicle=inv.vehicle,

                hours=inv.hours,
                price_per_hour=inv.price_per_hour,
                shop_supplies=inv.shop_supplies,
                parts_markup_percent=inv.parts_markup_percent,
                tax_rate=inv.tax_rate,
                tax_override=inv.tax_override,

                notes=inv.notes,
                useful_info=inv.useful_info,
                paid=0.0,
                date_in=inv.date_in or local_now.strftime("%m/%d/%Y"),

                customer_email=inv.customer_email,
                customer_phone=inv.customer_phone,

                pdf_path=None,
                pdf_generated_at=None,
            )

            for p in inv.parts:
                new_inv.parts.append(InvoicePart(part_name=p.part_name, part_price=p.part_price))

            for li in inv.labor_items:
                new_inv.labor_items.append(InvoiceLabor(labor_desc=li.labor_desc, labor_time_hours=li.labor_time_hours))

            s.add(new_inv)
            s.commit()

            flash(f"Invoice converted to estimate {display_no}.", "success")
            return redirect(url_for("estimate_edit", estimate_id=new_inv.id))

    # -----------------------------
    # Delete estimate — GATED
    # -----------------------------
    @app.route("/estimates/<int:estimate_id>/delete", methods=["POST"])
    @login_required
    @subscription_required
    def estimate_delete(estimate_id: int):
        delete_pdf = (request.form.get("delete_pdf") or "").strip() == "1"

        with db_session() as s:
            inv = _estimate_owned_or_404(s, estimate_id)
            pdf_path = inv.pdf_path
            s.delete(inv)
            s.commit()

        if delete_pdf and pdf_path and os.path.exists(pdf_path):
            try:
                os.remove(pdf_path)
            except Exception:
                pass

        flash("Estimate deleted.", "success")
        return redirect(url_for("customers_list"))

    # -----------------------------
    # View invoice
    # -----------------------------
    @app.route("/invoices/<int:invoice_id>")
    @login_required
    def invoice_view(invoice_id):
        with db_session() as s:
            inv = _invoice_owned_or_404(s, invoice_id)
            tmpl = _template_config_for(inv.invoice_template)
            c = s.get(Customer, inv.customer_id) if getattr(inv, "customer_id", None) else None
        return render_template("invoice_view.html", inv=inv, tmpl=tmpl, customer=c)

    # -----------------------------
    # Duplicate invoice — GATED
    # -----------------------------
    @app.route("/invoices/<int:invoice_id>/duplicate", methods=["POST"])
    @login_required
    @subscription_required
    def invoice_duplicate(invoice_id: int):
        uid = _current_user_id_int()
        with db_session() as s:
            inv = _invoice_owned_or_404(s, invoice_id)
            local_now = _user_local_now(s.get(User, uid))

            year = int(local_now.strftime("%Y"))
            new_no = next_invoice_number(s, year, Config.INVOICE_SEQ_WIDTH)
            display_no = next_display_number(s, uid, year, "invoice", Config.INVOICE_SEQ_WIDTH)

            new_inv = Invoice(
                user_id=uid,
                customer_id=inv.customer_id,
                invoice_number=new_no,
                display_number=display_no,
                invoice_template=inv.invoice_template,
                pdf_template=inv.pdf_template,

                name=inv.name,
                vehicle=inv.vehicle,

                hours=inv.hours,
                price_per_hour=inv.price_per_hour,
                shop_supplies=inv.shop_supplies,
                parts_markup_percent=inv.parts_markup_percent,
                tax_rate=inv.tax_rate,
                tax_override=inv.tax_override,

                notes=inv.notes,
                useful_info=inv.useful_info,
                paid=0.0,
                date_in=local_now.strftime("%m/%d/%Y"),

                customer_email=inv.customer_email,
                customer_phone=inv.customer_phone,

                pdf_path=None,
                pdf_generated_at=None,
            )

            for p in inv.parts:
                new_inv.parts.append(InvoicePart(part_name=p.part_name, part_price=p.part_price))

            for li in inv.labor_items:
                new_inv.labor_items.append(InvoiceLabor(labor_desc=li.labor_desc, labor_time_hours=li.labor_time_hours))

            s.add(new_inv)
            s.commit()

            flash(f"Duplicated invoice as {display_no}.", "success")
            return redirect(url_for("invoice_edit", invoice_id=new_inv.id))

    # -----------------------------
    # Edit invoice — GATED
    # -----------------------------
    @app.route("/invoices/<int:invoice_id>/edit", methods=["GET", "POST"])
    @login_required
    @subscription_required
    def invoice_edit(invoice_id):
        uid = _current_user_id_int()

        with db_session() as s:
            inv = _invoice_owned_or_404(s, invoice_id)

            customers = (
                s.query(Customer)
                .filter(Customer.user_id == uid)
                .order_by(Customer.name.asc())
                .all()
            )

            customers_for_js = [{
                "id": c.id,
                "name": (c.name or "").strip(),
                "email": (c.email or "").strip(),
                "phone": (c.phone or "").strip(),
            } for c in customers]

            tmpl_key = _template_key_fallback(inv.invoice_template)
            tmpl = _template_config_for(tmpl_key)

            if request.method == "POST":
                customer_id_raw = (request.form.get("customer_id") or "").strip()
                if not customer_id_raw.isdigit():
                    flash("Please select a customer from the list.", "error")
                    return render_template(
                        "invoice_form.html",
                        mode="edit",
                        inv=inv,
                        doc_type="invoice",
                        form=request.form,
                        tmpl=tmpl,
                        tmpl_key=tmpl_key,
                        user_tax_rate=inv.tax_rate or 0.0,
                        customers=customers,
                        customers_for_js=customers_for_js,
                    )

                customer_id = int(customer_id_raw)
                c = _customer_owned_or_404(s, customer_id)

                inv.customer_id = customer_id
                inv.name = (c.name or "").strip()

                parts_data = _parse_repeating_fields(
                    request.form.getlist("part_name"),
                    request.form.getlist("part_price")
                )
                labor_data = _parse_repeating_fields(
                    request.form.getlist("labor_desc"),
                    request.form.getlist("labor_time_hours")
                )

                inv.vehicle = (request.form.get("vehicle") or "").strip()
                inv.hours = _to_float(request.form.get("hours"))
                inv.price_per_hour = _to_float(request.form.get("price_per_hour"))
                inv.shop_supplies = _to_float(request.form.get("shop_supplies"))
                inv.parts_markup_percent = _to_float(request.form.get("parts_markup_percent"))
                inv.tax_rate = _to_float(request.form.get("tax_rate"), inv.tax_rate or 0.0)
                tax_override_raw = (request.form.get("tax_override") or "").strip()
                inv.tax_override = _to_float(tax_override_raw, 0.0) if tax_override_raw else None
                inv.paid = _to_float(request.form.get("paid"))
                if tmpl_key == "flipping_items":
                    inv.price_per_hour = 1.0
                    parts_total = sum(pp for _, pp in parts_data)
                    markup_multiplier = 1 + (inv.parts_markup_percent or 0.0) / 100.0
                    parts_total_with_markup = parts_total * markup_multiplier
                    inv.hours = inv.paid - parts_total_with_markup - inv.shop_supplies
                else:
                    inv.hours = sum(t for _, t in labor_data)
                inv.date_in = request.form.get("date_in", "").strip()
                inv.notes = (request.form.get("notes") or "").rstrip()
                inv.useful_info = (request.form.get("useful_info") or "").rstrip() or None

                cust_email_override = (request.form.get("customer_email") or "").strip() or None
                cust_phone_override = (request.form.get("customer_phone") or "").strip() or None

                inv.customer_email = cust_email_override or (c.email or None)
                inv.customer_phone = cust_phone_override or (c.phone or None)

                if tmpl_key in ("auto_repair", "lawn_care") and not inv.vehicle:
                    flash("Vehicle is required for Auto Repair invoices.", "error")
                    return render_template(
                        "invoice_form.html",
                        mode="edit",
                        inv=inv,
                        doc_type="invoice",
                        form=request.form,
                        tmpl=tmpl,
                        tmpl_key=tmpl_key,
                        user_tax_rate=inv.tax_rate or 0.0,
                        customers=customers,
                        customers_for_js=customers_for_js,
                    )

                inv.parts.clear()
                inv.labor_items.clear()

                for pn, pp in parts_data:
                    inv.parts.append(InvoicePart(part_name=pn, part_price=pp))

                if tmpl_key != "flipping_items":
                    for desc, t in labor_data:
                        inv.labor_items.append(InvoiceLabor(labor_desc=desc, labor_time_hours=t))

                try:
                    generate_and_store_pdf(s, inv.id)
                except Exception as exc:
                    flash(f"Invoice saved, but PDF generation failed: {exc}", "warning")
                s.commit()
                return redirect(url_for("invoice_view", invoice_id=inv.id))

        return render_template(
            "invoice_form.html",
            mode="edit",
            inv=inv,
            doc_type="invoice",
            tmpl=tmpl,
            tmpl_key=tmpl_key,
            user_tax_rate=inv.tax_rate or 0.0,
            customers=customers,
            customers_for_js=customers_for_js,
        )

    # -----------------------------
    # Year Summary — GATED
    # -----------------------------
    def _parse_year_from_datein(date_in: str):
        s = (date_in or "").strip()
        if not s:
            return None

        m = re.search(r"(19\d{2}|20\d{2})", s)
        if m:
            return int(m.group(1))

        for fmt in ("%m/%d/%Y", "%m-%d-%Y", "%Y-%m-%d", "%B %d, %Y", "%b %d, %Y"):
            try:
                return datetime.strptime(s, fmt).year
            except Exception:
                pass
        return None

    def _parse_month_from_datein(date_in: str):
        s = (date_in or "").strip()
        if not s:
            return None
        m = re.search(r"(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})", s)
        if m:
            try:
                month = int(m.group(1))
            except Exception:
                return None
            if 1 <= month <= 12:
                return month
            return None
        for fmt in ("%m/%d/%Y", "%m-%d-%Y", "%Y-%m-%d", "%B %d, %Y", "%b %d, %Y"):
            try:
                return datetime.strptime(s, fmt).month
            except Exception:
                pass
        return None

    def _money(x: float) -> str:
        try:
            return f"${float(x):,.2f}"
        except Exception:
            return str(x)

    @app.route("/year-summary")
    @login_required
    @subscription_required
    def year_summary():
        year_text = (request.args.get("year") or "").strip()
        month_text = (request.args.get("month") or "").strip()
        if not (year_text.isdigit() and len(year_text) == 4):
            year_text = datetime.now().strftime("%Y")
        target_year = int(year_text)
        target_month = int(month_text) if month_text.isdigit() else None
        if target_month is not None and not (1 <= target_month <= 12):
            target_month = None

        EPS = 0.01
        uid = _current_user_id_int()

        count = 0
        total_invoice_amount = 0.0
        total_labor = 0.0
        total_labor_raw = 0.0
        total_parts = 0.0
        total_parts_markup_profit = 0.0
        total_supplies = 0.0
        total_tax_collected = 0.0

        total_paid_invoices_amount = 0.0
        total_outstanding_unpaid = 0.0
        labor_unpaid = 0.0
        labor_unpaid_raw = 0.0

        unpaid = []

        with db_session() as s:
            invs = (
                s.query(Invoice)
                .options(selectinload(Invoice.parts), selectinload(Invoice.labor_items))
                .filter(Invoice.user_id == uid)
                .filter(or_(Invoice.is_estimate.is_(False), Invoice.is_estimate.is_(None)))
                .order_by(Invoice.created_at.desc())
                .all()
            )

            for inv in invs:
                yr = _parse_year_from_datein(inv.date_in)
                if yr != target_year:
                    continue
                if target_month is not None:
                    mo = _parse_month_from_datein(inv.date_in)
                    if mo != target_month:
                        continue

                parts_total_raw = inv.parts_total_raw()
                parts_markup_profit = inv.parts_markup_amount()
                labor_total = inv.labor_total()
                labor_income = labor_total
                if (inv.invoice_template or "") == "flipping_items" and labor_total < 0:
                    labor_income = 0.0
                invoice_total = inv.invoice_total()
                tax_amount = inv.tax_amount()
                supplies = float(inv.shop_supplies or 0.0)
                paid = float(inv.paid or 0.0)

                total_parts += parts_total_raw
                total_parts_markup_profit += parts_markup_profit
                total_labor += labor_income + parts_markup_profit
                total_labor_raw += labor_total
                total_supplies += supplies
                total_invoice_amount += invoice_total
                count += 1

                fully_paid = (paid + EPS >= invoice_total)

                if fully_paid:
                    total_paid_invoices_amount += invoice_total
                    total_tax_collected += tax_amount
                else:
                    outstanding = max(0.0, invoice_total - paid)
                    total_outstanding_unpaid += outstanding
                    labor_unpaid += labor_income + parts_markup_profit
                    labor_unpaid_raw += labor_total

                    unpaid.append({
                        "id": inv.id,
                        "invoice_number": (inv.display_number or inv.invoice_number),
                        "name": inv.name,
                        "vehicle": inv.vehicle,
                        "date_in": inv.date_in,
                        "outstanding": outstanding,
                    })

        profit_paid_labor_only = total_labor_raw - labor_unpaid_raw

        context = {
            "year": year_text,
            "month": (str(target_month) if target_month else ""),
            "count": count,
            "total_invoice_amount": total_invoice_amount,
            "total_parts": total_parts,
            "total_parts_markup_profit": total_parts_markup_profit,
            "total_labor": total_labor,
            "total_supplies": total_supplies,
            "total_tax_collected": total_tax_collected,
            "total_paid_invoices_amount": total_paid_invoices_amount,
            "total_outstanding_unpaid": total_outstanding_unpaid,
            "unpaid_count": len(unpaid),
            "profit_paid_labor_only": profit_paid_labor_only,
            "unpaid": unpaid,
            "money": _money,
        }
        return render_template("year_summary.html", **context)

    # -----------------------------
    # Delete invoice — GATED
    # -----------------------------
    @app.route("/invoices/<int:invoice_id>/delete", methods=["POST"])
    @login_required
    @subscription_required
    def invoice_delete(invoice_id: int):
        delete_pdf = (request.form.get("delete_pdf") or "").strip() == "1"

        with db_session() as s:
            inv = _invoice_owned_or_404(s, invoice_id)
            pdf_path = inv.pdf_path
            s.delete(inv)
            s.commit()

        if delete_pdf and pdf_path and os.path.exists(pdf_path):
            try:
                os.remove(pdf_path)
            except Exception:
                pass

        flash("Invoice deleted.", "success")
        return redirect(url_for("customers_list"))

    # -----------------------------
    # Mark invoice as paid — GATED
    # -----------------------------
    @app.route("/invoices/<int:invoice_id>/mark_paid", methods=["POST"])
    @login_required
    @subscription_required
    def invoice_mark_paid(invoice_id: int):
        with db_session() as s:
            inv = _invoice_owned_or_404(s, invoice_id)
            inv.paid = inv.invoice_total()
            s.commit()

        flash("Invoice marked as paid.", "success")
        return redirect(url_for("invoice_view", invoice_id=invoice_id))

    # -----------------------------
    # PDF routes — GATED
    # -----------------------------
    @app.route("/estimates/<int:estimate_id>/pdf/generate", methods=["POST"])
    @login_required
    @subscription_required
    def estimate_pdf_generate(estimate_id):
        with db_session() as s:
            _estimate_owned_or_404(s, estimate_id)
            generate_and_store_pdf(s, estimate_id)
        return redirect(url_for("estimate_view", estimate_id=estimate_id))

    @app.route("/estimates/<int:estimate_id>/pdf/download")
    @login_required
    @subscription_required
    def estimate_pdf_download(estimate_id):
        with db_session() as s:
            _estimate_owned_or_404(s, estimate_id)
            try:
                pdf_path = generate_and_store_pdf(s, estimate_id)
            except Exception as exc:
                flash(f"Could not generate estimate PDF: {exc}", "error")
                return redirect(url_for("estimate_view", estimate_id=estimate_id))

            return send_file(
                pdf_path,
                as_attachment=True,
                download_name=os.path.basename(pdf_path),
                mimetype="application/pdf"
            )

    @app.route("/estimates/<int:estimate_id>/send", methods=["POST"])
    @login_required
    @subscription_required
    def estimate_send(estimate_id: int):
        with db_session() as s:
            inv = _estimate_owned_or_404(s, estimate_id)

            customer = s.get(Customer, inv.customer_id) if getattr(inv, "customer_id", None) else None
            customer_name = (customer.name if customer else "").strip()

            to_email = (inv.customer_email or (customer.email if customer else "") or "").strip().lower()
            if not to_email or "@" not in to_email:
                flash("Customer email is missing. Add it on the estimate edit page (or customer profile).", "error")
                return redirect(url_for("estimate_view", estimate_id=estimate_id))

            if (not inv.pdf_path) or (not os.path.exists(inv.pdf_path)):
                generate_and_store_pdf(s, estimate_id)
                inv = _estimate_owned_or_404(s, estimate_id)

            display_no = inv.display_number or inv.invoice_number
            subject = f"Estimate {display_no}"
            body = (
                f"Hello {customer_name or 'there'},\n\n"
                f"Attached is your estimate {display_no}.\n"
                f"Details: {inv.vehicle}\n"
                f"Total: ${inv.invoice_total():,.2f}\n\n"
                "Thank you."
            )

            try:
                _send_invoice_pdf_email(
                    to_email=to_email,
                    subject=subject,
                    body_text=body,
                    pdf_path=inv.pdf_path,
                )
            except Exception as e:
                print(f"[ESTIMATE SEND] SMTP ERROR to={to_email} estimate={display_no}: {repr(e)}", flush=True)
                flash("Could not send email (SMTP / sender config issue). Check Render logs.", "error")
                return redirect(url_for("estimate_view", estimate_id=estimate_id))

        flash("Estimate email sent.", "success")
        return redirect(url_for("estimate_view", estimate_id=estimate_id))

    @app.route("/estimates/<int:estimate_id>/text", methods=["POST"])
    @login_required
    @subscription_required
    def estimate_text(estimate_id: int):
        with db_session() as s:
            inv = _estimate_owned_or_404(s, estimate_id)
            customer = s.get(Customer, inv.customer_id) if getattr(inv, "customer_id", None) else None
            customer_name = (customer.name if customer else "").strip()
            to_phone_raw = (inv.customer_phone or (customer.phone if customer else "") or "").strip()
            to_phone = _to_e164_phone(to_phone_raw)
            if not to_phone:
                flash("Customer phone is missing or invalid. Add it on the estimate edit page (or customer profile).", "error")
                return redirect(url_for("estimate_view", estimate_id=estimate_id))

            if (not inv.pdf_path) or (not os.path.exists(inv.pdf_path)):
                generate_and_store_pdf(s, estimate_id)
                inv = _estimate_owned_or_404(s, estimate_id)

            share_token = make_pdf_share_token(inv.user_id or _current_user_id_int(), inv.id)
            share_url = _public_url(url_for("shared_pdf_download", token=share_token))
            display_no = inv.display_number or inv.invoice_number
            body = (
                f"Hi {customer_name or 'there'}, your estimate {display_no} is ready. "
                f"Total: ${inv.invoice_total():,.2f}. View PDF: {share_url}"
            )

            try:
                _send_sms_via_twilio(to_phone_e164=to_phone, body_text=body)
            except Exception as e:
                print(f"[ESTIMATE SMS] TWILIO ERROR to={to_phone} estimate={display_no}: {repr(e)}", flush=True)
                flash("Could not send text message. Check Twilio config/logs.", "error")
                return redirect(url_for("estimate_view", estimate_id=estimate_id))

        flash("Estimate text sent.", "success")
        return redirect(url_for("estimate_view", estimate_id=estimate_id))

    @app.route("/invoices/<int:invoice_id>/pdf/generate", methods=["POST"])
    @login_required
    @subscription_required
    def invoice_pdf_generate(invoice_id):
        with db_session() as s:
            _invoice_owned_or_404(s, invoice_id)
            generate_and_store_pdf(s, invoice_id)
        return redirect(url_for("invoice_view", invoice_id=invoice_id))

    @app.route("/invoices/<int:invoice_id>/pdf/download")
    @login_required
    @subscription_required
    def invoice_pdf_download(invoice_id):
        with db_session() as s:
            _invoice_owned_or_404(s, invoice_id)
            try:
                pdf_path = generate_and_store_pdf(s, invoice_id)
            except Exception as exc:
                flash(f"Could not generate invoice PDF: {exc}", "error")
                return redirect(url_for("invoice_view", invoice_id=invoice_id))

            return send_file(
                pdf_path,
                as_attachment=True,
                download_name=os.path.basename(pdf_path),
                mimetype="application/pdf"
            )

    @app.route("/invoices/<int:invoice_id>/send", methods=["POST"])
    @login_required
    @subscription_required
    def invoice_send(invoice_id: int):
        with db_session() as s:
            inv = _invoice_owned_or_404(s, invoice_id)

            customer = s.get(Customer, inv.customer_id) if getattr(inv, "customer_id", None) else None
            customer_name = (customer.name if customer else "").strip()

            to_email = (inv.customer_email or (customer.email if customer else "") or "").strip().lower()
            if not to_email or "@" not in to_email:
                flash("Customer email is missing. Add it on the invoice edit page (or customer profile).", "error")
                return redirect(url_for("invoice_view", invoice_id=invoice_id))

            if (not inv.pdf_path) or (not os.path.exists(inv.pdf_path)):
                generate_and_store_pdf(s, invoice_id)
                inv = _invoice_owned_or_404(s, invoice_id)

            display_no = inv.display_number or inv.invoice_number
            subject = f"Invoice {display_no}"
            body = (
                f"Hello {customer_name or 'there'},\n\n"
                f"Attached is your invoice {display_no}.\n"
                f"Details: {inv.vehicle}\n"
                f"Total: ${inv.invoice_total():,.2f}\n\n"
                "Thank you."
            )

            try:
                _send_invoice_pdf_email(
                    to_email=to_email,
                    subject=subject,
                    body_text=body,
                    pdf_path=inv.pdf_path,
                )
            except Exception as e:
                print(f"[INVOICE SEND] SMTP ERROR to={to_email} inv={display_no}: {repr(e)}", flush=True)
                flash("Could not send email (SMTP / sender config issue). Check Render logs.", "error")
                return redirect(url_for("invoice_view", invoice_id=invoice_id))

        flash("Invoice email sent.", "success")
        return redirect(url_for("invoice_view", invoice_id=invoice_id))

    @app.route("/invoices/<int:invoice_id>/text", methods=["POST"])
    @login_required
    @subscription_required
    def invoice_text(invoice_id: int):
        with db_session() as s:
            inv = _invoice_owned_or_404(s, invoice_id)
            customer = s.get(Customer, inv.customer_id) if getattr(inv, "customer_id", None) else None
            customer_name = (customer.name if customer else "").strip()
            to_phone_raw = (inv.customer_phone or (customer.phone if customer else "") or "").strip()
            to_phone = _to_e164_phone(to_phone_raw)
            if not to_phone:
                flash("Customer phone is missing or invalid. Add it on the invoice edit page (or customer profile).", "error")
                return redirect(url_for("invoice_view", invoice_id=invoice_id))

            if (not inv.pdf_path) or (not os.path.exists(inv.pdf_path)):
                generate_and_store_pdf(s, invoice_id)
                inv = _invoice_owned_or_404(s, invoice_id)

            share_token = make_pdf_share_token(inv.user_id or _current_user_id_int(), inv.id)
            share_url = _public_url(url_for("shared_pdf_download", token=share_token))
            display_no = inv.display_number or inv.invoice_number
            body = (
                f"Hi {customer_name or 'there'}, your invoice {display_no} is ready. "
                f"Total: ${inv.invoice_total():,.2f}. View PDF: {share_url}"
            )

            try:
                _send_sms_via_twilio(to_phone_e164=to_phone, body_text=body)
            except Exception as e:
                print(f"[INVOICE SMS] TWILIO ERROR to={to_phone} inv={display_no}: {repr(e)}", flush=True)
                flash("Could not send text message. Check Twilio config/logs.", "error")
                return redirect(url_for("invoice_view", invoice_id=invoice_id))

        flash("Invoice text sent.", "success")
        return redirect(url_for("invoice_view", invoice_id=invoice_id))

    @app.get("/shared/p/<token>")
    def shared_pdf_download(token: str):
        max_age = int(current_app.config.get("PDF_SHARE_MAX_AGE_SECONDS") or os.getenv("PDF_SHARE_MAX_AGE_SECONDS") or "2592000")
        decoded = read_pdf_share_token(token, max_age_seconds=max_age)
        if not decoded:
            abort(404)
        user_id, invoice_id = decoded
        with db_session() as s:
            inv = (
                s.query(Invoice)
                .filter(Invoice.id == invoice_id, Invoice.user_id == user_id)
                .first()
            )
            if not inv:
                abort(404)
            try:
                pdf_path = generate_and_store_pdf(s, invoice_id)
            except Exception:
                abort(500)
            return send_file(
                pdf_path,
                as_attachment=True,
                download_name=os.path.basename(pdf_path),
                mimetype="application/pdf",
            )

    @app.route("/pdfs/download_all")
    @login_required
    @subscription_required
    def pdfs_download_all():
        year = (request.args.get("year") or "").strip()
        month_text = (request.args.get("month") or "").strip()
        target_month = int(month_text) if month_text.isdigit() else None
        if target_month is not None and not (1 <= target_month <= 12):
            target_month = None
        uid = _current_user_id_int()

        with db_session() as s:
            q = (
                s.query(Invoice)
                .filter(Invoice.user_id == uid)
                .filter(Invoice.pdf_path.isnot(None))
                .filter(or_(Invoice.is_estimate.is_(False), Invoice.is_estimate.is_(None)))
            )
            if year.isdigit() and len(year) == 4:
                q = q.filter(Invoice.display_number.startswith(year))
            invoices = q.all()

        mem = io.BytesIO()
        with zipfile.ZipFile(mem, "w", zipfile.ZIP_DEFLATED) as z:
            for inv in invoices:
                if target_month is not None:
                    mo = _parse_month_from_datein(inv.date_in)
                    if mo != target_month:
                        continue
                if inv.pdf_path and os.path.exists(inv.pdf_path):
                    z.write(inv.pdf_path, arcname=os.path.basename(inv.pdf_path))

        mem.seek(0)
        return send_file(mem, as_attachment=True, download_name="invoices_pdfs.zip")

    return app


if __name__ == "__main__":
    app = create_app()
    app.run(debug=True, port=5001)
