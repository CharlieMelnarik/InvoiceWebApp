# app.py
import os
import re
import io
import zipfile
import smtplib
from email.message import EmailMessage
from datetime import datetime
from pathlib import Path
from functools import wraps
from werkzeug.utils import secure_filename


import stripe
from flask import (
    Flask, render_template, request, redirect, url_for,
    flash, send_file, abort, current_app
)
from flask_login import (
    LoginManager, UserMixin, login_user, logout_user,
    login_required, current_user
)
from sqlalchemy import text
from sqlalchemy.orm import selectinload
from sqlalchemy.exc import IntegrityError
from werkzeug.security import generate_password_hash, check_password_hash
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired

from config import Config
from models import (
    Base, make_engine, make_session_factory,
    User, Customer, Invoice, InvoicePart, InvoiceLabor, next_invoice_number
)
from pdf_service import generate_and_store_pdf

login_manager = LoginManager()
login_manager.login_view = "login"


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
        "job_label": "Client / Engagement",
        "labor_title": "Services",
        "labor_desc_label": "Service Description",
        "parts_title": "Expenses",
        "parts_name_label": "Expense",
        "shop_supplies_label": "Admin / Filing Fees",
    },
}


def _template_key_fallback(key: str | None) -> str:
    key = (key or "").strip()
    return key if key in INVOICE_TEMPLATES else "auto_repair"


def _template_config_for(key: str | None) -> dict:
    return INVOICE_TEMPLATES[_template_key_fallback(key)]


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

    login_manager.init_app(app)

    engine = make_engine(Config.SQLALCHEMY_DATABASE_URI, echo=Config.SQLALCHEMY_ECHO)

    Base.metadata.create_all(bind=engine)

    _migrate_add_user_id(engine)
    _migrate_user_profile_fields(engine)
    _migrate_user_email(engine)
    _migrate_invoice_contact_fields(engine)
    _migrate_user_invoice_template(engine)
    _migrate_invoice_template(engine)
    _migrate_user_billing_fields(engine)
    _migrate_user_security_fields(engine)
    _migrate_customers(engine)
    _migrate_customers_unique_name_ci(engine)
    _migrate_invoice_customer_id(engine)
    _migrate_user_logo(engine)


    SessionLocal = make_session_factory(engine)

    def db_session():
        return SessionLocal()

    @app.context_processor
    def inject_billing():
        if not current_user.is_authenticated:
            return {}

        with db_session() as s:
            u = s.get(User, _current_user_id_int())
            status = (getattr(u, "subscription_status", None) or "").strip().lower()
            is_sub = status in ("trialing", "active")

        return {
            "billing_status": status or None,
            "is_subscribed": is_sub,
            "trial_used": bool(getattr(u, "trial_used_at", None)),
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

        # Fill missing target fields from source (only if target is empty)
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
                    flash(generic_fail_msg, "error")
                    return render_template("login.html")

                if bool(getattr(u, "password_reset_required", False)):
                    flash("Too many failed login attempts. Please reset your password.", "error")
                    return redirect(url_for("forgot_password"))

                if check_password_hash(u.password_hash, password):
                    u.failed_login_attempts = 0
                    u.password_reset_required = False
                    u.last_failed_login = None
                    s.commit()

                    login_user(AppUser(u.id, u.username))
                    return redirect(url_for("customers_list"))

                attempts = int(getattr(u, "failed_login_attempts", 0) or 0) + 1
                u.failed_login_attempts = attempts
                u.last_failed_login = datetime.utcnow()

                if attempts >= 6:
                    u.password_reset_required = True
                    s.commit()
                    flash("Too many failed login attempts. Please reset your password.", "error")
                    return redirect(url_for("forgot_password"))

                s.commit()
                flash(generic_fail_msg, "error")
                return render_template("login.html")

        return render_template("login.html")

    @app.route("/register", methods=["GET", "POST"])
    def register():
        if current_user.is_authenticated:
            return redirect(url_for("customers_list"))

        if request.method == "POST":
            username = (request.form.get("username") or "").strip()
            email = _normalize_email(request.form.get("email") or "")
            password = request.form.get("password") or ""
            confirm = request.form.get("confirm") or ""

            if not username or len(username) < 3:
                flash("Username must be at least 3 characters.", "error")
                return render_template("register.html")

            if not _looks_like_email(email):
                flash("A valid email address is required.", "error")
                return render_template("register.html")

            if not password or len(password) < 6:
                flash("Password must be at least 6 characters.", "error")
                return render_template("register.html")

            if password != confirm:
                flash("Passwords do not match.", "error")
                return render_template("register.html")

            with db_session() as s:
                taken_user = s.query(User).filter(User.username == username).first()
                if taken_user:
                    flash("That username is already taken.", "error")
                    return render_template("register.html")

                taken_email = (
                    s.query(User)
                    .filter(text("lower(email) = :e"))
                    .params(e=email)
                    .first()
                )
                if taken_email:
                    flash("That email is already registered.", "error")
                    return render_template("register.html")

                u = User(username=username, email=email, password_hash=generate_password_hash(password))
                s.add(u)
                s.commit()

                login_user(AppUser(u.id, u.username))
                return redirect(url_for("customers_list"))

        return render_template("register.html")

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
                    return render_template("settings.html", u=u, templates=INVOICE_TEMPLATES)

                if (u.email or "").strip().lower() != new_email:
                    taken_email = (
                        s.query(User)
                        .filter(text("lower(email) = :e AND id != :id"))
                        .params(e=new_email, id=u.id)
                        .first()
                    )
                    if taken_email:
                        flash("That email is already in use.", "error")
                        return render_template("settings.html", u=u, templates=INVOICE_TEMPLATES)

                    u.email = new_email
                
                # -----------------------------
                # Logo upload / remove
                # -----------------------------
                remove_logo = (request.form.get("remove_logo") or "").strip() == "1"
                logo_file = request.files.get("logo")

                if remove_logo:
                    # delete existing logo file if present
                    old_rel = (getattr(u, "logo_path", None) or "").strip()
                    if old_rel:
                        old_abs = (Path("instance") / old_rel).resolve()
                        try:
                            if old_abs.exists():
                                old_abs.unlink()
                        except Exception:
                            pass
                    u.logo_path = None

                elif logo_file and getattr(logo_file, "filename", ""):
                    filename = secure_filename(logo_file.filename or "")
                    ext = (os.path.splitext(filename)[1] or "").lower()

                    if ext not in (".png", ".jpg", ".jpeg"):
                        flash("Logo must be a .png, .jpg, or .jpeg file.", "error")
                        return render_template("settings.html", u=u, templates=INVOICE_TEMPLATES)

                    # store under instance/uploads/logos/
                    d = _logo_upload_dir()
                    out_name = f"user_{u.id}{ext}"
                    out_path = (d / out_name).resolve()

                    # delete old if different extension/name
                    old_rel = (getattr(u, "logo_path", None) or "").strip()
                    if old_rel:
                        old_abs = (Path("instance") / old_rel).resolve()
                        try:
                            if old_abs.exists() and old_abs != out_path:
                                old_abs.unlink()
                        except Exception:
                            pass

                    logo_file.save(out_path)

                    # store relative-to-instance path (portable)
                    u.logo_path = str(Path("uploads") / "logos" / out_name)


                tmpl = (request.form.get("invoice_template") or "").strip()
                tmpl = _template_key_fallback(tmpl)
                u.invoice_template = tmpl

                u.business_name = (request.form.get("business_name") or "").strip() or None
                u.phone = (request.form.get("phone") or "").strip() or None
                u.address = (request.form.get("address") or "").strip() or None

                s.commit()
                flash("Settings saved.", "success")
                return redirect(url_for("settings"))

            return render_template("settings.html", u=u, templates=INVOICE_TEMPLATES)

    # -----------------------------
    # Billing pages
    # -----------------------------
    @app.route("/billing")
    @login_required
    def billing():
        with db_session() as s:
            u = s.get(User, _current_user_id_int())
            status = (getattr(u, "subscription_status", None) or "none")
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
                abort(403)

            status = (getattr(u, "subscription_status", None) or "").lower().strip()

            if status in ("trialing", "active", "past_due"):
                flash("You already have an active subscription. Manage billing below.", "info")
                return redirect(url_for("billing"))

            cust = (getattr(u, "stripe_customer_id", None) or "").strip()
            if not cust:
                customer = stripe.Customer.create(
                    email=(u.email or None),
                    metadata={"app_user_id": str(uid)},
                )
                cust = customer["id"]
                u.stripe_customer_id = cust
                s.commit()

            subscription_data = {}
            if getattr(u, "trial_used_at", None) is None:
                subscription_data["trial_period_days"] = 7

            cs = stripe.checkout.Session.create(
                mode="subscription",
                customer=cust,
                line_items=[{"price": STRIPE_PRICE_ID, "quantity": 1}],
                client_reference_id=str(uid),
                subscription_data=subscription_data,
                success_url=f"{base}{url_for('billing_success')}?session_id={{CHECKOUT_SESSION_ID}}",
                cancel_url=f"{base}{url_for('billing')}",
            )

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
            cust = (getattr(u, "stripe_customer_id", None) or "").strip()
            if not cust:
                flash("No billing profile yet. Start your trial first.", "error")
                return redirect(url_for("billing"))

        ps = stripe.billing_portal.Session.create(
            customer=cust,
            return_url=f"{base}{url_for('billing')}",
        )
        return redirect(ps.url, code=303)

    # -----------------------------
    # Stripe Webhook
    # -----------------------------
    @app.route("/stripe/webhook", methods=["POST"])
    def stripe_webhook():
        STRIPE_WEBHOOK_SECRET = app.config.get("STRIPE_WEBHOOK_SECRET") or os.getenv("STRIPE_WEBHOOK_SECRET")
        if not STRIPE_WEBHOOK_SECRET:
            abort(500)

        payload = request.get_data(as_text=False)
        sig_header = request.headers.get("Stripe-Signature", "")

        try:
            event = stripe.Webhook.construct_event(
                payload=payload,
                sig_header=sig_header,
                secret=STRIPE_WEBHOOK_SECRET,
            )
        except Exception as e:
            print(f"[STRIPE] Webhook signature verification failed: {repr(e)}", flush=True)
            return ("bad signature", 400)

        etype = event["type"]
        obj = event["data"]["object"]

        with db_session() as s:
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

        return ("ok", 200)

    # -----------------------------
    # Index
    # -----------------------------
    @app.route("/")
    def index():
        return redirect(url_for("customers_list" if current_user.is_authenticated else "login"))

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
            address = (request.form.get("address") or "").strip() or None

            if not name:
                flash("Customer name is required.", "error")
                return render_template("customer_form.html", mode="new", form=request.form)

            with db_session() as s:
                # quick UX: if exists (case-insensitive), jump to it
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
                )
                s.add(c)
                try:
                    s.commit()
                except IntegrityError:
                    s.rollback()
                    flash("That customer name is already in use.", "error")
                    return render_template("customer_form.html", mode="new", form=request.form)

                return redirect(url_for("customer_view", customer_id=c.id))

        return render_template("customer_form.html", mode="new")

    @app.route("/customers/<int:customer_id>/edit", methods=["GET", "POST"])
    @login_required
    @subscription_required
    def customer_edit(customer_id: int):
        with db_session() as s:
            c = _customer_owned_or_404(s, customer_id)

            if request.method == "POST":
                name = (request.form.get("name") or "").strip()
                email = _normalize_email(request.form.get("email") or "").strip() or None
                phone = (request.form.get("phone") or "").strip() or None
                address = (request.form.get("address") or "").strip() or None

                if not name:
                    flash("Customer name is required.", "error")
                    return render_template("customer_form.html", mode="edit", c=c)

                # If renaming to an existing customer name for this user, MERGE instead of 500.
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

                # Normal update (no collision)
                c.name = name
                c.email = (email if (email and _looks_like_email(email)) else (email or None))
                c.phone = phone
                c.address = address

                try:
                    s.commit()
                except IntegrityError:
                    s.rollback()
                    flash("That customer name is already in use. Try a different name, or merge customers.", "error")
                    return render_template("customer_form.html", mode="edit", c=c)

                flash("Customer updated.", "success")
                return redirect(url_for("customer_view", customer_id=c.id))

        return render_template("customer_form.html", mode="edit", c=c)

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

            inv_q = (
                s.query(Invoice)
                .options(selectinload(Invoice.parts), selectinload(Invoice.labor_items))
                .filter(Invoice.user_id == uid)
                .filter(Invoice.customer_id == c.id)
                .order_by(Invoice.created_at.desc())
            )

            if year.isdigit() and len(year) == 4:
                inv_q = inv_q.filter(Invoice.invoice_number.startswith(year))

            invoices_list = inv_q.all()

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

        return render_template(
            "customer_view.html",
            c=c,
            invoices=invoices_list,
            year=year,
            status=status or "all"
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
                .order_by(Invoice.created_at.desc())
            )

            if q:
                like = f"%{q}%"
                invoices_q = invoices_q.filter(
                    (Invoice.name.ilike(like)) |
                    (Invoice.vehicle.ilike(like))
                )

            if year.isdigit() and len(year) == 4:
                invoices_q = invoices_q.filter(Invoice.invoice_number.startswith(year))

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
    # Create invoice — GATED
    # -----------------------------
    @app.route("/invoices/new", methods=["GET", "POST"])
    @login_required
    @subscription_required
    def invoice_new():
        uid = _current_user_id_int()
        pre_customer_id = (request.args.get("customer_id") or "").strip()

        with db_session() as s:
            u = s.get(User, uid)
            user_template_key = _template_key_fallback(getattr(u, "invoice_template", None) if u else None)
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
                    form=request.form,
                    tmpl=tmpl,
                    tmpl_key=user_template_key,
                    customers=customers,
                    customers_for_js=customers_for_js,
                    pre_customer=pre_customer,
                )

            customer_id = int(customer_id_raw)

            if user_template_key == "auto_repair" and not vehicle:
                flash("Vehicle is required for Auto Repair invoices.", "error")
                return render_template(
                    "invoice_form.html",
                    mode="new",
                    form=request.form,
                    tmpl=tmpl,
                    tmpl_key=user_template_key,
                    customers=customers,
                    customers_for_js=customers_for_js,
                    pre_customer=pre_customer,
                )

            with db_session() as s:
                c = _customer_owned_or_404(s, customer_id)

                year = int(datetime.now().strftime("%Y"))
                inv_no = next_invoice_number(s, year, Config.INVOICE_SEQ_WIDTH)

                cust_email_override = (request.form.get("customer_email") or "").strip() or None
                cust_phone_override = (request.form.get("customer_phone") or "").strip() or None

                inv = Invoice(
                    user_id=uid,
                    customer_id=c.id,

                    invoice_number=inv_no,
                    invoice_template=user_template_key,

                    # ✅ autofill fallback
                    customer_email=(cust_email_override or (c.email or None)),
                    customer_phone=(cust_phone_override or (c.phone or None)),

                    name=(c.name or "").strip(),
                    vehicle=vehicle,

                    hours=_to_float(request.form.get("hours")),
                    price_per_hour=_to_float(request.form.get("price_per_hour")),
                    shop_supplies=_to_float(request.form.get("shop_supplies")),
                    paid=_to_float(request.form.get("paid")),
                    date_in=request.form.get("date_in", "").strip(),
                    notes=request.form.get("notes", "").rstrip(),
                )

                for pn, pp in _parse_repeating_fields(
                    request.form.getlist("part_name"),
                    request.form.getlist("part_price")
                ):
                    inv.parts.append(InvoicePart(part_name=pn, part_price=pp))

                for desc, t in _parse_repeating_fields(
                    request.form.getlist("labor_desc"),
                    request.form.getlist("labor_time_hours")
                ):
                    inv.labor_items.append(InvoiceLabor(labor_desc=desc, labor_time_hours=t))

                s.add(inv)
                s.commit()

                return redirect(url_for("invoice_view", invoice_id=inv.id))

        return render_template(
            "invoice_form.html",
            mode="new",
            default_date=datetime.now().strftime("%m/%d/%Y"),
            tmpl=tmpl,
            tmpl_key=user_template_key,
            customers=customers,
            customers_for_js=customers_for_js,
            pre_customer=pre_customer,
        )

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

            year = int(datetime.now().strftime("%Y"))
            new_no = next_invoice_number(s, year, Config.INVOICE_SEQ_WIDTH)

            new_inv = Invoice(
                user_id=uid,
                customer_id=inv.customer_id,
                invoice_number=new_no,
                invoice_template=inv.invoice_template,

                name=inv.name,
                vehicle=inv.vehicle,

                hours=inv.hours,
                price_per_hour=inv.price_per_hour,
                shop_supplies=inv.shop_supplies,

                notes=inv.notes,
                paid=0.0,
                date_in=datetime.now().strftime("%m/%d/%Y"),

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

            flash(f"Duplicated invoice as {new_no}.", "success")
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
                        form=request.form,
                        tmpl=tmpl,
                        tmpl_key=tmpl_key,
                        customers=customers,
                        customers_for_js=customers_for_js,
                    )

                customer_id = int(customer_id_raw)
                c = _customer_owned_or_404(s, customer_id)

                inv.customer_id = customer_id
                inv.name = (c.name or "").strip()

                inv.vehicle = (request.form.get("vehicle") or "").strip()
                inv.hours = _to_float(request.form.get("hours"))
                inv.price_per_hour = _to_float(request.form.get("price_per_hour"))
                inv.shop_supplies = _to_float(request.form.get("shop_supplies"))
                inv.paid = _to_float(request.form.get("paid"))
                inv.date_in = request.form.get("date_in", "").strip()
                inv.notes = request.form.get("notes", "").rstrip()

                cust_email_override = (request.form.get("customer_email") or "").strip() or None
                cust_phone_override = (request.form.get("customer_phone") or "").strip() or None

                # ✅ autofill fallback
                inv.customer_email = cust_email_override or (c.email or None)
                inv.customer_phone = cust_phone_override or (c.phone or None)

                if tmpl_key == "auto_repair" and not inv.vehicle:
                    flash("Vehicle is required for Auto Repair invoices.", "error")
                    return render_template(
                        "invoice_form.html",
                        mode="edit",
                        inv=inv,
                        form=request.form,
                        tmpl=tmpl,
                        tmpl_key=tmpl_key,
                        customers=customers,
                        customers_for_js=customers_for_js,
                    )

                inv.parts.clear()
                inv.labor_items.clear()

                for pn, pp in _parse_repeating_fields(
                    request.form.getlist("part_name"),
                    request.form.getlist("part_price")
                ):
                    inv.parts.append(InvoicePart(part_name=pn, part_price=pp))

                for desc, t in _parse_repeating_fields(
                    request.form.getlist("labor_desc"),
                    request.form.getlist("labor_time_hours")
                ):
                    inv.labor_items.append(InvoiceLabor(labor_desc=desc, labor_time_hours=t))

                s.commit()
                return redirect(url_for("invoice_view", invoice_id=inv.id))

        return render_template(
            "invoice_form.html",
            mode="edit",
            inv=inv,
            tmpl=tmpl,
            tmpl_key=tmpl_key,
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
        if not (year_text.isdigit() and len(year_text) == 4):
            year_text = datetime.now().strftime("%Y")
        target_year = int(year_text)

        EPS = 0.01
        uid = _current_user_id_int()

        count = 0
        total_invoice_amount = 0.0
        total_labor = 0.0
        total_parts = 0.0
        total_supplies = 0.0

        total_paid_invoices_amount = 0.0
        total_outstanding_unpaid = 0.0
        labor_unpaid = 0.0

        unpaid = []

        with db_session() as s:
            invs = (
                s.query(Invoice)
                .options(selectinload(Invoice.parts), selectinload(Invoice.labor_items))
                .filter(Invoice.user_id == uid)
                .order_by(Invoice.created_at.desc())
                .all()
            )

            for inv in invs:
                yr = _parse_year_from_datein(inv.date_in)
                if yr != target_year:
                    continue

                parts_total = inv.parts_total()
                labor_total = inv.labor_total()
                invoice_total = inv.invoice_total()
                supplies = float(inv.shop_supplies or 0.0)
                paid = float(inv.paid or 0.0)

                total_parts += parts_total
                total_labor += labor_total
                total_supplies += supplies
                total_invoice_amount += invoice_total
                count += 1

                fully_paid = (paid + EPS >= invoice_total)

                if fully_paid:
                    total_paid_invoices_amount += invoice_total
                else:
                    outstanding = max(0.0, invoice_total - paid)
                    total_outstanding_unpaid += outstanding
                    labor_unpaid += labor_total

                    unpaid.append({
                        "id": inv.id,
                        "invoice_number": inv.invoice_number,
                        "name": inv.name,
                        "vehicle": inv.vehicle,
                        "date_in": inv.date_in,
                        "outstanding": outstanding,
                    })

        profit_paid_labor_only = total_labor - labor_unpaid

        context = {
            "year": year_text,
            "count": count,
            "total_invoice_amount": total_invoice_amount,
            "total_parts": total_parts,
            "total_labor": total_labor,
            "total_supplies": total_supplies,
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
            inv = _invoice_owned_or_404(s, invoice_id)
            if not inv.pdf_path or not os.path.exists(inv.pdf_path):
                flash("PDF not found. Generate it first.", "error")
                return redirect(url_for("invoice_view", invoice_id=invoice_id))

            return send_file(
                inv.pdf_path,
                as_attachment=True,
                download_name=os.path.basename(inv.pdf_path),
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

            subject = f"Invoice {inv.invoice_number}"
            body = (
                f"Hello {customer_name or 'there'},\n\n"
                f"Attached is your invoice {inv.invoice_number}.\n"
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
                print(f"[INVOICE SEND] SMTP ERROR to={to_email} inv={inv.invoice_number}: {repr(e)}", flush=True)
                flash("Could not send email (SMTP / sender config issue). Check Render logs.", "error")
                return redirect(url_for("invoice_view", invoice_id=invoice_id))

        flash("Invoice email sent.", "success")
        return redirect(url_for("invoice_view", invoice_id=invoice_id))

    @app.route("/pdfs/download_all")
    @login_required
    @subscription_required
    def pdfs_download_all():
        year = (request.args.get("year") or "").strip()
        uid = _current_user_id_int()

        with db_session() as s:
            q = (
                s.query(Invoice)
                .filter(Invoice.user_id == uid)
                .filter(Invoice.pdf_path.isnot(None))
            )
            if year.isdigit() and len(year) == 4:
                q = q.filter(Invoice.invoice_number.startswith(year))
            invoices = q.all()

        mem = io.BytesIO()
        with zipfile.ZipFile(mem, "w", zipfile.ZIP_DEFLATED) as z:
            for inv in invoices:
                if inv.pdf_path and os.path.exists(inv.pdf_path):
                    z.write(inv.pdf_path, arcname=os.path.basename(inv.pdf_path))

        mem.seek(0)
        return send_file(mem, as_attachment=True, download_name="invoices_pdfs.zip")

    return app


if __name__ == "__main__":
    app = create_app()
    app.run(debug=True, port=5001)

