
# app.py
import os
import re
import io
import zipfile
import smtplib
from email.message import EmailMessage
from datetime import datetime
from pathlib import Path

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
from werkzeug.security import generate_password_hash, check_password_hash
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired

from config import Config
from models import (
    Base, make_engine, make_session_factory,
    User, Invoice, InvoicePart, InvoiceLabor, next_invoice_number
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


def _normalize_email(email: str) -> str:
    return (email or "").strip().lower()


def _looks_like_email(email: str) -> bool:
    e = _normalize_email(email)
    # simple sanity check (you can tighten later)
    return bool(e) and ("@" in e) and ("." in e.split("@")[-1])


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
    """
    SMTP email sender. Configure via env vars (Render):
      SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS, MAIL_FROM
    """
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
    """
    Works for Postgres + SQLite.
    """
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
    """
    Works for Postgres + SQLite:
    - Postgres: information_schema.columns
    - SQLite: PRAGMA table_info(table)
    """
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
    """
    Legacy helper:
    If an old DB already has invoices but not invoices.user_id, add it.
    """
    if not _table_exists(engine, "invoices"):
        return

    if not _column_exists(engine, "invoices", "user_id"):
        with engine.begin() as conn:
            try:
                conn.execute(text("ALTER TABLE invoices ADD COLUMN IF NOT EXISTS user_id INTEGER"))
            except Exception:
                conn.execute(text("ALTER TABLE invoices ADD COLUMN user_id INTEGER"))


def _migrate_user_profile_fields(engine):
    """
    Adds per-user business header fields used in PDFs:
      users.business_name, users.phone, users.address
    """
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
    """
    Adds users.email for password reset and uniqueness.
    """
    if not _table_exists(engine, "users"):
        return

    if not _column_exists(engine, "users", "email"):
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE users ADD COLUMN email VARCHAR(255)"))

    # Unique index on lower(email)
    try:
        with engine.begin() as conn:
            conn.execute(text("CREATE UNIQUE INDEX IF NOT EXISTS users_email_unique ON users (LOWER(email))"))
    except Exception:
        pass


def _migrate_invoice_contact_fields(engine):
    """
    Adds invoices.customer_email and invoices.customer_phone.
    """
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


# -----------------------------
# App factory
# -----------------------------
def create_app():
    _ensure_dirs()

    app = Flask(__name__)
    app.config.from_object(Config)

    # Defaults for password reset + SMTP (can be overridden in Config or Render env vars)
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

    # ✅ Always create tables first
    Base.metadata.create_all(bind=engine)

    # ✅ Then run any legacy “add column” migrations safely
    _migrate_add_user_id(engine)
    _migrate_user_profile_fields(engine)
    _migrate_user_email(engine)
    _migrate_invoice_contact_fields(engine)

    SessionLocal = make_session_factory(engine)

    def db_session():
        return SessionLocal()

    # Now that SessionLocal exists, bind the user_loader properly.
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

    # Ensure at least one user exists (so legacy installs can log in immediately)
    def _bootstrap_first_user():
        username = os.getenv("INITIAL_ADMIN_USERNAME", "admin")
        password = os.getenv("INITIAL_ADMIN_PASSWORD", os.getenv("ADMIN_PASSWORD", "changeme"))

        # Only use INITIAL_ADMIN_EMAIL if explicitly set.
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

            # No users exist -> create initial admin
            if not _looks_like_email(email):
                email = "no-reply@placeholder.local"

            u = User(username=username, email=email, password_hash=generate_password_hash(password))
            s.add(u)
            s.commit()

            # Backfill legacy invoices
            s.query(Invoice).filter(Invoice.user_id.is_(None)).update({"user_id": u.id})
            s.commit()

    _bootstrap_first_user()

    # -----------------------------
    # Auth routes
    # -----------------------------
    @app.route("/login", methods=["GET", "POST"])
    def login():
        if current_user.is_authenticated:
            return redirect(url_for("invoices"))

        if request.method == "POST":
            username = (request.form.get("username") or "").strip()
            password = request.form.get("password") or ""
            with db_session() as s:
                u = s.query(User).filter(User.username == username).first()
                if u and check_password_hash(u.password_hash, password):
                    login_user(AppUser(u.id, u.username))
                    return redirect(url_for("invoices"))

            flash("Invalid username or password.", "error")
        return render_template("login.html")

    @app.route("/register", methods=["GET", "POST"])
    def register():
        if current_user.is_authenticated:
            return redirect(url_for("invoices"))

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
                return redirect(url_for("invoices"))

        return render_template("register.html")

    @app.route("/forgot-password", methods=["GET", "POST"])
    def forgot_password():
        if current_user.is_authenticated:
            return redirect(url_for("invoices"))

        if request.method == "POST":
            email = _normalize_email(request.form.get("email") or "")

            # Always same response to prevent account enumeration
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
                            print(f"[RESET] Sent reset email to {email}")
                        except Exception as e:
                            # Keep UX identical even if mail fails; log in production if desired.
                            print(f"[RESET] SMTP ERROR for {email}: {repr(e)}")

            return redirect(url_for("login"))

        return render_template("forgot_password.html")

    @app.route("/reset-password/<token>", methods=["GET", "POST"])
    def reset_password(token: str):
        if current_user.is_authenticated:
            return redirect(url_for("invoices"))

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
    # User Settings (business header for PDFs + email)
    # -----------------------------
    @app.route("/settings", methods=["GET", "POST"])
    @login_required
    def settings():
        with db_session() as s:
            u = s.get(User, _current_user_id_int())
            if not u:
                abort(404)

            if request.method == "POST":
                # Email (required)
                new_email = _normalize_email(request.form.get("email") or "")
                if not _looks_like_email(new_email):
                    flash("Please enter a valid email address.", "error")
                    return render_template("settings.html", u=u)

                # If email changed, enforce case-insensitive uniqueness
                if (u.email or "").strip().lower() != new_email:
                    taken_email = (
                        s.query(User)
                        .filter(text("lower(email) = :e AND id != :id"))
                        .params(e=new_email, id=u.id)
                        .first()
                    )
                    if taken_email:
                        flash("That email is already in use.", "error")
                        return render_template("settings.html", u=u)

                    u.email = new_email

                # Existing fields
                u.business_name = (request.form.get("business_name") or "").strip() or None
                u.phone = (request.form.get("phone") or "").strip() or None
                u.address = (request.form.get("address") or "").strip() or None

                s.commit()
                flash("Settings saved.", "success")
                return redirect(url_for("settings"))

            return render_template("settings.html", u=u)

    # -----------------------------
    # Index
    # -----------------------------
    @app.route("/")
    def index():
        return redirect(url_for("invoices" if current_user.is_authenticated else "login"))

    # -----------------------------
    # Invoice list (scoped to user)
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
                .options(
                    selectinload(Invoice.parts),
                    selectinload(Invoice.labor_items),
                )
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
            q=q,
            year=year,
            status=status or "all"
        )

    # -----------------------------
    # Create invoice (owned by user)
    # -----------------------------
    @app.route("/invoices/new", methods=["GET", "POST"])
    @login_required
    def invoice_new():
        if request.method == "POST":
            name = request.form.get("name", "").strip()
            vehicle = request.form.get("vehicle", "").strip()

            if not name or not vehicle:
                flash("Name and Vehicle are required.", "error")
                return render_template("invoice_form.html", mode="new", form=request.form)

            with db_session() as s:
                year = int(datetime.now().strftime("%Y"))
                inv_no = next_invoice_number(s, year, Config.INVOICE_SEQ_WIDTH)

                inv = Invoice(
                    user_id=_current_user_id_int(),
                    invoice_number=inv_no,
                    customer_email=(request.form.get("customer_email") or "").strip() or None,
                    customer_phone=(request.form.get("customer_phone") or "").strip() or None,
                    name=name,
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
            default_date=datetime.now().strftime("%m/%d/%Y")
        )

    # -----------------------------
    # View invoice (scoped)
    # -----------------------------
    @app.route("/invoices/<int:invoice_id>")
    @login_required
    def invoice_view(invoice_id):
        with db_session() as s:
            inv = _invoice_owned_or_404(s, invoice_id)
        return render_template("invoice_view.html", inv=inv)

    # -----------------------------
    # Edit invoice (scoped)
    # -----------------------------
    @app.route("/invoices/<int:invoice_id>/edit", methods=["GET", "POST"])
    @login_required
    def invoice_edit(invoice_id):
        with db_session() as s:
            inv = _invoice_owned_or_404(s, invoice_id)

            if request.method == "POST":
                inv.name = request.form.get("name", "").strip()
                inv.vehicle = request.form.get("vehicle", "").strip()
                inv.hours = _to_float(request.form.get("hours"))
                inv.price_per_hour = _to_float(request.form.get("price_per_hour"))
                inv.shop_supplies = _to_float(request.form.get("shop_supplies"))
                inv.paid = _to_float(request.form.get("paid"))
                inv.date_in = request.form.get("date_in", "").strip()
                inv.notes = request.form.get("notes", "").rstrip()
                inv.customer_email = (request.form.get("customer_email") or "").strip() or None
                inv.customer_phone = (request.form.get("customer_phone") or "").strip() or None

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

        return render_template("invoice_form.html", mode="edit", inv=inv)

    # -----------------------------
    # Year Summary (scoped)
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
    # Delete invoice (scoped)
    # -----------------------------
    @app.route("/invoices/<int:invoice_id>/delete", methods=["POST"])
    @login_required
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
        return redirect(url_for("invoices"))

    # -----------------------------
    # Mark invoice as paid (scoped)
    # -----------------------------
    @app.route("/invoices/<int:invoice_id>/mark_paid", methods=["POST"])
    @login_required
    def invoice_mark_paid(invoice_id: int):
        with db_session() as s:
            inv = _invoice_owned_or_404(s, invoice_id)
            inv.paid = inv.invoice_total()
            s.commit()

        flash("Invoice marked as paid.", "success")
        return redirect(url_for("invoice_view", invoice_id=invoice_id))

    # -----------------------------
    # PDF routes (scoped)
    # -----------------------------
    @app.route("/invoices/<int:invoice_id>/pdf/generate", methods=["POST"])
    @login_required
    def invoice_pdf_generate(invoice_id):
        with db_session() as s:
            _invoice_owned_or_404(s, invoice_id)
            generate_and_store_pdf(s, invoice_id)
        return redirect(url_for("invoice_view", invoice_id=invoice_id))

    @app.route("/invoices/<int:invoice_id>/pdf/download")
    @login_required
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
    def invoice_send(invoice_id: int):
        with db_session() as s:
            inv = _invoice_owned_or_404(s, invoice_id)

            to_email = (inv.customer_email or "").strip().lower()
            if not to_email or "@" not in to_email:
                flash("Customer email is missing. Add it on the invoice edit page first.", "error")
                return redirect(url_for("invoice_view", invoice_id=invoice_id))

            # Ensure PDF exists; generate if missing
            if (not inv.pdf_path) or (not os.path.exists(inv.pdf_path)):
                generate_and_store_pdf(s, invoice_id)
                inv = _invoice_owned_or_404(s, invoice_id)

            subject = f"Invoice {inv.invoice_number}"
            body = (
                f"Hello {inv.name},\n\n"
                f"Attached is your invoice {inv.invoice_number}.\n"
                f"Vehicle: {inv.vehicle}\n"
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
                print(f"[INVOICE SEND] SMTP ERROR to={to_email} inv={inv.invoice_number}: {repr(e)}")
                flash("Could not send email (SMTP / sender config issue). Check Render logs.", "error")
                return redirect(url_for("invoice_view", invoice_id=invoice_id))

        flash("Invoice email sent.", "success")
        return redirect(url_for("invoice_view", invoice_id=invoice_id))

    @app.route("/pdfs/download_all")
    @login_required
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
    app.run(debug=True)




