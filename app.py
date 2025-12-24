
# app.py
import os
import re
import io
import zipfile
from datetime import datetime
from pathlib import Path

from flask import (
    Flask, render_template, request, redirect, url_for,
    flash, send_file, abort
)
from flask_login import (
    LoginManager, UserMixin, login_user, logout_user,
    login_required, current_user
)
from sqlalchemy import text
from sqlalchemy.orm import selectinload
from werkzeug.security import generate_password_hash, check_password_hash

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


# -----------------------------
# DB migration (lightweight)
# -----------------------------
def _column_exists(engine, table_name: str, column_name: str) -> bool:
    # Works for SQLite/Postgres generally via INFORMATION_SCHEMA-ish fallbacks.
    # For SQLite: PRAGMA table_info(table)
    try:
        with engine.connect() as conn:
            rows = conn.execute(text(f"PRAGMA table_info({table_name})")).fetchall()
        return any(r[1] == column_name for r in rows)
    except Exception:
        return False


def _migrate_add_user_id(engine):
    # Add invoices.user_id if missing (legacy DB)
    if not _column_exists(engine, "invoices", "user_id"):
        with engine.begin() as conn:
            conn.execute(text("ALTER TABLE invoices ADD COLUMN user_id INTEGER"))
    # Create users table if missing is handled by metadata.create_all()


# -----------------------------
# App factory
# -----------------------------
def create_app():
    _ensure_dirs()

    app = Flask(__name__)
    app.config.from_object(Config)

    login_manager.init_app(app)

    engine = make_engine(Config.SQLALCHEMY_DATABASE_URI, echo=Config.SQLALCHEMY_ECHO)

    # Lightweight migration BEFORE create_all is still okay; create_all will create missing tables.
    _migrate_add_user_id(engine)
    Base.metadata.create_all(engine)

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
    # If there are no users, create an initial admin user from env vars.
    def _bootstrap_first_user():
        username = os.getenv("INITIAL_ADMIN_USERNAME", "admin")
        password = os.getenv("INITIAL_ADMIN_PASSWORD", os.getenv("ADMIN_PASSWORD", "changeme"))
        with db_session() as s:
            exists = s.query(User).first()
            if exists:
                return
            u = User(username=username, password_hash=generate_password_hash(password))
            s.add(u)
            s.commit()

            # Backfill legacy invoices to this first user (so you don't "lose" old invoices)
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
            password = request.form.get("password") or ""
            confirm = request.form.get("confirm") or ""

            if not username or len(username) < 3:
                flash("Username must be at least 3 characters.", "error")
                return render_template("register.html")

            if not password or len(password) < 6:
                flash("Password must be at least 6 characters.", "error")
                return render_template("register.html")

            if password != confirm:
                flash("Passwords do not match.", "error")
                return render_template("register.html")

            with db_session() as s:
                taken = s.query(User).filter(User.username == username).first()
                if taken:
                    flash("That username is already taken.", "error")
                    return render_template("register.html")

                u = User(username=username, password_hash=generate_password_hash(password))
                s.add(u)
                s.commit()

                login_user(AppUser(u.id, u.username))
                return redirect(url_for("invoices"))

        return render_template("register.html")

    @app.route("/logout")
    @login_required
    def logout():
        logout_user()
        return redirect(url_for("login"))

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


