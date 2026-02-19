# pdf_service.py
import os
import re
import io
import json
from datetime import datetime, timedelta
from pathlib import Path

from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.lib.utils import ImageReader

from config import Config
from models import Invoice, User, Customer, InvoiceDesignTemplate


def _money(x) -> str:
    try:
        return f"${float(x):,.2f}"
    except Exception:
        return f"${x}"


def _safe_filename(name: str) -> str:
    # strip characters not allowed on Windows/mac paths
    return re.sub(r'[\\/*?:"<>|]', "", (name or "")).strip() or "Invoice"


def _format_phone(phone: str | None) -> str:
    raw = (phone or "").strip()
    if not raw:
        return ""
    digits = re.sub(r"\D", "", raw)
    if len(digits) == 10:
        return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
    if len(digits) == 11 and digits.startswith("1"):
        return f"+1 ({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
    return re.sub(r"\)\s+", ") ", raw)


def _city_state_postal_line(city: str | None, state: str | None, postal_code: str | None) -> str:
    city_val = (city or "").strip()
    state_val = (state or "").strip().upper()
    postal_val = (postal_code or "").strip()
    city_state = ", ".join([p for p in [city_val, state_val] if p])
    if city_state and postal_val:
        return f"{city_state} {postal_val}"
    return city_state or postal_val


def _owner_address_lines(owner: User | None) -> list[str]:
    if not owner:
        return []

    line1 = (getattr(owner, "address_line1", None) or "").strip()
    line2 = (getattr(owner, "address_line2", None) or "").strip()
    city = (getattr(owner, "city", None) or "").strip()
    state = (getattr(owner, "state", None) or "").strip()
    postal_code = (getattr(owner, "postal_code", None) or "").strip()
    city_line = _city_state_postal_line(city, state, postal_code)
    if line1 or line2 or city_line:
        street_line = ", ".join([p for p in [line1, line2] if p])
        return [p for p in [street_line, city_line] if p]

    legacy = (getattr(owner, "address", None) or "").strip()
    if not legacy:
        return []
    lines = [ln.strip() for ln in re.split(r"[\r\n]+", legacy) if ln.strip()]
    return lines or [legacy]


def _invoice_due_date_line(inv: Invoice, owner: User | None, *, is_estimate: bool) -> str:
    if is_estimate:
        return ""
    if float(inv.amount_due() or 0.0) <= 0.0:
        return ""
    raw_due_days = getattr(owner, "payment_due_days", None) if owner else None
    due_days = 30 if raw_due_days is None else int(raw_due_days)
    due_days = max(0, min(3650, due_days))
    created_at = getattr(inv, "created_at", None) or datetime.utcnow()
    due_dt = created_at + timedelta(days=due_days)
    return f"Payment due date: {due_dt.strftime('%B %d, %Y')}"


def _tax_label(inv: Invoice) -> str:
    if getattr(inv, "tax_override", None) is not None:
        return "Tax"
    rate = float(getattr(inv, "tax_rate", 0.0) or 0.0)
    if rate:
        return f"Tax ({rate:g}%)"
    return "Tax"


def _wrap_text(text, font, size, max_width):
    words = str(text).split()
    lines = []
    current = ""

    def split_long_token(token: str):
        """Break a single long token (like an email) into width-safe chunks."""
        if stringWidth(token, font, size) <= max_width:
            return [token]
        chunks = []
        remaining = token
        while remaining:
            lo, hi = 1, len(remaining)
            fit = 1
            while lo <= hi:
                mid = (lo + hi) // 2
                piece = remaining[:mid]
                if stringWidth(piece, font, size) <= max_width:
                    fit = mid
                    lo = mid + 1
                else:
                    hi = mid - 1
            chunks.append(remaining[:fit])
            remaining = remaining[fit:]
        return chunks

    expanded_words = []
    for w in words:
        expanded_words.extend(split_long_token(w))

    for w in expanded_words:
        test = current + (" " if current else "") + w
        if stringWidth(test, font, size) <= max_width:
            current = test
        else:
            if current:
                lines.append(current)
            current = w
    if current:
        lines.append(current)
    return lines or [""]


def _split_notes_into_lines(notes_text: str, max_width, font="Helvetica", size=10):
    """
    Notes are stored as plain text in DB. We:
    - split into lines
    - wrap each line to fit the box width
    - keep a small spacer between original lines
    """
    raw = (notes_text or "").strip()
    if not raw:
        return []

    out = []
    for ln in raw.splitlines():
        ln = ln.strip()
        if not ln:
            continue
        out.extend(_wrap_text(ln, font, size, max_width))
        out.append("__SPACER__")
    while out and out[-1] == "__SPACER__":
        out.pop()
    return out


def _pdf_template_key_fallback(key: str | None) -> str:
    key = (key or "").strip().lower()
    return key if key in {"classic", "modern", "split_panel", "strip"} else "classic"


def _builder_template_vars(inv: Invoice, owner: User | None, customer: Customer | None, *, is_estimate: bool) -> dict[str, str]:
    due_line = _invoice_due_date_line(inv, owner, is_estimate=is_estimate)
    due_date = ""
    if due_line:
        due_date = due_line.replace("Payment due date:", "").strip()
    business_name = (getattr(owner, "business_name", None) or getattr(owner, "username", None) or "InvoiceRunner").strip()
    customer_name = (getattr(customer, "name", None) or inv.name or "").strip()
    customer_email = (getattr(inv, "customer_email", None) or getattr(customer, "email", None) or "").strip()
    customer_phone = _format_phone((getattr(inv, "customer_phone", None) or getattr(customer, "phone", None) or "").strip())
    rate_val = float(getattr(inv, "price_per_hour", 0.0) or 0.0)
    labor_lines: list[str] = []
    for li in getattr(inv, "labor_items", []) or []:
        try:
            t = float(li.labor_time_hours or 0.0)
        except Exception:
            t = 0.0
        line_total = t * rate_val
        desc = (li.labor_desc or "").strip() or "Labor Item"
        if t > 0:
            labor_lines.append(f"{desc} - {t:g} hr - {_money(line_total)}")
        elif line_total:
            labor_lines.append(f"{desc} - {_money(line_total)}")
        else:
            labor_lines.append(desc)
    if not labor_lines and (getattr(inv, "hours", 0.0) or 0.0):
        h = float(getattr(inv, "hours", 0.0) or 0.0)
        labor_lines.append(f"Labor - {h:g} hr - {_money(h * rate_val)}")

    parts_lines: list[str] = []
    for p in getattr(inv, "parts", []) or []:
        name = (p.part_name or "").strip() or "Part"
        price = float(p.part_price or 0.0)
        if price:
            parts_lines.append(f"{name} - {_money(inv.part_price_with_markup(price))}")
        else:
            parts_lines.append(name)

    notes_text = (getattr(inv, "notes", None) or "").strip()
    return {
        "doc_label": "ESTIMATE" if is_estimate else "INVOICE",
        "invoice_number": str(getattr(inv, "display_number", None) or inv.invoice_number or ""),
        "date": str(inv.date_in or ""),
        "due_date": due_date,
        "business_name": business_name,
        "customer_name": customer_name,
        "customer_email": customer_email,
        "customer_phone": customer_phone,
        "job": str(getattr(inv, "vehicle", None) or ""),
        "rate": _money(rate_val),
        "hours": str(getattr(inv, "hours", 0.0) or 0.0),
        "labor_lines": "\n".join(labor_lines),
        "parts_lines": "\n".join(parts_lines),
        "notes_text": notes_text,
        "parts_total": _money(inv.parts_total()),
        "labor_total": _money(inv.labor_total()),
        "tax": _money(inv.tax_amount()),
        "total": _money(inv.invoice_total()),
        "paid": _money(getattr(inv, "paid", 0.0) or 0.0),
        "amount_due": _money(inv.amount_due()),
    }


def _render_invoice_builder_pdf(
    *,
    session,
    inv: Invoice,
    owner: User | None,
    customer: Customer | None,
    pdf_path: str,
    generated_dt: datetime,
    is_estimate: bool,
    design_obj: dict,
) -> str:
    PAGE_W, PAGE_H = LETTER
    pdf = canvas.Canvas(pdf_path, pagesize=LETTER)
    display_no = getattr(inv, "display_number", None) or inv.invoice_number
    doc_label = "Estimate" if is_estimate else "Invoice"
    pdf.setTitle(f"{doc_label} - {display_no}")

    canvas_cfg = design_obj.get("canvas") if isinstance(design_obj, dict) else {}
    canvas_w = float((canvas_cfg or {}).get("width") or 816.0)
    canvas_h = float((canvas_cfg or {}).get("height") or 1056.0)
    canvas_bg = str((canvas_cfg or {}).get("bg") or "#ffffff")
    if not re.fullmatch(r"#[0-9a-fA-F]{6}", canvas_bg):
        canvas_bg = "#ffffff"
    scale_x = PAGE_W / max(1.0, canvas_w)
    scale_y = PAGE_H / max(1.0, canvas_h)

    pdf.setFillColor(colors.HexColor(canvas_bg))
    pdf.rect(0, 0, PAGE_W, PAGE_H, stroke=0, fill=1)

    vars_map = _builder_template_vars(inv, owner, customer, is_estimate=is_estimate)
    elements = design_obj.get("elements") if isinstance(design_obj, dict) else []
    if not isinstance(elements, list):
        elements = []

    def _map_y(y: float, h: float) -> float:
        return PAGE_H - ((y + h) * scale_y)

    for el in elements:
        if not isinstance(el, dict):
            continue
        x = float(el.get("x") or 0.0)
        y = float(el.get("y") or 0.0)
        w = max(10.0, float(el.get("w") or 10.0))
        h = max(10.0, float(el.get("h") or 10.0))
        rx = x * scale_x
        ry = _map_y(y, h)
        rw = w * scale_x
        rh = h * scale_y
        etype = str(el.get("type") or "text").strip().lower()

        border_color = str(el.get("borderColor") or "transparent")
        fill_color = str(el.get("fillColor") or "transparent")
        text_color = str(el.get("color") or "#111827")
        radius = float(el.get("radius") or 0.0) * min(scale_x, scale_y)

        if etype == "box":
            has_fill = re.fullmatch(r"#[0-9a-fA-F]{6}", fill_color or "") is not None
            has_stroke = re.fullmatch(r"#[0-9a-fA-F]{6}", border_color or "") is not None
            pdf.setFillColor(colors.HexColor(fill_color if has_fill else "#ffffff"))
            pdf.setStrokeColor(colors.HexColor(border_color if has_stroke else "#111827"))
            pdf.roundRect(rx, ry, rw, rh, max(0.0, radius), stroke=1 if has_stroke else 0, fill=1 if has_fill else 0)
            continue

        text_raw = str(el.get("text") or "")
        # Backward compatibility for older starter templates that hardcoded sample lines.
        text_raw = text_raw.replace(
            "LABOR\nOil change service - 1.0 hr - $100.00\nBrake inspection - 1.5 hr - $150.00\nLabor Total: {{labor_total}}",
            "LABOR\n{{labor_lines}}\nLabor Total: {{labor_total}}",
        )
        text_raw = text_raw.replace(
            "PARTS\nOil Filter - $18.99\nEngine Oil - $37.95\nAir Filter - $27.99\nParts Total: {{parts_total}}",
            "PARTS\n{{parts_lines}}\nParts Total: {{parts_total}}",
        )
        text_raw = text_raw.replace(
            "NOTES\nThank you for your business.\nRecommended: tire rotation in 5,000 miles.",
            "NOTES\n{{notes_text}}",
        )
        for k, v in vars_map.items():
            text_raw = text_raw.replace(f"{{{{{k}}}}}", str(v))
        font_size = max(7.0, min(64.0, float(el.get("fontSize") or 14.0))) * min(scale_x, scale_y)
        font_weight = int(el.get("fontWeight") or 500)
        font_name = "Helvetica-Bold" if font_weight >= 600 else "Helvetica"
        if re.fullmatch(r"#[0-9a-fA-F]{6}", text_color or ""):
            pdf.setFillColor(colors.HexColor(text_color))
        else:
            pdf.setFillColor(colors.black)
        pdf.setFont(font_name, font_size)
        line_h = max(8.0, font_size * 1.2)
        tx = rx + (6 * scale_x)
        ty = ry + rh - line_h
        max_w = rw - (12 * scale_x)
        for ln in str(text_raw).splitlines():
            for wrapped in _wrap_text(ln, font_name, font_size, max_w):
                if ty < ry + 2:
                    break
                pdf.drawString(tx, ty, wrapped)
                ty -= line_h

    pdf.save()
    inv.pdf_path = pdf_path
    inv.pdf_generated_at = generated_dt
    session.add(inv)
    session.commit()
    return pdf_path


def _render_modern_pdf(
    *,
    session,
    inv: Invoice,
    owner: User | None,
    customer: Customer | None,
    customer_email: str,
    customer_phone: str,
    customer_address: str,
    cfg: dict,
    template_key: str,
    pdf_path: str,
    display_no: str,
    doc_label: str,
    generated_dt: datetime,
    generated_str: str,
    owner_logo_abs: str,
    owner_logo_blob: bytes | None,
    is_estimate: bool,
    builder_cfg: dict | None = None,
):
    PAGE_W, PAGE_H = LETTER
    M = 0.65 * inch

    pdf = canvas.Canvas(pdf_path, pagesize=LETTER)
    pdf.setTitle(f"{doc_label.title()} - {display_no}")
    builder_enabled = bool(builder_cfg.get("enabled", False)) if isinstance(builder_cfg, dict) else False
    builder_accent = colors.HexColor(
        (builder_cfg.get("accent_color") if isinstance(builder_cfg, dict) else None) or "#0f172a"
    )
    builder_header_style = (
        (builder_cfg.get("header_style") if isinstance(builder_cfg, dict) else None) or "classic"
    ).strip().lower()
    builder_compact_mode = bool(builder_cfg.get("compact_mode", False)) if isinstance(builder_cfg, dict) else False

    brand_dark = colors.black
    brand_accent = colors.HexColor("#2563eb")
    brand_muted = colors.HexColor("#64748b")
    line_color = colors.HexColor("#e2e8f0")
    soft_bg = colors.HexColor("#f8fafc")
    if builder_enabled:
        brand_accent = builder_accent
        if builder_header_style == "banded":
            brand_dark = builder_accent

    def right_text(x, y, text, font="Helvetica", size=10, color=colors.black):
        pdf.setFont(font, size)
        pdf.setFillColor(color)
        w = pdf.stringWidth(str(text), font, size)
        pdf.drawString(x - w, y, str(text))

    def label_right_value(x_left, x_right, y, label, value, label_font=("Helvetica-Bold", 9), value_font=("Helvetica", 10)):
        pdf.setFont(*label_font)
        pdf.setFillColor(colors.black)
        pdf.drawString(x_left, y, label)
        right_text(x_right, y, str(value), value_font[0], value_font[1], colors.black)

    show_job = bool(cfg.get("show_job", True))
    show_labor = bool(cfg.get("show_labor", True))
    show_parts = bool(cfg.get("show_parts", True))
    show_shop_supplies = bool(cfg.get("show_shop_supplies", True))
    show_notes = bool(cfg.get("show_notes", True))

    header_h = 1.35 * inch
    pdf.setFillColor(brand_dark)
    pdf.rect(0, PAGE_H - header_h, PAGE_W, header_h, stroke=0, fill=1)

    # Optional logo
    logo_w = 0
    if owner_logo_blob:
        try:
            img = ImageReader(io.BytesIO(owner_logo_blob))
            iw, ih = img.getSize()
            max_h = 0.5 * inch
            max_w = 1.4 * inch
            scale = min(max_w / iw, max_h / ih)
            logo_w = iw * scale
            logo_h = ih * scale
            logo_x = M
            logo_y = PAGE_H - (header_h / 2) - (logo_h / 2)
            pdf.drawImage(img, logo_x, logo_y, width=logo_w, height=logo_h, mask="auto")
        except Exception:
            logo_w = 0
    elif owner_logo_abs and os.path.exists(owner_logo_abs):
        try:
            img = ImageReader(owner_logo_abs)
            iw, ih = img.getSize()
            max_h = 0.5 * inch
            max_w = 1.4 * inch
            scale = min(max_w / float(iw), max_h / float(ih))
            logo_w = float(iw) * scale
            logo_h = float(ih) * scale
            logo_x = M
            logo_y = PAGE_H - (header_h / 2) - (logo_h / 2)
            pdf.drawImage(img, logo_x, logo_y, width=logo_w, height=logo_h, mask="auto")
        except Exception:
            logo_w = 0

    business_name = (getattr(owner, "business_name", None) or "").strip() if owner else ""
    username = (getattr(owner, "username", None) or "").strip() if owner else ""
    header_name = business_name or username or ""
    header_address_lines = _owner_address_lines(owner)
    header_phone = (getattr(owner, "phone", None) or "").strip() if owner else ""

    left_x = M + (logo_w + 10 if logo_w else 0)
    pdf.setFillColor(colors.white)
    pdf.setFont("Helvetica-Bold", 14)
    pdf.drawString(left_x, PAGE_H - 0.55 * inch, header_name or "InvoiceRunner")

    pdf.setFont("Helvetica", 9)
    info_lines = []
    for addr_line in header_address_lines:
        info_lines.extend(_wrap_text(addr_line, "Helvetica", 9, 3.6 * inch))
    if header_phone:
        info_lines.append(header_phone)
    info_y = PAGE_H - 0.82 * inch
    for ln in info_lines[:2]:
        pdf.drawString(left_x, info_y, ln)
        info_y -= 12

    right_x = PAGE_W - M
    right_text(right_x, PAGE_H - 0.48 * inch, doc_label, "Helvetica-Bold", 18, colors.white)
    right_text(right_x, PAGE_H - 0.80 * inch, f"{doc_label.title()} #: {display_no}", "Helvetica", 10, colors.white)
    right_text(right_x, PAGE_H - 1.02 * inch, f"Date: {inv.date_in}", "Helvetica", 10, colors.white)
    due_line = _invoice_due_date_line(inv, owner, is_estimate=is_estimate)
    if due_line:
        right_text(right_x, PAGE_H - 1.24 * inch, due_line, "Helvetica", 9, colors.white)

    # Info cards
    top_y = PAGE_H - header_h - 0.35 * inch
    box_w = (PAGE_W - 2 * M - 0.35 * inch) / 2
    box_h = 1.6 * inch

    def draw_card(x, y, title):
        pdf.setFillColor(soft_bg)
        pdf.roundRect(x, y - box_h, box_w, box_h, 10, stroke=1, fill=1)
        pdf.setStrokeColor(line_color)
        pdf.setLineWidth(1)
        pdf.roundRect(x, y - box_h, box_w, box_h, 10, stroke=1, fill=0)
        pdf.setFont("Helvetica-Bold", 10)
        pdf.setFillColor(brand_muted)
        pdf.drawString(x + 12, y - 16, title)
        pdf.setFillColor(colors.black)

    # Customer name
    if customer and (getattr(customer, "name", None) or "").strip():
        customer_name = (customer.name or "").strip()
    else:
        nameFirst, _, _tail = (inv.name or "").partition(":")
        customer_name = (nameFirst or inv.name or "").strip()

    # Address formatting
    addr_lines = []
    addr_text = (customer_address or "").strip()
    if addr_text:
        addr_parts = [p.strip() for p in addr_text.split(",") if p.strip()]
        if len(addr_parts) >= 3:
            line1 = f"{', '.join(addr_parts[:-2])}"
            line2 = f"{addr_parts[-2]}, {addr_parts[-1]}"
        elif len(addr_parts) == 2:
            line1 = addr_parts[0]
            line2 = addr_parts[1]
        else:
            line1 = addr_text
            line2 = ""
        addr_lines.extend(_wrap_text(line1, "Helvetica", 10, box_w - 24))
        if line2:
            addr_lines.extend(_wrap_text(line2, "Helvetica", 10, box_w - 24))

    draw_card(M, top_y, "BILL TO")
    pdf.setFont("Helvetica-Bold", 10)
    pdf.drawString(M + 12, top_y - 36, customer_name)

    pdf.setFont("Helvetica", 10)
    y_cursor = top_y - 54
    if customer_phone:
        pdf.drawString(M + 12, y_cursor, f"Phone: {customer_phone}")
        y_cursor -= 14
    if customer_email:
        email_lines = _wrap_text(f"Email: {customer_email}", "Helvetica", 10, box_w - 24)
        for ln in email_lines[:2]:
            pdf.drawString(M + 12, y_cursor, ln)
            y_cursor -= 14
    for ln in addr_lines[:2]:
        pdf.drawString(M + 12, y_cursor, ln)
        y_cursor -= 14

    x2 = M + box_w + 0.35 * inch
    draw_card(x2, top_y, cfg.get("job_box_title", "DETAILS"))
    pdf.setFont("Helvetica", 10)
    job_text = f"{cfg['job_label']}: {inv.vehicle or ''}" if show_job else ""
    job_lines = _wrap_text(job_text, "Helvetica", 10, box_w - 24) if show_job else []
    y_job = top_y - 36
    for ln in job_lines[:2]:
        pdf.drawString(x2 + 12, y_job, ln)
        y_job -= 14

    if template_key == "flipping_items":
        pdf.drawString(x2 + 12, y_job, f"Profit: {_money(inv.labor_total())}")
        y_job -= 14
        pdf.drawString(x2 + 12, y_job, f"Sold For: {_money(inv.paid)}")
    else:
        pdf.drawString(x2 + 12, y_job, f"{cfg['job_rate_label']}: {_money(inv.price_per_hour)}")
        y_job -= 14
        pdf.drawString(
            x2 + 12,
            y_job,
            f"{cfg['job_hours_label']}: {inv.hours} {cfg.get('hours_suffix', 'hrs')}"
        )

    # Tables
    def draw_table(title, x, y_top, col_titles, rows, col_widths, money_cols=None):
        money_cols = set(money_cols or [])
        base_row_h = 14 if builder_compact_mode else 16
        title_gap = 18

        pdf.setFont("Helvetica-Bold", 12)
        pdf.setFillColor(colors.black)
        pdf.drawString(x, y_top, title)
        y = y_top - title_gap

        table_w = sum(col_widths)
        pdf.setFillColor(brand_dark)
        pdf.roundRect(x, y - base_row_h + 9, table_w, base_row_h + 2, 6, stroke=0, fill=1)
        pdf.setFont("Helvetica-Bold", 9)
        pdf.setFillColor(colors.white)

        cx = x
        for i, h in enumerate(col_titles):
            if i in money_cols:
                right_text(cx + col_widths[i] - 8, y + 1, h, "Helvetica-Bold", 9, colors.white)
            else:
                pdf.drawString(cx + 8, y + 1, h)
            cx += col_widths[i]

        y_cursor = y - base_row_h
        pdf.setFont("Helvetica", 10)
        pdf.setFillColor(colors.black)

        for row in rows:
            wrapped_cells = []
            row_height = base_row_h
            for i, cell in enumerate(row):
                max_w = col_widths[i] - 16
                lines = _wrap_text(cell, "Helvetica", 10, max_w)
                wrapped_cells.append(lines)
                row_height = max(row_height, len(lines) * base_row_h)

            cx = x
            for i, lines in enumerate(wrapped_cells):
                line_y = y_cursor
                for line in lines:
                    if i in money_cols:
                        right_text(cx + col_widths[i] - 8, line_y, line, "Helvetica", 10, colors.black)
                    else:
                        pdf.drawString(cx + 8, line_y, line)
                    line_y -= base_row_h
                cx += col_widths[i]

            pdf.setStrokeColor(line_color)
            pdf.setLineWidth(1)
            pdf.line(x, y_cursor - 4, x + table_w, y_cursor - 4)
            y_cursor -= row_height

        pdf.setStrokeColor(colors.black)
        return y_cursor - 8

    body_y = top_y - box_h - 0.45 * inch

    labor_rows = []
    rate = float(inv.price_per_hour or 0.0)
    for li in inv.labor_items:
        try:
            t = float(li.labor_time_hours or 0.0)
        except Exception:
            t = 0.0
        line_total = t * rate
        labor_rows.append([
            li.labor_desc or "",
            f"{t:g} {cfg.get('hours_suffix', 'hrs')}" if t else "",
            _money(line_total) if line_total else ""
        ])

    if show_labor:
        body_y = draw_table(
            cfg["labor_title"],
            M,
            body_y,
            [cfg["labor_desc_label"], cfg.get("labor_time_label", "Time"), cfg.get("labor_total_label", "Line Total")],
            labor_rows,
            col_widths=[PAGE_W - 2 * M - 190, 90, 100],
            money_cols={2}
        )

    parts_rows = []
    for p in inv.parts:
        parts_rows.append([
            p.part_name or "",
            _money(inv.part_price_with_markup(p.part_price or 0.0)) if (p.part_price or 0.0) else ""
        ])

    has_parts_rows = any((row[0] or row[1]) for row in parts_rows)
    if show_parts and has_parts_rows:
        body_y = draw_table(
            cfg["parts_title"],
            M,
            body_y - 10,
            [cfg["parts_name_label"], cfg.get("parts_price_label", "Price")],
            parts_rows,
            col_widths=[PAGE_W - 2 * M - 120, 120],
            money_cols={1}
        )

    # Notes + Summary
    notes_box_w = PAGE_W - 2 * M - 250
    notes_y_top = max(body_y - 10, 2.2 * inch + M)

    pdf.setFont("Helvetica", 10)
    left_padding = 12
    right_padding = 12
    line_height = 14
    SPACER_GAP = 3
    header_title_gap = 42
    bottom_padding = 12

    max_width = notes_box_w - left_padding - right_padding
    all_note_lines = _split_notes_into_lines(inv.notes or "", max_width, font="Helvetica", size=10) if show_notes else []

    footer_y = 0.55 * inch
    footer_clearance = 0.20 * inch
    page_bottom_limit = footer_y + footer_clearance

    needed_text_h = 0
    for ln in all_note_lines:
        needed_text_h += SPACER_GAP if ln == "__SPACER__" else line_height
    needed_box_h = header_title_gap + needed_text_h + bottom_padding

    max_box_h_this_page = notes_y_top - page_bottom_limit
    notes_box_h = min(max_box_h_this_page, needed_box_h) if show_notes else 0

    remaining_lines = []
    if show_notes:
        pdf.setFillColor(soft_bg)
        pdf.roundRect(M, notes_y_top - notes_box_h, notes_box_w, notes_box_h, 10, stroke=1, fill=1)
        pdf.setStrokeColor(line_color)
        pdf.roundRect(M, notes_y_top - notes_box_h, notes_box_w, notes_box_h, 10, stroke=1, fill=0)
        pdf.setFont("Helvetica-Bold", 10)
        pdf.setFillColor(brand_muted)
        pdf.drawString(M + 12, notes_y_top - 18, "NOTES")

        pdf.setFont("Helvetica", 10)
        pdf.setFillColor(colors.black)
        y_note = notes_y_top - header_title_gap
        bottom_limit = notes_y_top - notes_box_h + bottom_padding

        lines_fit = 0
        for line in all_note_lines:
            if y_note < bottom_limit:
                break
            if line == "__SPACER__":
                y_note -= SPACER_GAP
            else:
                pdf.drawString(M + left_padding, y_note, line)
                y_note -= line_height
            lines_fit += 1

        remaining_lines = all_note_lines[lines_fit:]

    sum_x = PAGE_W - M - 240
    sum_w = 240
    sum_h = 1.9 * inch
    pdf.setFillColor(soft_bg)
    pdf.roundRect(sum_x, notes_y_top - sum_h, sum_w, sum_h, 10, stroke=1, fill=1)
    pdf.setStrokeColor(line_color)
    pdf.roundRect(sum_x, notes_y_top - sum_h, sum_w, sum_h, 10, stroke=1, fill=0)

    pdf.setFont("Helvetica-Bold", 10)
    pdf.setFillColor(brand_muted)
    pdf.drawString(sum_x + 12, notes_y_top - 18, "SUMMARY")
    pdf.setFillColor(colors.black)

    total_parts = inv.parts_total() if show_parts else 0.0
    total_labor = inv.labor_total() if show_labor else 0.0
    total_price = inv.invoice_total()
    tax_amount = inv.tax_amount()
    price_owed = inv.amount_due()

    right_edge = sum_x + sum_w - 12
    y = notes_y_top - 44

    if show_parts and has_parts_rows and total_parts:
        label_right_value(sum_x + 12, right_edge, y, f"{cfg['parts_title']}:", _money(total_parts)); y -= 16
    if show_labor:
        label_right_value(sum_x + 12, right_edge, y, f"{cfg['labor_title']}:", _money(total_labor)); y -= 16
    if show_shop_supplies and inv.shop_supplies:
        label_right_value(sum_x + 12, right_edge, y, f"{cfg['shop_supplies_label']}:", _money(inv.shop_supplies)); y -= 16
    if tax_amount:
        label_right_value(sum_x + 12, right_edge, y, f"{_tax_label(inv)}:", _money(tax_amount)); y -= 16

    pdf.setStrokeColor(line_color)
    pdf.line(sum_x + 12, y + 4, sum_x + sum_w - 12, y + 4)
    pdf.setStrokeColor(colors.black)
    y -= 10

    label = "Estimated Total:" if is_estimate else "Total:"
    label_right_value(sum_x + 12, right_edge, y, label, _money(total_price)); y -= 18

    if not is_estimate:
        label_right_value(sum_x + 12, right_edge, y, "Paid:", _money(inv.paid)); y -= 18

    pdf.setFont("Helvetica-Bold", 12)
    if is_estimate:
        right_text(sum_x + sum_w - 12, (notes_y_top - sum_h) - 26, f"ESTIMATED TOTAL: {_money(total_price)}", "Helvetica-Bold", 12, brand_dark)
    elif price_owed < 0:
        profit = abs(price_owed)
        right_text(sum_x + sum_w - 12, (notes_y_top - sum_h) - 26, f"PROFIT: {_money(profit)}", "Helvetica-Bold", 12, colors.HexColor("#15803d"))
    else:
        right_text(sum_x + sum_w - 12, (notes_y_top - sum_h) - 26, f"AMOUNT DUE: {_money(price_owed)}", "Helvetica-Bold", 12, brand_dark)

    def footer():
        pdf.setFont("Helvetica-Oblique", 9)
        pdf.setFillColor(brand_muted)
        if is_estimate:
            pdf.drawString(M, footer_y, "Total is an estimated cost of service. Actual amount may differ.")
        else:
            pdf.drawString(M, footer_y, "Thank you for your business.")
        pdf.setFillColor(colors.black)

    def start_new_page_with_header():
        pdf.showPage()
        pdf.setFillColor(soft_bg)
        pdf.rect(0, PAGE_H - 0.8 * inch, PAGE_W, 0.8 * inch, stroke=0, fill=1)
        pdf.setFont("Helvetica-Bold", 12)
        pdf.setFillColor(brand_dark)
        pdf.drawString(M, PAGE_H - 0.45 * inch, f"{doc_label} (cont.)")
        pdf.setFont("Helvetica", 9)
        pdf.setFillColor(brand_muted)
        pdf.drawString(M, PAGE_H - 0.62 * inch, f"{display_no}  •  Generated: {generated_str}")

    if show_notes and remaining_lines:
        footer()
        while remaining_lines:
            start_new_page_with_header()

            notes_y_top_2 = PAGE_H - (M + 0.6 * inch)
            page_bottom_limit_2 = footer_y + footer_clearance
            notes_box_h_2 = notes_y_top_2 - page_bottom_limit_2

            pdf.setFillColor(soft_bg)
            pdf.roundRect(M, notes_y_top_2 - notes_box_h_2, notes_box_w, notes_box_h_2, 10, stroke=1, fill=1)
            pdf.setStrokeColor(line_color)
            pdf.roundRect(M, notes_y_top_2 - notes_box_h_2, notes_box_w, notes_box_h_2, 10, stroke=1, fill=0)
            pdf.setFont("Helvetica-Bold", 10)
            pdf.setFillColor(brand_muted)
            pdf.drawString(M + 12, notes_y_top_2 - 18, "NOTES (cont.)")
            pdf.setFillColor(colors.black)

            pdf.setFont("Helvetica", 10)
            y_note2 = notes_y_top_2 - header_title_gap
            bottom2 = notes_y_top_2 - notes_box_h_2 + bottom_padding

            fit2 = 0
            for line in remaining_lines:
                if y_note2 < bottom2:
                    break
                if line == "__SPACER__":
                    y_note2 -= SPACER_GAP
                else:
                    pdf.drawString(M + left_padding, y_note2, line)
                    y_note2 -= line_height
                fit2 += 1

            remaining_lines = remaining_lines[fit2:]
            footer()
    else:
        footer()

    pdf.save()

    inv.pdf_path = pdf_path
    inv.pdf_generated_at = generated_dt
    session.add(inv)
    session.commit()

    return pdf_path


def _render_split_panel_pdf(
    *,
    session,
    inv: Invoice,
    owner: User | None,
    customer: Customer | None,
    customer_email: str,
    customer_phone: str,
    customer_address: str,
    cfg: dict,
    template_key: str,
    pdf_path: str,
    display_no: str,
    doc_label: str,
    generated_dt: datetime,
    generated_str: str,
    owner_logo_abs: str,
    owner_logo_blob: bytes | None,
    is_estimate: bool,
    builder_cfg: dict | None = None,
):
    PAGE_W, PAGE_H = LETTER
    M = 0.55 * inch

    pdf = canvas.Canvas(pdf_path, pagesize=LETTER)
    pdf.setTitle(f"{doc_label.title()} - {display_no}")
    builder_enabled = bool(builder_cfg.get("enabled", False)) if isinstance(builder_cfg, dict) else False
    builder_accent = colors.HexColor(
        (builder_cfg.get("accent_color") if isinstance(builder_cfg, dict) else None) or "#0f172a"
    )
    builder_header_style = (
        (builder_cfg.get("header_style") if isinstance(builder_cfg, dict) else None) or "classic"
    ).strip().lower()
    builder_compact_mode = bool(builder_cfg.get("compact_mode", False)) if isinstance(builder_cfg, dict) else False

    rail_color = colors.HexColor("#0f172a")
    rail_text = colors.HexColor("#e2e8f0")
    accent = colors.HexColor("#2563eb")
    line_color = colors.HexColor("#e2e8f0")
    soft_bg = colors.HexColor("#f8fafc")
    if builder_enabled:
        accent = builder_accent
        if builder_header_style == "banded":
            rail_color = builder_accent

    def right_text(x, y, text, font="Helvetica", size=10, color=colors.black):
        pdf.setFont(font, size)
        pdf.setFillColor(color)
        w = pdf.stringWidth(str(text), font, size)
        pdf.drawString(x - w, y, str(text))

    def label_right_value(x_left, x_right, y, label, value, label_font=("Helvetica-Bold", 9), value_font=("Helvetica", 10)):
        pdf.setFont(*label_font)
        pdf.setFillColor(colors.black)
        pdf.drawString(x_left, y, label)
        right_text(x_right, y, str(value), value_font[0], value_font[1], colors.black)

    show_job = bool(cfg.get("show_job", True))
    show_labor = bool(cfg.get("show_labor", True))
    show_parts = bool(cfg.get("show_parts", True))
    show_shop_supplies = bool(cfg.get("show_shop_supplies", True))
    show_notes = bool(cfg.get("show_notes", True))

    # Left summary rail
    rail_w = 1.55 * inch
    rail_x = M
    rail_y = M
    rail_h = PAGE_H - (2 * M)
    pdf.setFillColor(rail_color)
    pdf.roundRect(rail_x, rail_y, rail_w, rail_h, 14, stroke=0, fill=1)

    # Logo / brand on rail
    logo_x = rail_x + 12
    logo_y_top = PAGE_H - M - 36
    logo_drawn = False
    logo_h_drawn = 0.0
    if owner_logo_blob:
        try:
            img = ImageReader(io.BytesIO(owner_logo_blob))
            iw, ih = img.getSize()
            max_h = 0.55 * inch
            max_w = rail_w - 24
            scale = min(max_w / iw, max_h / ih)
            w = iw * scale
            h = ih * scale
            pdf.drawImage(img, logo_x, logo_y_top - h, width=w, height=h, mask="auto")
            logo_drawn = True
            logo_h_drawn = h
        except Exception:
            logo_drawn = False
    elif owner_logo_abs and os.path.exists(owner_logo_abs):
        try:
            img = ImageReader(owner_logo_abs)
            iw, ih = img.getSize()
            max_h = 0.55 * inch
            max_w = rail_w - 24
            scale = min(max_w / float(iw), max_h / float(ih))
            w = float(iw) * scale
            h = float(ih) * scale
            pdf.drawImage(img, logo_x, logo_y_top - h, width=w, height=h, mask="auto")
            logo_drawn = True
            logo_h_drawn = h
        except Exception:
            logo_drawn = False

    business_name = (getattr(owner, "business_name", None) or "").strip() if owner else ""
    username = (getattr(owner, "username", None) or "").strip() if owner else ""
    header_name = business_name or username or "InvoiceRunner"
    header_address_lines = _owner_address_lines(owner)
    header_phone = (getattr(owner, "phone", None) or "").strip() if owner else ""

    pdf.setFillColor(rail_text)
    pdf.setFont("Helvetica-Bold", 10)
    if logo_drawn:
        # Start text below the rendered logo so the two never overlap.
        name_y = logo_y_top - logo_h_drawn - 10
    else:
        name_y = logo_y_top - 0.1 * inch
    for ln in _wrap_text(header_name, "Helvetica-Bold", 10, rail_w - 20)[:2]:
        pdf.drawString(rail_x + 12, name_y, ln)
        name_y -= 12

    pdf.setFont("Helvetica", 8)
    rail_info = []
    for addr_line in header_address_lines:
        rail_info.extend(_wrap_text(addr_line, "Helvetica", 8, rail_w - 20))
    if header_phone:
        rail_info.append(header_phone)
    info_y = name_y - 6
    for ln in rail_info[:3]:
        pdf.drawString(rail_x + 12, info_y, ln)
        info_y -= 11

    # Summary block on rail
    pdf.setFont("Helvetica-Bold", 10)
    pdf.setFillColor(rail_text)
    pdf.drawString(rail_x + 12, rail_y + 145, "SUMMARY")

    total_parts = inv.parts_total() if show_parts else 0.0
    total_labor = inv.labor_total() if show_labor else 0.0
    total_price = inv.invoice_total()
    tax_amount = inv.tax_amount()
    price_owed = inv.amount_due()

    pdf.setFont("Helvetica", 9)
    y = rail_y + 125
    if show_labor:
        pdf.drawString(rail_x + 12, y, f"{cfg['labor_title']}: {_money(total_labor)}"); y -= 14
    if show_parts and total_parts:
        pdf.drawString(rail_x + 12, y, f"{cfg['parts_title']}: {_money(total_parts)}"); y -= 14
    if show_shop_supplies and inv.shop_supplies:
        pdf.drawString(rail_x + 12, y, f"{cfg['shop_supplies_label']}: {_money(inv.shop_supplies)}"); y -= 14
    if tax_amount:
        pdf.drawString(rail_x + 12, y, f"{_tax_label(inv)}: {_money(tax_amount)}"); y -= 14

    y -= 4
    pdf.setFont("Helvetica-Bold", 10)
    label = "Est. Total" if is_estimate else "Total"
    pdf.drawString(rail_x + 12, y, f"{label}: {_money(total_price)}"); y -= 16
    if not is_estimate:
        pdf.setFont("Helvetica", 9)
        pdf.drawString(rail_x + 12, y, f"Paid: {_money(inv.paid)}"); y -= 14
        pdf.setFont("Helvetica-Bold", 10)
        if price_owed < 0:
            pdf.drawString(rail_x + 12, y, f"Profit: {_money(abs(price_owed))}")
        else:
            pdf.drawString(rail_x + 12, y, f"Due: {_money(price_owed)}")

    # Right content area
    content_x = rail_x + rail_w + 0.45 * inch
    content_w = PAGE_W - content_x - M

    pdf.setFillColor(soft_bg)
    pdf.roundRect(content_x, PAGE_H - M - 1.0 * inch, content_w, 1.0 * inch, 12, stroke=1, fill=1)
    pdf.setStrokeColor(line_color)
    pdf.roundRect(content_x, PAGE_H - M - 1.0 * inch, content_w, 1.0 * inch, 12, stroke=1, fill=0)

    pdf.setFont("Helvetica-Bold", 12)
    pdf.setFillColor(colors.black)
    pdf.drawString(content_x + 14, PAGE_H - M - 0.55 * inch, doc_label)

    right_text(content_x + content_w - 14, PAGE_H - M - 0.48 * inch, f"{doc_label.title()} #: {display_no}", "Helvetica", 9, colors.black)
    right_text(content_x + content_w - 14, PAGE_H - M - 0.70 * inch, f"Date: {inv.date_in}", "Helvetica", 9, colors.black)
    due_line = _invoice_due_date_line(inv, owner, is_estimate=is_estimate)
    if due_line:
        right_text(content_x + content_w - 14, PAGE_H - M - 0.88 * inch, due_line, "Helvetica", 8, colors.black)

    # Info cards
    card_y_top = PAGE_H - M - 1.35 * inch
    card_h = 1.55 * inch
    card_w = (content_w - 0.35 * inch) / 2

    def draw_card(x, y, title):
        pdf.setFillColor(colors.white)
        pdf.roundRect(x, y - card_h, card_w, card_h, 12, stroke=1, fill=1)
        pdf.setStrokeColor(line_color)
        pdf.roundRect(x, y - card_h, card_w, card_h, 12, stroke=1, fill=0)
        pdf.setFont("Helvetica-Bold", 9)
        pdf.setFillColor(colors.HexColor("#64748b"))
        pdf.drawString(x + 12, y - 16, title)
        pdf.setFillColor(colors.black)

    # Customer name
    if customer and (getattr(customer, "name", None) or "").strip():
        customer_name = (customer.name or "").strip()
    else:
        nameFirst, _, _tail = (inv.name or "").partition(":")
        customer_name = (nameFirst or inv.name or "").strip()

    addr_lines = []
    addr_text = (customer_address or "").strip()
    if addr_text:
        addr_parts = [p.strip() for p in addr_text.split(",") if p.strip()]
        if len(addr_parts) >= 3:
            line1 = f"{', '.join(addr_parts[:-2])}"
            line2 = f"{addr_parts[-2]}, {addr_parts[-1]}"
        elif len(addr_parts) == 2:
            line1 = addr_parts[0]
            line2 = addr_parts[1]
        else:
            line1 = addr_text
            line2 = ""
        addr_lines.extend(_wrap_text(line1, "Helvetica", 9, card_w - 24))
        if line2:
            addr_lines.extend(_wrap_text(line2, "Helvetica", 9, card_w - 24))

    card1_x = content_x
    card2_x = content_x + card_w + 0.35 * inch
    draw_card(card1_x, card_y_top, "BILL TO")
    draw_card(card2_x, card_y_top, cfg.get("job_box_title", "DETAILS"))

    pdf.setFont("Helvetica-Bold", 11)
    pdf.drawString(card1_x + 12, card_y_top - 36, customer_name)
    pdf.setFont("Helvetica", 9)
    y_cursor = card_y_top - 52
    if customer_phone:
        pdf.drawString(card1_x + 12, y_cursor, f"Phone: {customer_phone}"); y_cursor -= 12
    if customer_email:
        for ln in _wrap_text(f"Email: {customer_email}", "Helvetica", 9, card_w - 24)[:2]:
            pdf.drawString(card1_x + 12, y_cursor, ln); y_cursor -= 12
    for ln in addr_lines[:2]:
        pdf.drawString(card1_x + 12, y_cursor, ln); y_cursor -= 12

    pdf.setFont("Helvetica", 9)
    job_text = f"{cfg['job_label']}: {inv.vehicle or ''}" if show_job else ""
    job_lines = _wrap_text(job_text, "Helvetica", 9, card_w - 24) if show_job else []
    y_job = card_y_top - 36
    for ln in job_lines[:2]:
        pdf.drawString(card2_x + 12, y_job, ln)
        y_job -= 12
    if template_key == "flipping_items":
        pdf.drawString(card2_x + 12, y_job, f"Profit: {_money(inv.labor_total())}"); y_job -= 12
        pdf.drawString(card2_x + 12, y_job, f"Sold For: {_money(inv.paid)}")
    else:
        pdf.drawString(card2_x + 12, y_job, f"{cfg['job_rate_label']}: {_money(inv.price_per_hour)}"); y_job -= 12
        pdf.drawString(card2_x + 12, y_job, f"{cfg['job_hours_label']}: {inv.hours} {cfg.get('hours_suffix', 'hrs')}")

    # Tables
    def draw_table(title, x, y_top, col_titles, rows, col_widths, money_cols=None):
        money_cols = set(money_cols or [])
        base_row_h = 14 if builder_compact_mode else 16
        title_gap = 18

        pdf.setFont("Helvetica-Bold", 12)
        pdf.setFillColor(colors.black)
        pdf.drawString(x, y_top, title)
        y = y_top - title_gap

        table_w = sum(col_widths)
        pdf.setFillColor(accent)
        pdf.roundRect(x, y - base_row_h + 9, table_w, base_row_h + 2, 6, stroke=0, fill=1)
        pdf.setFont("Helvetica-Bold", 9)
        pdf.setFillColor(colors.white)

        cx = x
        for i, h in enumerate(col_titles):
            if i in money_cols:
                right_text(cx + col_widths[i] - 8, y + 1, h, "Helvetica-Bold", 9, colors.white)
            else:
                pdf.drawString(cx + 8, y + 1, h)
            cx += col_widths[i]

        y_cursor = y - base_row_h
        pdf.setFont("Helvetica", 10)
        pdf.setFillColor(colors.black)

        for row in rows:
            wrapped_cells = []
            row_height = base_row_h
            for i, cell in enumerate(row):
                max_w = col_widths[i] - 16
                lines = _wrap_text(cell, "Helvetica", 10, max_w)
                wrapped_cells.append(lines)
                row_height = max(row_height, len(lines) * base_row_h)

            cx = x
            for i, lines in enumerate(wrapped_cells):
                line_y = y_cursor
                for line in lines:
                    if i in money_cols:
                        right_text(cx + col_widths[i] - 8, line_y, line, "Helvetica", 10, colors.black)
                    else:
                        pdf.drawString(cx + 8, line_y, line)
                    line_y -= base_row_h
                cx += col_widths[i]

            pdf.setStrokeColor(line_color)
            pdf.setLineWidth(1)
            pdf.line(x, y_cursor - 4, x + table_w, y_cursor - 4)
            y_cursor -= row_height

        pdf.setStrokeColor(colors.black)
        return y_cursor - 8

    body_y = card_y_top - card_h - 0.4 * inch

    labor_rows = []
    rate = float(inv.price_per_hour or 0.0)
    for li in inv.labor_items:
        try:
            t = float(li.labor_time_hours or 0.0)
        except Exception:
            t = 0.0
        line_total = t * rate
        labor_rows.append([
            li.labor_desc or "",
            f"{t:g} {cfg.get('hours_suffix', 'hrs')}" if t else "",
            _money(line_total) if line_total else ""
        ])

    if show_labor:
        body_y = draw_table(
            cfg["labor_title"],
            content_x,
            body_y,
            [cfg["labor_desc_label"], cfg.get("labor_time_label", "Time"), cfg.get("labor_total_label", "Line Total")],
            labor_rows,
            col_widths=[content_w - 190, 90, 100],
            money_cols={2}
        )

    parts_rows = []
    for p in inv.parts:
        parts_rows.append([
            p.part_name or "",
            _money(inv.part_price_with_markup(p.part_price or 0.0)) if (p.part_price or 0.0) else ""
        ])

    has_parts_rows = any((row[0] or row[1]) for row in parts_rows)
    if show_parts and has_parts_rows:
        body_y = draw_table(
            cfg["parts_title"],
            content_x,
            body_y - 10,
            [cfg["parts_name_label"], cfg.get("parts_price_label", "Price")],
            parts_rows,
            col_widths=[content_w - 120, 120],
            money_cols={1}
        )

    # Notes box
    notes_box_w = content_w
    notes_y_top = max(body_y - 8, 2.2 * inch + M)

    left_padding = 12
    right_padding = 12
    line_height = 14
    SPACER_GAP = 3
    header_title_gap = 42
    bottom_padding = 12

    max_width = notes_box_w - left_padding - right_padding
    all_note_lines = _split_notes_into_lines(inv.notes or "", max_width, font="Helvetica", size=10) if show_notes else []

    footer_y = 0.55 * inch
    footer_clearance = 0.2 * inch
    page_bottom_limit = footer_y + footer_clearance

    needed_text_h = 0
    for ln in all_note_lines:
        needed_text_h += SPACER_GAP if ln == "__SPACER__" else line_height
    needed_box_h = header_title_gap + needed_text_h + bottom_padding

    max_box_h_this_page = notes_y_top - page_bottom_limit
    notes_box_h = min(max_box_h_this_page, needed_box_h) if show_notes else 0

    remaining_lines = []
    if show_notes:
        pdf.setFillColor(soft_bg)
        pdf.roundRect(content_x, notes_y_top - notes_box_h, notes_box_w, notes_box_h, 12, stroke=1, fill=1)
        pdf.setStrokeColor(line_color)
        pdf.roundRect(content_x, notes_y_top - notes_box_h, notes_box_w, notes_box_h, 12, stroke=1, fill=0)
        pdf.setFont("Helvetica-Bold", 10)
        pdf.setFillColor(colors.HexColor("#64748b"))
        pdf.drawString(content_x + 12, notes_y_top - 18, "NOTES")

        pdf.setFont("Helvetica", 10)
        pdf.setFillColor(colors.black)
        y_note = notes_y_top - header_title_gap
        bottom_limit = notes_y_top - notes_box_h + bottom_padding

        lines_fit = 0
        for line in all_note_lines:
            if y_note < bottom_limit:
                break
            if line == "__SPACER__":
                y_note -= SPACER_GAP
            else:
                pdf.drawString(content_x + left_padding, y_note, line)
                y_note -= line_height
            lines_fit += 1

        remaining_lines = all_note_lines[lines_fit:]

    def footer():
        pdf.setFont("Helvetica-Oblique", 9)
        pdf.setFillColor(colors.HexColor("#94a3b8"))
        if is_estimate:
            pdf.drawString(content_x, footer_y, "Total is an estimated cost of service. Actual amount may differ.")
        else:
            pdf.drawString(content_x, footer_y, "Thank you for your business.")
        pdf.setFillColor(colors.black)

    def start_new_page_with_header():
        pdf.showPage()
        pdf.setFillColor(soft_bg)
        pdf.rect(0, PAGE_H - 0.8 * inch, PAGE_W, 0.8 * inch, stroke=0, fill=1)
        pdf.setFont("Helvetica-Bold", 12)
        pdf.setFillColor(rail_color)
        pdf.drawString(M, PAGE_H - 0.45 * inch, f"{doc_label} (cont.)")
        pdf.setFont("Helvetica", 9)
        pdf.setFillColor(colors.HexColor("#94a3b8"))
        pdf.drawString(M, PAGE_H - 0.62 * inch, f"{display_no}  •  Generated: {generated_str}")

    if show_notes and remaining_lines:
        footer()
        while remaining_lines:
            start_new_page_with_header()

            notes_y_top_2 = PAGE_H - (M + 0.6 * inch)
            page_bottom_limit_2 = footer_y + footer_clearance
            notes_box_h_2 = notes_y_top_2 - page_bottom_limit_2

            pdf.setFillColor(soft_bg)
            pdf.roundRect(M, notes_y_top_2 - notes_box_h_2, PAGE_W - 2 * M, notes_box_h_2, 12, stroke=1, fill=1)
            pdf.setStrokeColor(line_color)
            pdf.roundRect(M, notes_y_top_2 - notes_box_h_2, PAGE_W - 2 * M, notes_box_h_2, 12, stroke=1, fill=0)
            pdf.setFont("Helvetica-Bold", 10)
            pdf.setFillColor(colors.HexColor("#64748b"))
            pdf.drawString(M + 12, notes_y_top_2 - 18, "NOTES (cont.)")
            pdf.setFillColor(colors.black)

            pdf.setFont("Helvetica", 10)
            y_note2 = notes_y_top_2 - header_title_gap
            bottom2 = notes_y_top_2 - notes_box_h_2 + bottom_padding

            fit2 = 0
            for line in remaining_lines:
                if y_note2 < bottom2:
                    break
                if line == "__SPACER__":
                    y_note2 -= SPACER_GAP
                else:
                    pdf.drawString(M + left_padding, y_note2, line)
                    y_note2 -= line_height
                fit2 += 1

            remaining_lines = remaining_lines[fit2:]
            footer()
    else:
        footer()

    pdf.save()
    inv.pdf_path = pdf_path
    inv.pdf_generated_at = generated_dt
    session.add(inv)
    session.commit()

    return pdf_path


def _render_strip_pdf(
    *,
    session,
    inv: Invoice,
    owner: User | None,
    customer: Customer | None,
    customer_email: str,
    customer_phone: str,
    customer_address: str,
    cfg: dict,
    template_key: str,
    pdf_path: str,
    display_no: str,
    doc_label: str,
    generated_dt: datetime,
    generated_str: str,
    owner_logo_abs: str,
    owner_logo_blob: bytes | None,
    is_estimate: bool,
    builder_cfg: dict | None = None,
):
    PAGE_W, PAGE_H = LETTER
    M = 0.7 * inch

    pdf = canvas.Canvas(pdf_path, pagesize=LETTER)
    pdf.setTitle(f"{doc_label.title()} - {display_no}")
    builder_enabled = bool(builder_cfg.get("enabled", False)) if isinstance(builder_cfg, dict) else False
    builder_accent = colors.HexColor(
        (builder_cfg.get("accent_color") if isinstance(builder_cfg, dict) else None) or "#0f172a"
    )
    builder_header_style = (
        (builder_cfg.get("header_style") if isinstance(builder_cfg, dict) else None) or "classic"
    ).strip().lower()
    builder_compact_mode = bool(builder_cfg.get("compact_mode", False)) if isinstance(builder_cfg, dict) else False

    accent = colors.HexColor("#0ea5a4")
    accent_dark = colors.HexColor("#0f172a")
    line_color = colors.HexColor("#e5e7eb")
    muted = colors.HexColor("#6b7280")
    if builder_enabled:
        accent = builder_accent
        if builder_header_style == "banded":
            accent_dark = builder_accent

    def right_text(x, y, text, font="Helvetica", size=10, color=colors.black):
        pdf.setFont(font, size)
        pdf.setFillColor(color)
        w = pdf.stringWidth(str(text), font, size)
        pdf.drawString(x - w, y, str(text))

    def label_value(x, y, label, value, label_font=("Helvetica-Bold", 9), value_font=("Helvetica", 10)):
        pdf.setFont(*label_font)
        pdf.setFillColor(colors.black)
        pdf.drawString(x, y, label)
        pdf.setFont(*value_font)
        pdf.drawString(x + 70, y, str(value))

    def label_right_value(x_left, x_right, y, label, value, label_font=("Helvetica-Bold", 9), value_font=("Helvetica", 10)):
        pdf.setFont(*label_font)
        pdf.setFillColor(colors.black)
        pdf.drawString(x_left, y, label)
        right_text(x_right, y, str(value), value_font[0], value_font[1], colors.black)

    show_job = bool(cfg.get("show_job", True))
    show_labor = bool(cfg.get("show_labor", True))
    show_parts = bool(cfg.get("show_parts", True))
    show_shop_supplies = bool(cfg.get("show_shop_supplies", True))
    show_notes = bool(cfg.get("show_notes", True))

    # Header
    header_h = 1.1 * inch
    header_bg = colors.white
    header_text = colors.black
    header_muted = muted
    if builder_enabled and builder_header_style == "banded":
        header_bg = accent_dark
        header_text = colors.white
        header_muted = colors.white
    pdf.setFillColor(header_bg)
    pdf.rect(0, PAGE_H - header_h, PAGE_W, header_h, stroke=0, fill=1)
    pdf.setStrokeColor(line_color)
    pdf.setLineWidth(1)
    pdf.line(M, PAGE_H - header_h - 6, PAGE_W - M, PAGE_H - header_h - 6)

    # Logo / business name left
    logo_w = 0
    if owner_logo_blob:
        try:
            img = ImageReader(io.BytesIO(owner_logo_blob))
            iw, ih = img.getSize()
            max_h = 0.45 * inch
            max_w = 1.1 * inch
            scale = min(max_w / iw, max_h / ih)
            w = iw * scale
            h = ih * scale
            pdf.drawImage(img, M, PAGE_H - 0.7 * inch - (h / 2), width=w, height=h, mask="auto")
            logo_w = w + 10
        except Exception:
            logo_w = 0
    elif owner_logo_abs and os.path.exists(owner_logo_abs):
        try:
            img = ImageReader(owner_logo_abs)
            iw, ih = img.getSize()
            max_h = 0.45 * inch
            max_w = 1.1 * inch
            scale = min(max_w / float(iw), max_h / float(ih))
            w = float(iw) * scale
            h = float(ih) * scale
            pdf.drawImage(img, M, PAGE_H - 0.7 * inch - (h / 2), width=w, height=h, mask="auto")
            logo_w = w + 10
        except Exception:
            logo_w = 0

    business_name = (getattr(owner, "business_name", None) or "").strip() if owner else ""
    username = (getattr(owner, "username", None) or "").strip() if owner else ""
    header_name = business_name or username or "InvoiceRunner"
    pdf.setFont("Helvetica-Bold", 12)
    pdf.setFillColor(header_text)
    pdf.drawString(M + logo_w, PAGE_H - 0.72 * inch, header_name)

    pdf.setFont("Helvetica", 9)
    pdf.setFillColor(header_muted)
    header_address_lines = _owner_address_lines(owner)
    header_phone = (getattr(owner, "phone", None) or "").strip() if owner else ""
    info_lines = []
    for addr_line in header_address_lines:
        info_lines.extend(_wrap_text(addr_line, "Helvetica", 9, 3.6 * inch))
    if header_phone:
        info_lines.append(header_phone)
    info_y = PAGE_H - 0.95 * inch
    for ln in info_lines[:2]:
        pdf.drawString(M + logo_w, info_y, ln)
        info_y -= 12

    # Invoice label right
    right_x = PAGE_W - M
    right_text(right_x, PAGE_H - 0.55 * inch, f"{doc_label.title()} {display_no}", "Helvetica-Bold", 12, header_text)
    template_labels = {
        "auto_repair": "Auto Repair",
        "general_service": "General Service",
        "accountant": "Accountant",
        "computer_repair": "Computer Repair",
        "lawn_care": "Lawn Care",
        "flipping_items": "Flipping Items",
        "custom": "Custom",
    }
    prof_label = (cfg.get("profession_label") or "").strip() or template_labels.get(template_key, "Service")
    if template_key == "custom" and not (cfg.get("profession_label") or "").strip() and owner is not None:
        custom_name = (getattr(owner, "custom_profession_name", None) or "").strip()
        if custom_name:
            prof_label = custom_name
    right_text(right_x, PAGE_H - 0.74 * inch, f"{prof_label} {doc_label.lower()}", "Helvetica", 9, header_muted)

    # Bill to + meta
    top_y = PAGE_H - header_h - 0.35 * inch
    pdf.setFont("Helvetica-Bold", 9)
    pdf.setFillColor(colors.black)
    pdf.drawString(M, top_y, "BILL TO")

    # Customer name
    if customer and (getattr(customer, "name", None) or "").strip():
        customer_name = (customer.name or "").strip()
    else:
        nameFirst, _, _tail = (inv.name or "").partition(":")
        customer_name = (nameFirst or inv.name or "").strip()

    pdf.setFont("Helvetica", 9)
    pdf.setFillColor(colors.black)
    y_bill = top_y - 14
    for ln in _wrap_text(customer_name, "Helvetica", 9, 3.2 * inch):
        pdf.drawString(M, y_bill, ln)
        y_bill -= 12

    if customer_address:
        for ln in _wrap_text(customer_address, "Helvetica", 9, 3.2 * inch)[:3]:
            pdf.drawString(M, y_bill, ln)
            y_bill -= 12

    if customer_email:
        pdf.drawString(M, y_bill, f"Email: {customer_email}")
        y_bill -= 12
    if customer_phone:
        pdf.drawString(M, y_bill, f"Phone: {customer_phone}")
        y_bill -= 12

    # Meta block on right
    meta_x = PAGE_W - M - 180
    meta_y = top_y - 4
    label_value(meta_x, meta_y, "Issue date:", inv.date_in)
    label_value(meta_x, meta_y - 14, "Reference:", display_no)
    due_line = _invoice_due_date_line(inv, owner, is_estimate=is_estimate)
    if due_line:
        pdf.setFont("Helvetica", 8)
        pdf.setFillColor(muted)
        pdf.drawString(meta_x, meta_y - 28, due_line)
        pdf.setFillColor(colors.black)

    # Summary strip
    strip_y = top_y - 70
    strip_h = 0.55 * inch
    strip_x = M
    strip_w = PAGE_W - 2 * M
    pdf.setFillColor(accent)
    pdf.rect(strip_x, strip_y - strip_h, strip_w, strip_h, stroke=0, fill=1)

    box_w = strip_w / 4
    pdf.setFillColor(colors.white)
    pdf.setFont("Helvetica-Bold", 9)
    pdf.drawString(strip_x + 10, strip_y - 16, "Invoice No.")
    pdf.setFont("Helvetica-Bold", 12)
    pdf.drawString(strip_x + 10, strip_y - 34, display_no)

    pdf.setFont("Helvetica-Bold", 9)
    pdf.drawString(strip_x + box_w + 10, strip_y - 16, "Issue date")
    pdf.setFont("Helvetica-Bold", 12)
    pdf.drawString(strip_x + box_w + 10, strip_y - 34, inv.date_in)

    total_price = inv.invoice_total()
    pdf.setFillColor(accent_dark)
    pdf.rect(strip_x + (2 * box_w), strip_y - strip_h, box_w * 2, strip_h, stroke=0, fill=1)
    pdf.setFillColor(colors.white)
    pdf.setFont("Helvetica-Bold", 9)
    pdf.drawString(strip_x + (2 * box_w) + 10, strip_y - 16, "Total due")
    pdf.setFont("Helvetica-Bold", 14)
    pdf.drawString(strip_x + (2 * box_w) + 10, strip_y - 36, _money(total_price))

    # Table
    def draw_table(title, x, y_top, col_titles, rows, col_widths, money_cols=None):
        money_cols = set(money_cols or [])
        base_row_h = 14 if builder_compact_mode else 16
        title_gap = 18

        pdf.setFont("Helvetica-Bold", 11)
        pdf.setFillColor(colors.black)
        pdf.drawString(x, y_top, title)
        y = y_top - title_gap

        table_w = sum(col_widths)
        pdf.setStrokeColor(line_color)
        pdf.setLineWidth(1)
        pdf.line(x, y - 6, x + table_w, y - 6)

        pdf.setFont("Helvetica-Bold", 9)
        pdf.setFillColor(colors.black)
        cx = x
        for i, h in enumerate(col_titles):
            if i in money_cols:
                right_text(cx + col_widths[i] - 6, y, h, "Helvetica-Bold", 9)
            else:
                pdf.drawString(cx + 6, y, h)
            cx += col_widths[i]

        y_cursor = y - base_row_h
        pdf.setFont("Helvetica", 10)

        for row in rows:
            wrapped_cells = []
            row_height = base_row_h
            for i, cell in enumerate(row):
                max_w = col_widths[i] - 12
                lines = _wrap_text(cell, "Helvetica", 10, max_w)
                wrapped_cells.append(lines)
                row_height = max(row_height, len(lines) * base_row_h)

            cx = x
            for i, lines in enumerate(wrapped_cells):
                line_y = y_cursor
                for line in lines:
                    if i in money_cols:
                        right_text(cx + col_widths[i] - 6, line_y, line, "Helvetica", 10)
                    else:
                        pdf.drawString(cx + 6, line_y, line)
                    line_y -= base_row_h
                cx += col_widths[i]

            pdf.setStrokeColor(line_color)
            pdf.line(x, y_cursor - 4, x + table_w, y_cursor - 4)
            y_cursor -= row_height

        pdf.setStrokeColor(colors.black)
        return y_cursor - 6

    body_y = strip_y - strip_h - 0.35 * inch

    labor_rows = []
    rate = float(inv.price_per_hour or 0.0)
    for li in inv.labor_items:
        try:
            t = float(li.labor_time_hours or 0.0)
        except Exception:
            t = 0.0
        line_total = t * rate
        labor_rows.append([
            li.labor_desc or "",
            f"{t:g} {cfg.get('hours_suffix', 'hrs')}" if t else "",
            _money(line_total) if line_total else ""
        ])

    if show_labor:
        body_y = draw_table(
            cfg["labor_title"],
            M,
            body_y,
            [cfg["labor_desc_label"], cfg.get("labor_time_label", "Time"), cfg.get("labor_total_label", "Line Total")],
            labor_rows,
            col_widths=[PAGE_W - 2 * M - 190, 90, 100],
            money_cols={2}
        )

    parts_rows = []
    for p in inv.parts:
        parts_rows.append([
            p.part_name or "",
            _money(inv.part_price_with_markup(p.part_price or 0.0)) if (p.part_price or 0.0) else ""
        ])

    has_parts_rows = any((row[0] or row[1]) for row in parts_rows)
    if show_parts and has_parts_rows:
        body_y = draw_table(
            cfg["parts_title"],
            M,
            body_y - 10,
            [cfg["parts_name_label"], cfg.get("parts_price_label", "Price")],
            parts_rows,
            col_widths=[PAGE_W - 2 * M - 120, 120],
            money_cols={1}
        )

    # Optional notes block (left side, below tables)
    if show_notes and (inv.notes or "").strip():
        notes_x = M
        notes_w = max(2.2 * inch, PAGE_W - (3 * M) - 2.5 * inch)
        notes_y_top = max(body_y - 8, 2.0 * inch + M)
        notes_h = 1.35 * inch
        pdf.setStrokeColor(line_color)
        pdf.roundRect(notes_x, notes_y_top - notes_h, notes_w, notes_h, 8, stroke=1, fill=0)
        pdf.setFont("Helvetica-Bold", 10)
        pdf.setFillColor(colors.black)
        pdf.drawString(notes_x + 10, notes_y_top - 16, "Notes")
        pdf.setFont("Helvetica", 9)
        y_notes = notes_y_top - 32
        for ln in _wrap_text(inv.notes or "", "Helvetica", 9, notes_w - 20)[:5]:
            pdf.drawString(notes_x + 10, y_notes, ln)
            y_notes -= 12

    # Summary block at bottom right
    total_parts = inv.parts_total() if show_parts else 0.0
    total_labor = inv.labor_total() if show_labor else 0.0
    tax_amount = inv.tax_amount()
    price_owed = inv.amount_due()

    sum_w = 2.5 * inch
    sum_x = PAGE_W - M - sum_w
    row_count = 1  # total
    if show_labor:
        row_count += 1
    if show_parts and has_parts_rows and total_parts:
        row_count += 1
    if show_shop_supplies and inv.shop_supplies:
        row_count += 1
    if tax_amount:
        row_count += 1
    if not is_estimate:
        row_count += 1  # paid
        row_count += 1  # amount due
    sum_h = max(1.2 * inch, (0.48 * inch + (row_count * 0.22 * inch)))
    sum_y = max(body_y - 12, (sum_h + 0.4 * inch))
    pdf.setStrokeColor(line_color)
    pdf.roundRect(sum_x, sum_y - sum_h, sum_w, sum_h, 8, stroke=1, fill=0)
    pdf.setFont("Helvetica-Bold", 10)
    pdf.drawString(sum_x + 10, sum_y - 16, "Summary")

    y = sum_y - 36
    right_edge = sum_x + sum_w - 10
    if show_labor:
        label_right_value(sum_x + 10, right_edge, y, f"{cfg['labor_title']}:", _money(total_labor)); y -= 14
    if show_parts and has_parts_rows and total_parts:
        label_right_value(sum_x + 10, right_edge, y, f"{cfg['parts_title']}:", _money(total_parts)); y -= 14
    if show_shop_supplies and inv.shop_supplies:
        label_right_value(sum_x + 10, right_edge, y, f"{cfg['shop_supplies_label']}:", _money(inv.shop_supplies)); y -= 14
    if tax_amount:
        label_right_value(sum_x + 10, right_edge, y, f"{_tax_label(inv)}:", _money(tax_amount)); y -= 14

    pdf.setStrokeColor(line_color)
    pdf.line(sum_x + 10, y + 4, sum_x + sum_w - 10, y + 4)
    pdf.setStrokeColor(colors.black)
    y -= 8
    label = "Estimated Total:" if is_estimate else "Total:"
    label_right_value(sum_x + 10, right_edge, y, label, _money(total_price)); y -= 16
    if not is_estimate:
        label_right_value(sum_x + 10, right_edge, y, "Paid:", _money(inv.paid)); y -= 16
        label_right_value(sum_x + 10, right_edge, y, "Amount Due:", _money(price_owed))

    # Footer
    pdf.setFont("Helvetica-Oblique", 9)
    pdf.setFillColor(muted)
    if is_estimate:
        pdf.drawString(M, 0.55 * inch, "Total is an estimated cost of service. Actual amount may differ.")
    else:
        pdf.drawString(M, 0.55 * inch, "Thank you for your business.")
    pdf.setFillColor(colors.black)

    pdf.save()
    inv.pdf_path = pdf_path
    inv.pdf_generated_at = generated_dt
    session.add(inv)
    session.commit()

    return pdf_path


def generate_and_store_pdf(
    session,
    invoice_id: int,
    custom_cfg_override: dict | None = None,
    pdf_template_override: str | None = None,
    builder_cfg_override: dict | None = None,
) -> str:
    """
    Generates (or regenerates) a PDF for the given invoice_id.
    Saves to disk (Option A) and updates invoice.pdf_path + invoice.pdf_generated_at.

    Returns: absolute pdf path on disk.
    """
    inv = session.get(Invoice, invoice_id)
    if not inv:
        raise ValueError(f"Invoice not found: id={invoice_id}")

    is_estimate = bool(getattr(inv, "is_estimate", False))

    # Pull the invoice owner's profile (business header fields)
    owner = None
    try:
        if getattr(inv, "user_id", None):
            owner = session.get(User, inv.user_id)
    except Exception:
        owner = None

    # Pull customer (for Bill To)
    customer = None
    try:
        if getattr(inv, "customer_id", None):
            customer = session.get(Customer, inv.customer_id)
    except Exception:
        customer = None

    # Customer contact priority:
    # invoice override -> customer profile
    customer_email = (getattr(inv, "customer_email", None) or "").strip() or (
        (getattr(customer, "email", None) or "").strip() if customer else ""
    )
    customer_phone = (getattr(inv, "customer_phone", None) or "").strip() or (
        (getattr(customer, "phone", None) or "").strip() if customer else ""
    )
    customer_phone = _format_phone(customer_phone)
    customer_address = ((getattr(customer, "address", None) or "").strip() if customer else "")

    # -----------------------------
    # Invoice template / profession config (locked per invoice)
    # -----------------------------
    TEMPLATE_CFG = {
        "auto_repair": {
            "job_label": "Vehicle",
            "job_box_title": "JOB DETAILS",
            "job_rate_label": "Rate/Hour",
            "job_hours_label": "Total Hours",
            "hours_suffix": "hrs",

            "labor_title": "Labor",
            "labor_desc_label": "Description",
            "labor_time_label": "Time",
            "labor_total_label": "Line Total",

            "parts_title": "Parts",
            "parts_name_label": "Part Name",
            "parts_price_label": "Price",

            "shop_supplies_label": "Shop Supplies",
        },
        "general_service": {
            "job_label": "Job / Project",
            "job_box_title": "JOB DETAILS",
            "job_rate_label": "Rate/Hour",
            "job_hours_label": "Total Hours",
            "hours_suffix": "hrs",

            "labor_title": "Services",
            "labor_desc_label": "Description",
            "labor_time_label": "Time",
            "labor_total_label": "Line Total",

            "parts_title": "Materials",
            "parts_name_label": "Material",
            "parts_price_label": "Price",

            "shop_supplies_label": "Supplies / Fees",
        },
        "accountant": {
            "job_label": "Engagement",
            "job_box_title": "ENGAGEMENT DETAILS",
            "job_rate_label": "Hourly Rate",
            "job_hours_label": "Hours Billed",
            "hours_suffix": "hrs",

            "labor_title": "Services",
            "labor_desc_label": "Description",
            "labor_time_label": "Hours",
            "labor_total_label": "Line Total",

            "parts_title": "Expenses",
            "parts_name_label": "Expense",
            "parts_price_label": "Amount",

            "shop_supplies_label": "Admin Fees",
        },
        "computer_repair": {
            "job_label": "Device",
            "job_box_title": "DEVICE DETAILS",
            "job_rate_label": "Rate/Hour",
            "job_hours_label": "Total Hours",
            "hours_suffix": "hrs",

            "labor_title": "Services",
            "labor_desc_label": "Description",
            "labor_time_label": "Time",
            "labor_total_label": "Line Total",

            "parts_title": "Parts",
            "parts_name_label": "Part Name",
            "parts_price_label": "Price",

            "shop_supplies_label": "Shop Supplies",
        },
        "lawn_care": {
            "job_label": "Service Address",
            "job_box_title": "PROPERTY DETAILS",
            "job_rate_label": "Rate",
            "job_hours_label": "Units / Hours",
            "hours_suffix": "hrs",

            "labor_title": "Services",
            "labor_desc_label": "Description",
            "labor_time_label": "Qty / Time",
            "labor_total_label": "Line Total",

            "parts_title": "Materials",
            "parts_name_label": "Material",
            "parts_price_label": "Amount",

            "shop_supplies_label": "Disposal / Trip Fees",
        },
        "flipping_items": {
            "job_label": "Item",
            "job_box_title": "ITEM DETAILS",
            "job_rate_label": "Sale Price",
            "job_hours_label": "Quantity",
            "hours_suffix": "qty",

            "labor_title": "Sales",
            "labor_desc_label": "Description",
            "labor_time_label": "Qty",
            "labor_total_label": "Line Total",

            "parts_title": "Costs",
            "parts_name_label": "Cost Item",
            "parts_price_label": "Amount",

            "shop_supplies_label": "Other Expenses",
        },
        "custom": {
            "job_label": "Job / Project",
            "job_box_title": "JOB DETAILS",
            "job_rate_label": "Rate/Hour",
            "job_hours_label": "Total Hours",
            "hours_suffix": "hrs",
            "profession_label": "Custom",
            "labor_title": "Services",
            "labor_desc_label": "Description",
            "labor_time_label": "Time",
            "labor_total_label": "Line Total",
            "parts_title": "Items",
            "parts_name_label": "Item Name",
            "parts_price_label": "Price",
            "shop_supplies_label": "Additional Fees",
            "show_job": True,
            "show_labor": True,
            "show_parts": True,
            "show_shop_supplies": True,
            "show_notes": True,
        },
    }

    template_key = (getattr(inv, "invoice_template", None) or "").strip() or (
        (getattr(owner, "invoice_template", None) or "").strip() if owner else ""
    )
    if template_key not in TEMPLATE_CFG:
        template_key = "auto_repair"
    cfg = TEMPLATE_CFG[template_key]
    if template_key == "custom" and owner is not None:
        def _txt(attr: str, fallback: str) -> str:
            val = (getattr(owner, attr, None) or "").strip()
            return val or fallback

        cfg = dict(cfg)
        cfg["job_label"] = _txt("custom_job_label", cfg["job_label"])
        cfg["labor_title"] = _txt("custom_labor_title", cfg["labor_title"])
        cfg["labor_desc_label"] = _txt("custom_labor_desc_label", cfg["labor_desc_label"])
        cfg["parts_title"] = _txt("custom_parts_title", cfg["parts_title"])
        cfg["parts_name_label"] = _txt("custom_parts_name_label", cfg["parts_name_label"])
        cfg["shop_supplies_label"] = _txt("custom_shop_supplies_label", cfg["shop_supplies_label"])
        cfg["profession_label"] = _txt("custom_profession_name", cfg.get("profession_label", "Custom"))
        cfg["show_job"] = bool(getattr(owner, "custom_show_job", True))
        cfg["show_labor"] = bool(getattr(owner, "custom_show_labor", True))
        cfg["show_parts"] = bool(getattr(owner, "custom_show_parts", True))
        cfg["show_shop_supplies"] = bool(getattr(owner, "custom_show_shop_supplies", True))
        cfg["show_notes"] = bool(getattr(owner, "custom_show_notes", True))
    if custom_cfg_override:
        cfg = dict(cfg)
        for k, v in custom_cfg_override.items():
            if k in cfg:
                cfg[k] = v
    builder_cfg = _invoice_builder_cfg(owner, override=builder_cfg_override)

    show_job = bool(cfg.get("show_job", True))
    show_labor = bool(cfg.get("show_labor", True))
    show_parts = bool(cfg.get("show_parts", True))
    show_shop_supplies = bool(cfg.get("show_shop_supplies", True))
    show_notes = bool(cfg.get("show_notes", True))

    requested_pdf_template = (pdf_template_override or "").strip()
    if requested_pdf_template:
        pdf_template_key = _pdf_template_key_fallback(requested_pdf_template)
    else:
        owner_pdf_template = (getattr(owner, "pdf_template", None) or "").strip() if owner else ""
        inv_pdf_template = (getattr(inv, "pdf_template", None) or "").strip()
        if owner_pdf_template:
            inv.pdf_template = owner_pdf_template
        pdf_template_key = _pdf_template_key_fallback(owner_pdf_template or inv_pdf_template)

    # Determine header identity lines (left side)
    business_name = (getattr(owner, "business_name", None) or "").strip() if owner else ""
    username = (getattr(owner, "username", None) or "").strip() if owner else ""
    header_name = business_name or username or ""

    header_address_lines = _owner_address_lines(owner)
    header_phone = (getattr(owner, "phone", None) or "").strip() if owner else ""

    # Owner logo (stored relative to instance/)
    owner_logo_rel = (getattr(owner, "logo_path", None) or "").strip() if owner else ""
    owner_logo_abs = ""
    owner_logo_blob = (getattr(owner, "logo_blob", None) if owner else None)
    if owner_logo_rel:
        owner_logo_abs = str((Path("instance") / owner_logo_rel).resolve())

    # Ensure parts + labor loaded
    parts = inv.parts
    labor_items = inv.labor_items

    PAGE_W, PAGE_H = LETTER
    M = 0.75 * inch

    owner_offset_minutes = int(getattr(owner, "schedule_summary_tz_offset_minutes", 0) or 0) if owner else 0
    owner_offset_minutes = max(-720, min(840, owner_offset_minutes))
    generated_dt = datetime.utcnow() + timedelta(minutes=owner_offset_minutes)
    generated_str = generated_dt.strftime("%B %d, %Y")

    display_no = inv.display_number or inv.invoice_number

    # Year from invoice_number prefix (YYYY######)
    year = (inv.invoice_number or "")[:4]
    if not (len(year) == 4 and year.isdigit()):
        year = generated_dt.strftime("%Y")

    exports_dir = Config.EXPORTS_DIR
    year_dir = os.path.join(exports_dir, year)
    os.makedirs(year_dir, exist_ok=True)

    pdf_filename = f"{_safe_filename(inv.invoice_number)}.pdf"
    pdf_path = os.path.abspath(os.path.join(year_dir, pdf_filename))

    doc_label = "ESTIMATE" if is_estimate else "INVOICE"

    # Advanced drag/drop invoice builder output (active template).
    if builder_cfg.get("enabled", False) and owner is not None:
        active_design = (
            session.query(InvoiceDesignTemplate)
            .filter(
                InvoiceDesignTemplate.user_id == owner.id,
                InvoiceDesignTemplate.is_active.is_(True),
            )
            .order_by(InvoiceDesignTemplate.updated_at.desc(), InvoiceDesignTemplate.id.desc())
            .first()
        )
        if active_design and (active_design.design_json or "").strip():
            try:
                design_obj = json.loads(active_design.design_json)
                return _render_invoice_builder_pdf(
                    session=session,
                    inv=inv,
                    owner=owner,
                    customer=customer,
                    pdf_path=pdf_path,
                    generated_dt=generated_dt,
                    is_estimate=is_estimate,
                    design_obj=design_obj,
                )
            except Exception:
                pass

    if pdf_template_key == "modern":
        return _render_modern_pdf(
            session=session,
            inv=inv,
            owner=owner,
            customer=customer,
            customer_email=customer_email,
            customer_phone=customer_phone,
            customer_address=customer_address,
            cfg=cfg,
            template_key=template_key,
            pdf_path=pdf_path,
            display_no=display_no,
            doc_label=doc_label,
            generated_dt=generated_dt,
            generated_str=generated_str,
            owner_logo_abs=owner_logo_abs,
            owner_logo_blob=owner_logo_blob,
            is_estimate=is_estimate,
            builder_cfg=builder_cfg,
        )
    if pdf_template_key == "split_panel":
        return _render_split_panel_pdf(
            session=session,
            inv=inv,
            owner=owner,
            customer=customer,
            customer_email=customer_email,
            customer_phone=customer_phone,
            customer_address=customer_address,
            cfg=cfg,
            template_key=template_key,
            pdf_path=pdf_path,
            display_no=display_no,
            doc_label=doc_label,
            generated_dt=generated_dt,
            generated_str=generated_str,
            owner_logo_abs=owner_logo_abs,
            owner_logo_blob=owner_logo_blob,
            is_estimate=is_estimate,
            builder_cfg=builder_cfg,
        )
    if pdf_template_key == "strip":
        return _render_strip_pdf(
            session=session,
            inv=inv,
            owner=owner,
            customer=customer,
            customer_email=customer_email,
            customer_phone=customer_phone,
            customer_address=customer_address,
            cfg=cfg,
            template_key=template_key,
            pdf_path=pdf_path,
            display_no=display_no,
            doc_label=doc_label,
            generated_dt=generated_dt,
            generated_str=generated_str,
            owner_logo_abs=owner_logo_abs,
            owner_logo_blob=owner_logo_blob,
            is_estimate=is_estimate,
            builder_cfg=builder_cfg,
        )

    pdf = canvas.Canvas(pdf_path, pagesize=LETTER)
    pdf.setTitle(f"{doc_label.title()} - {display_no}")
    builder_enabled = bool(builder_cfg.get("enabled", False)) if isinstance(builder_cfg, dict) else False
    builder_accent = colors.HexColor(
        (builder_cfg.get("accent_color") if isinstance(builder_cfg, dict) else None) or "#0f172a"
    )
    builder_header_style = (
        (builder_cfg.get("header_style") if isinstance(builder_cfg, dict) else None) or "classic"
    ).strip().lower()
    builder_compact_mode = bool(builder_cfg.get("compact_mode", False)) if isinstance(builder_cfg, dict) else False

    # -----------------------------
    # Helpers bound to this canvas
    # -----------------------------
    def right_text(x, y, text, font="Helvetica", size=10):
        pdf.setFont(font, size)
        w = pdf.stringWidth(str(text), font, size)
        pdf.drawString(x - w, y, str(text))

    def label_value(x, y, label, value, label_font=("Helvetica-Bold", 9), value_font=("Helvetica", 10)):
        pdf.setFont(*label_font)
        pdf.drawString(x, y, label)
        pdf.setFont(*value_font)
        pdf.drawString(x + 70, y, str(value))

    # ✅ NEW: label on left, value right-aligned to a right edge
    def label_right_value(x_left, x_right, y, label, value, label_font=("Helvetica-Bold", 9), value_font=("Helvetica", 10)):
        pdf.setFont(*label_font)
        pdf.drawString(x_left, y, label)
        right_text(x_right, y, str(value), value_font[0], value_font[1])

    def start_new_page_with_header():
        pdf.showPage()
        pdf.setFillColorRGB(0, 0, 0)
        pdf.setFont("Helvetica-Bold", 14)
        pdf.drawString(M, PAGE_H - M, f"{doc_label} (cont.)")
        pdf.setFont("Helvetica", 10)
        pdf.drawString(M, PAGE_H - M - 16, f"{display_no}  •  Generated: {generated_str}")

    # -----------------------------
    # Header (classic printer-friendly)
    # -----------------------------
    header_h = 1.35 * inch
    header_fill = colors.white
    header_text_color = colors.black
    divider_color = colors.HexColor("#CCCCCC")
    if builder_enabled and builder_header_style == "banded":
        header_fill = builder_accent
        header_text_color = colors.white
        divider_color = builder_accent
    pdf.setFillColor(header_fill)
    pdf.rect(0, PAGE_H - header_h, PAGE_W, header_h, stroke=0, fill=1)
    pdf.setStrokeColor(divider_color)
    pdf.setLineWidth(1)
    pdf.line(M, PAGE_H - header_h - 0.02 * inch, PAGE_W - M, PAGE_H - header_h - 0.02 * inch)

    # Optional logo (same placement behavior as modern)
    logo_w = 0
    if owner_logo_blob:
        try:
            img = ImageReader(io.BytesIO(owner_logo_blob))
            iw, ih = img.getSize()
            max_h = 0.5 * inch
            max_w = 1.4 * inch
            scale = min(max_w / iw, max_h / ih)
            logo_w = iw * scale
            logo_h = ih * scale
            logo_x = M
            logo_y = PAGE_H - (header_h / 2) - (logo_h / 2)
            pdf.drawImage(img, logo_x, logo_y, width=logo_w, height=logo_h, mask="auto")
        except Exception:
            logo_w = 0
    elif owner_logo_abs and os.path.exists(owner_logo_abs):
        try:
            img = ImageReader(owner_logo_abs)
            iw, ih = img.getSize()
            max_h = 0.5 * inch
            max_w = 1.4 * inch
            scale = min(max_w / float(iw), max_h / float(ih))
            logo_w = float(iw) * scale
            logo_h = float(ih) * scale
            logo_x = M
            logo_y = PAGE_H - (header_h / 2) - (logo_h / 2)
            pdf.drawImage(img, logo_x, logo_y, width=logo_w, height=logo_h, mask="auto")
        except Exception:
            logo_w = 0

    # Business info (left)
    left_x = M + (logo_w + 10 if logo_w else 0)
    pdf.setFillColor(header_text_color)
    pdf.setFont("Helvetica-Bold", 12)
    pdf.drawString(left_x, PAGE_H - 0.55 * inch, header_name or "InvoiceRunner")

    pdf.setFont("Helvetica", 9)
    info_lines = []
    for addr_line in header_address_lines:
        info_lines.extend(_wrap_text(addr_line, "Helvetica", 9, 3.6 * inch))
    if header_phone:
        info_lines.append(header_phone)
    info_y = PAGE_H - 0.82 * inch
    for ln in info_lines[:2]:
        pdf.drawString(left_x, info_y, ln)
        info_y -= 12

    # Meta (right)
    meta_x = PAGE_W - M
    right_text(meta_x, PAGE_H - 0.48 * inch, doc_label, "Helvetica-Bold", 18)
    right_text(meta_x, PAGE_H - 0.80 * inch, f"{doc_label.title()} #: {display_no}", "Helvetica", 10)
    right_text(meta_x, PAGE_H - 1.02 * inch, f"Date: {inv.date_in}", "Helvetica", 10)
    due_line = _invoice_due_date_line(inv, owner, is_estimate=is_estimate)
    if due_line:
        right_text(meta_x, PAGE_H - 1.24 * inch, due_line, "Helvetica", 9)
    pdf.setFillColor(colors.black)

    # -----------------------------
    # Bill To + Job Details boxes
    # -----------------------------
    top_y = PAGE_H - header_h - 0.35 * inch
    box_w = (PAGE_W - 2 * M - 0.35 * inch) / 2
    line_step = 12
    name_start_y = top_y - 34
    max_w_left = (box_w / 2) - 20
    max_w_right = (box_w / 2) - 20

    # Prefer Customer.name when available (otherwise invoice legacy name parsing)
    if customer and (getattr(customer, "name", None) or "").strip():
        customer_name = (customer.name or "").strip()
    else:
        nameFirst, _, _tail = (inv.name or "").partition(":")
        customer_name = (nameFirst or inv.name or "").strip()

    # Build Bill To stacked lines (strip-style content inside classic box).
    addr_lines = []
    addr_text = (customer_address or "").strip()
    if addr_text:
        addr_lines.extend(_wrap_text(addr_text, "Helvetica", 11, box_w - 20))
    name_lines = _wrap_text(customer_name, "Helvetica", 11, box_w - 20)
    email_lines = _wrap_text(f"Email: {customer_email}", "Helvetica", 11, box_w - 20) if customer_email else []
    phone_lines = _wrap_text(f"Phone: {customer_phone}", "Helvetica", 11, box_w - 20) if customer_phone else []

    bill_lines = []
    bill_lines.extend(name_lines[:2])
    if addr_lines:
        bill_lines.extend(addr_lines[:2])
    if email_lines:
        bill_lines.extend(email_lines[:2])
    if phone_lines:
        bill_lines.extend(phone_lines[:2])

    # Keep all Bill To lines visible inside the box.
    min_box_h = 1.35 * inch
    required_box_h = 32 + (max(1, len(bill_lines)) * line_step)
    box_h = max(min_box_h, required_box_h)

    pdf.setStrokeColor(colors.black)
    pdf.setLineWidth(1)

    # Bill To
    pdf.roundRect(M, top_y - box_h, box_w, box_h, 8, stroke=1, fill=0)
    pdf.setFont("Helvetica-Bold", 11)
    pdf.drawString(M + 10, top_y - 16, "BILL TO")

    left_x = M + 10
    y_bottom = top_y - box_h + 12

    # Stacked lines like strip layout, while staying inside classic box.
    pdf.setFont("Helvetica", 11)
    y_line = name_start_y
    for ln in bill_lines:
        if y_line < y_bottom:
            break
        pdf.drawString(left_x, y_line, ln)
        y_line -= line_step

    
    # Job Details (wrapped job/address line)
    x2 = M + box_w + 0.35 * inch
    pdf.roundRect(x2, top_y - box_h, box_w, box_h, 8, stroke=1, fill=0)

    pdf.setFont("Helvetica-Bold", 11)
    pdf.drawString(x2 + 10, top_y - 18, cfg.get("job_box_title", "JOB DETAILS"))

    pdf.setFont("Helvetica", 10)

    job_text = f"{cfg['job_label']}: {inv.vehicle or ''}" if show_job else ""
    max_job_w = box_w - 20
    job_lines = _wrap_text(job_text, "Helvetica", 10, max_job_w) if show_job else []

    y_job = top_y - 32
    line_step = 14

    for ln in job_lines[:2]:  # limit so rate/hours still fit
        pdf.drawString(x2 + 10, y_job, ln)
        y_job -= line_step

    if template_key == "flipping_items":
        pdf.drawString(x2 + 10, y_job, f"Profit: {_money(inv.labor_total())}")
        y_job -= line_step
        pdf.drawString(x2 + 10, y_job, f"Sold For: {_money(inv.paid)}")
    else:
        pdf.drawString(x2 + 10, y_job, f"{cfg['job_rate_label']}: {_money(inv.price_per_hour)}")
        y_job -= line_step

        pdf.drawString(
            x2 + 10,
            y_job,
            f"{cfg['job_hours_label']}: {inv.hours} {cfg.get('hours_suffix', 'hrs')}"
        )


    # -----------------------------
    # Table drawer (wrap + dynamic row heights)
    # -----------------------------
    def draw_table(title, x, y_top, col_titles, rows, col_widths, money_cols=None):
        money_cols = set(money_cols or [])
        base_row_h = 14 if builder_compact_mode else 16
        title_gap = 16

        pdf.setFont("Helvetica-Bold", 12)
        pdf.drawString(x, y_top, title)
        y = y_top - title_gap

        table_w = sum(col_widths)

        # Header background
        pdf.setFillColorRGB(0.95, 0.95, 0.95)
        pdf.rect(x, y - base_row_h + 10, table_w, base_row_h, stroke=0, fill=1)
        pdf.setFillColorRGB(0, 0, 0)
        pdf.setFont("Helvetica-Bold", 10)

        cx = x
        for i, h in enumerate(col_titles):
            if i in money_cols:
                right_text(cx + col_widths[i] - 6, y, h, "Helvetica-Bold", 10)
            else:
                pdf.drawString(cx + 6, y, h)
            cx += col_widths[i]

        pdf.setStrokeColor(colors.black)
        pdf.setLineWidth(0.5)
        pdf.line(x, y - 4, x + table_w, y - 4)

        y_cursor = y - base_row_h
        pdf.setFont("Helvetica", 10)

        for row in rows:
            wrapped_cells = []
            row_height = base_row_h

            for i, cell in enumerate(row):
                max_w = col_widths[i] - 12
                lines = _wrap_text(cell, "Helvetica", 10, max_w)
                wrapped_cells.append(lines)
                row_height = max(row_height, len(lines) * base_row_h)

            cx = x
            for i, lines in enumerate(wrapped_cells):
                line_y = y_cursor
                for line in lines:
                    if i in money_cols:
                        right_text(cx + col_widths[i] - 6, line_y, line, "Helvetica", 10)
                    else:
                        pdf.drawString(cx + 6, line_y, line)
                    line_y -= base_row_h
                cx += col_widths[i]

            pdf.setStrokeColor(colors.HexColor("#DDDDDD"))
            pdf.line(x, y_cursor - 4, x + table_w, y_cursor - 4)
            y_cursor -= row_height

        pdf.setStrokeColor(colors.black)
        return y_cursor - 6

    body_y = top_y - box_h - 0.5 * inch

    # -----------------------------
    # Labor table
    # -----------------------------
    labor_rows = []
    rate = float(inv.price_per_hour or 0.0)
    for li in labor_items:
        try:
            t = float(li.labor_time_hours or 0.0)
        except Exception:
            t = 0.0
        line_total = t * rate
        labor_rows.append([
            li.labor_desc or "",
            f"{t:g} {cfg.get('hours_suffix', 'hrs')}" if t else "",
            _money(line_total) if line_total else ""
        ])

    if show_labor:
        body_y = draw_table(
            cfg["labor_title"],
            M,
            body_y,
            [cfg["labor_desc_label"], cfg.get("labor_time_label", "Time"), cfg.get("labor_total_label", "Line Total")],
            labor_rows,
            col_widths=[PAGE_W - 2 * M - 190, 90, 100],
            money_cols={2}
        )

    # -----------------------------
    # Parts / Materials table
    # -----------------------------
    parts_rows = []
    for p in parts:
        parts_rows.append([
            p.part_name or "",
            _money(inv.part_price_with_markup(p.part_price or 0.0)) if (p.part_price or 0.0) else ""
        ])

    has_parts_rows = any((row[0] or row[1]) for row in parts_rows)
    if show_parts and has_parts_rows:
        body_y = draw_table(
            cfg["parts_title"],
            M,
            body_y - 10,
            [cfg["parts_name_label"], cfg.get("parts_price_label", "Price")],
            parts_rows,
            col_widths=[PAGE_W - 2 * M - 120, 120],
            money_cols={1}
        )

    # -----------------------------
    # Notes + Summary boxes
    # -----------------------------
    notes_box_w = PAGE_W - 2 * M - 250
    notes_y_top = max(body_y - 10, 2.2 * inch + M)

    pdf.setFont("Helvetica", 10)
    left_padding = 10
    right_padding = 10
    line_height = 14
    SPACER_GAP = 3
    header_title_gap = 44
    bottom_padding = 12

    max_width = notes_box_w - left_padding - right_padding
    all_note_lines = _split_notes_into_lines(inv.notes or "", max_width, font="Helvetica", size=10) if show_notes else []

    footer_y = 0.55 * inch
    footer_clearance = 0.20 * inch
    page_bottom_limit = footer_y + footer_clearance

    needed_text_h = 0
    for ln in all_note_lines:
        needed_text_h += SPACER_GAP if ln == "__SPACER__" else line_height
    needed_box_h = header_title_gap + needed_text_h + bottom_padding

    max_box_h_this_page = notes_y_top - page_bottom_limit
    notes_box_h = min(max_box_h_this_page, needed_box_h) if show_notes else 0

    remaining_lines = []
    if show_notes:
        pdf.roundRect(M, notes_y_top - notes_box_h, notes_box_w, notes_box_h, 8, stroke=1, fill=0)
        pdf.setFont("Helvetica-Bold", 11)
        pdf.drawString(M + 10, notes_y_top - 18, "NOTES")

        pdf.setFont("Helvetica", 10)
        y_note = notes_y_top - header_title_gap
        bottom_limit = notes_y_top - notes_box_h + bottom_padding

        lines_fit = 0
        for line in all_note_lines:
            if y_note < bottom_limit:
                break
            if line == "__SPACER__":
                y_note -= SPACER_GAP
            else:
                pdf.drawString(M + left_padding, y_note, line)
                y_note -= line_height
            lines_fit += 1

        remaining_lines = all_note_lines[lines_fit:]

    # Summary box
    sum_x = PAGE_W - M - 240
    sum_w = 240
    total_parts = inv.parts_total() if show_parts else 0.0
    total_labor = inv.labor_total() if show_labor else 0.0
    total_price = inv.invoice_total()
    tax_amount = inv.tax_amount()
    price_owed = inv.amount_due()

    summary_rows = 1  # Total / Estimated Total
    if show_parts and has_parts_rows and total_parts:
        summary_rows += 1
    if show_labor:
        summary_rows += 1
    if show_shop_supplies and inv.shop_supplies:
        summary_rows += 1
    if tax_amount:
        summary_rows += 1
    if not is_estimate:
        summary_rows += 1  # Paid
    sum_h = max(1.8 * inch, (0.58 * inch + (summary_rows * 0.23 * inch)))
    pdf.roundRect(sum_x, notes_y_top - sum_h, sum_w, sum_h, 8, stroke=1, fill=0)

    pdf.setFont("Helvetica-Bold", 11)
    pdf.drawString(sum_x + 10, notes_y_top - 18, "SUMMARY")

    # ✅ Right-align all $ values to the right edge of the summary box
    right_edge = sum_x + sum_w - 12
    y = notes_y_top - 42

    if show_parts and has_parts_rows and total_parts:
        label_right_value(sum_x + 10, right_edge, y, f"{cfg['parts_title']}:", _money(total_parts)); y -= 16
    if show_labor:
        label_right_value(sum_x + 10, right_edge, y, f"{cfg['labor_title']}:", _money(total_labor)); y -= 16
    if show_shop_supplies and inv.shop_supplies:
        label_right_value(sum_x + 10, right_edge, y, f"{cfg['shop_supplies_label']}:", _money(inv.shop_supplies)); y -= 16
    if tax_amount:
        label_right_value(sum_x + 10, right_edge, y, f"{_tax_label(inv)}:", _money(tax_amount)); y -= 16

    pdf.setStrokeColor(colors.HexColor("#DDDDDD"))
    pdf.line(sum_x + 10, y + 4, sum_x + sum_w - 10, y + 4)
    pdf.setStrokeColor(colors.black)
    y -= 10

    label = "Estimated Total:" if is_estimate else "Total:"
    label_right_value(sum_x + 10, right_edge, y, label, _money(total_price)); y -= 18

    if not is_estimate:
        label_right_value(sum_x + 10, right_edge, y, "Paid:", _money(inv.paid)); y -= 18

    # Amount Due / Profit below the box
    pdf.setFont("Helvetica-Bold", 13)
    if is_estimate:
        pdf.setFillColorRGB(0, 0, 0)
        right_text(sum_x + sum_w - 12, (notes_y_top - sum_h) - 28, f"ESTIMATED TOTAL: {_money(total_price)}", "Helvetica-Bold", 13)
    elif price_owed < 0:
        profit = abs(price_owed)
        pdf.setFillColorRGB(0.10, 0.55, 0.25)
        right_text(sum_x + sum_w - 12, (notes_y_top - sum_h) - 28, f"PROFIT: {_money(profit)}", "Helvetica-Bold", 13)
    else:
        pdf.setFillColorRGB(0, 0, 0)
        right_text(sum_x + sum_w - 12, (notes_y_top - sum_h) - 28, f"AMOUNT DUE: {_money(price_owed)}", "Helvetica-Bold", 13)
    pdf.setFillColorRGB(0, 0, 0)

    # -----------------------------
    # Continuation pages for notes
    # -----------------------------
    def footer():
        pdf.setFont("Helvetica-Oblique", 9)
        pdf.setFillColor(colors.grey)
        if is_estimate:
            pdf.drawString(M, footer_y, "Total is an estimated cost of service. Actual amount may differ.")
        else:
            pdf.drawString(M, footer_y, "Thank you for your business.")
        pdf.setFillColorRGB(0, 0, 0)

    if show_notes and remaining_lines:
        footer()
        while remaining_lines:
            start_new_page_with_header()

            notes_y_top_2 = PAGE_H - (M + 0.6 * inch)
            page_bottom_limit_2 = footer_y + footer_clearance
            notes_box_h_2 = notes_y_top_2 - page_bottom_limit_2

            pdf.roundRect(M, notes_y_top_2 - notes_box_h_2, notes_box_w, notes_box_h_2, 8, stroke=1, fill=0)
            pdf.setFont("Helvetica-Bold", 11)
            pdf.drawString(M + 10, notes_y_top_2 - 18, "NOTES (cont.)")

            pdf.setFont("Helvetica", 10)
            y_note2 = notes_y_top_2 - header_title_gap
            bottom2 = notes_y_top_2 - notes_box_h_2 + bottom_padding

            fit2 = 0
            for line in remaining_lines:
                if y_note2 < bottom2:
                    break
                if line == "__SPACER__":
                    y_note2 -= SPACER_GAP
                else:
                    pdf.drawString(M + left_padding, y_note2, line)
                    y_note2 -= line_height
                fit2 += 1

            remaining_lines = remaining_lines[fit2:]
            footer()
    else:
        footer()

    pdf.save()

    # Update DB record
    inv.pdf_path = pdf_path
    inv.pdf_generated_at = generated_dt
    session.add(inv)
    session.commit()

    return pdf_path


def generate_profit_loss_pdf(
    *,
    owner: User | None,
    period_label: str,
    income_total: float,
    expense_lines: list[tuple[str, float]],
) -> str:
    now = datetime.utcnow()
    year = now.strftime("%Y")
    out_dir = os.path.join(Config.EXPORTS_DIR, "profit_loss", year)
    os.makedirs(out_dir, exist_ok=True)

    owner_name = (getattr(owner, "business_name", None) or "").strip() or (getattr(owner, "username", None) or "InvoiceRunner")
    stamp = now.strftime("%Y%m%d_%H%M%S")
    fname = _safe_filename(f"profit_loss_{owner_name}_{stamp}") + ".pdf"
    pdf_path = os.path.join(out_dir, fname)

    total_expenses = sum(float(v or 0.0) for _, v in expense_lines)
    profit_or_loss = float(income_total or 0.0) - total_expenses

    PAGE_W, PAGE_H = LETTER
    M = 0.75 * inch
    pdf = canvas.Canvas(pdf_path, pagesize=LETTER)
    pdf.setTitle(f"Profit and Loss - {period_label}")

    def right_text(x, y, text, font="Helvetica", size=10):
        pdf.setFont(font, size)
        w = pdf.stringWidth(str(text), font, size)
        pdf.drawString(x - w, y, str(text))

    y = PAGE_H - M
    pdf.setFont("Helvetica-Bold", 17)
    pdf.drawString(M, y, "Profit & Loss Statement")
    y -= 24

    pdf.setFont("Helvetica", 11)
    pdf.drawString(M, y, f"For: {owner_name}")
    y -= 16
    pdf.drawString(M, y, f"Period: {period_label}")
    y -= 24

    pdf.setStrokeColor(colors.HexColor("#111827"))
    pdf.line(M, y, PAGE_W - M, y)
    y -= 18

    pdf.setFont("Helvetica-Bold", 12)
    pdf.drawString(M, y, "Income")
    y -= 18
    pdf.setFont("Helvetica", 10)
    pdf.drawString(M + 14, y, "Business Income")
    right_text(PAGE_W - M, y, _money(income_total), "Helvetica", 10)
    y -= 18
    pdf.setFont("Helvetica-Bold", 11)
    pdf.drawString(M + 14, y, "TOTAL INCOME")
    right_text(PAGE_W - M, y, _money(income_total), "Helvetica-Bold", 11)
    y -= 24

    pdf.setFont("Helvetica-Bold", 12)
    pdf.drawString(M, y, "Business Expenses")
    y -= 18

    pdf.setFont("Helvetica", 10)
    row_h = 14
    for label, amount in expense_lines:
        if y < M + 80:
            pdf.showPage()
            y = PAGE_H - M
            pdf.setFont("Helvetica-Bold", 12)
            pdf.drawString(M, y, "Business Expenses (cont.)")
            y -= 18
            pdf.setFont("Helvetica", 10)
        safe_label = (label or "").strip() or "Other Expense"
        pdf.drawString(M + 14, y, safe_label[:60])
        right_text(PAGE_W - M, y, _money(amount), "Helvetica", 10)
        y -= row_h

    y -= 4
    pdf.setStrokeColor(colors.HexColor("#111827"))
    pdf.line(M + 14, y, PAGE_W - M, y)
    y -= 16
    pdf.setFont("Helvetica-Bold", 11)
    pdf.drawString(M + 14, y, "TOTAL OPERATING EXPENSES")
    right_text(PAGE_W - M, y, _money(total_expenses), "Helvetica-Bold", 11)
    y -= 28

    pdf.setFont("Helvetica-Bold", 13)
    label = "PROFIT FROM BUSINESS" if profit_or_loss >= 0 else "LOSS FROM BUSINESS"
    pdf.drawString(M + 14, y, label)
    right_text(PAGE_W - M, y, _money(abs(profit_or_loss) if profit_or_loss < 0 else profit_or_loss), "Helvetica-Bold", 13)
    y -= 18

    if profit_or_loss < 0:
        pdf.setFont("Helvetica", 10)
        pdf.drawString(M + 14, y, "(negative result shown as loss)")

    footer_y = 0.58 * inch
    pdf.setFont("Helvetica-Oblique", 9)
    pdf.setFillColor(colors.grey)
    pdf.drawString(M, footer_y, f"Prepared: {now.strftime('%Y-%m-%d %H:%M UTC')}")
    pdf.setFillColor(colors.black)

    pdf.save()
    return pdf_path
def _invoice_builder_cfg(owner: User | None, override: dict | None = None) -> dict:
    raw_enabled = bool(getattr(owner, "invoice_builder_enabled", False)) if owner else False
    raw_accent = (getattr(owner, "invoice_builder_accent_color", None) or "#0f172a") if owner else "#0f172a"
    raw_header = (getattr(owner, "invoice_builder_header_style", None) or "classic") if owner else "classic"
    raw_compact = bool(getattr(owner, "invoice_builder_compact_mode", False)) if owner else False
    if override:
        raw_enabled = bool(override.get("enabled", raw_enabled))
        raw_accent = str(override.get("accent_color", raw_accent) or raw_accent)
        raw_header = str(override.get("header_style", raw_header) or raw_header)
        raw_compact = bool(override.get("compact_mode", raw_compact))

    accent = raw_accent.strip()
    if not re.fullmatch(r"#[0-9a-fA-F]{6}", accent):
        accent = "#0f172a"
    header_style = raw_header.strip().lower()
    if header_style not in ("classic", "banded"):
        header_style = "classic"
    return {
        "enabled": raw_enabled,
        "accent_color": accent,
        "header_style": header_style,
        "compact_mode": raw_compact,
    }
