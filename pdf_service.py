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
from reportlab.lib.styles import ParagraphStyle
from reportlab.platypus import Paragraph

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


def _show_business_name(owner: User | None) -> bool:
    return bool(getattr(owner, "show_business_name", True)) if owner else True


def _show_business_phone(owner: User | None) -> bool:
    return bool(getattr(owner, "show_business_phone", True)) if owner else True


def _show_business_address(owner: User | None) -> bool:
    return bool(getattr(owner, "show_business_address", True)) if owner else True


def _show_business_email(owner: User | None) -> bool:
    return bool(getattr(owner, "show_business_email", True)) if owner else True


def _business_header_name(owner: User | None) -> str:
    if not _show_business_name(owner):
        return ""
    business_name = (getattr(owner, "business_name", None) or "").strip() if owner else ""
    username = (getattr(owner, "username", None) or "").strip() if owner else ""
    return (business_name or username or "").strip()


def _business_header_display_name(owner: User | None, fallback: str = "InvoiceRunner") -> str:
    if not _show_business_name(owner):
        return ""
    return _business_header_name(owner) or fallback


def _business_header_info_lines(owner: User | None) -> list[str]:
    lines: list[str] = []
    if _show_business_address(owner):
        lines.extend(_owner_address_lines(owner))
    if _show_business_phone(owner):
        phone = _format_phone((getattr(owner, "phone", None) or "").strip()) if owner else ""
        if phone:
            lines.append(phone)
    if _show_business_email(owner):
        email = (getattr(owner, "email", None) or "").strip() if owner else ""
        if email:
            lines.append(email)
    return lines


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


def _invoice_due_date_utc(inv: Invoice, owner: User | None) -> datetime:
    raw_due_days = getattr(owner, "payment_due_days", None) if owner else None
    due_days = 30 if raw_due_days is None else int(raw_due_days)
    due_days = max(0, min(3650, due_days))
    created = getattr(inv, "created_at", None) or datetime.utcnow()
    return created + timedelta(days=due_days)


def _invoice_late_fee_amount(inv: Invoice, owner: User | None, *, as_of: datetime | None = None) -> float:
    if not owner or not bool(getattr(owner, "late_fee_enabled", False)):
        return 0.0
    if bool(getattr(inv, "is_estimate", False)):
        return 0.0
    if float(inv.amount_due() or 0.0) <= 0.0:
        return 0.0

    now_utc = as_of or datetime.utcnow()
    due_dt = _invoice_due_date_utc(inv, owner)
    tz_offset = int(getattr(owner, "schedule_summary_tz_offset_minutes", 0) or 0)
    tz_offset = max(-720, min(840, tz_offset))
    now_local = now_utc + timedelta(minutes=tz_offset)
    due_local = due_dt + timedelta(minutes=tz_offset)
    overdue_days = (now_local.date() - due_local.date()).days
    if overdue_days < 1:
        return 0.0

    frequency_days = int(getattr(owner, "late_fee_frequency_days", 30) or 30)
    frequency_days = max(1, min(365, frequency_days))
    cycles = 1 + ((overdue_days - 1) // frequency_days)

    mode = (getattr(owner, "late_fee_mode", "fixed") or "fixed").strip().lower()
    base_total = float(inv.invoice_total() or 0.0)
    if mode == "percent":
        pct = max(0.0, float(getattr(owner, "late_fee_percent", 0.0) or 0.0))
        fee_per_cycle = base_total * (pct / 100.0)
    else:
        fee_per_cycle = max(0.0, float(getattr(owner, "late_fee_fixed", 0.0) or 0.0))
    return round(max(0.0, fee_per_cycle * cycles), 2)


def _invoice_pdf_amounts(inv: Invoice, owner: User | None, *, is_estimate: bool) -> tuple[float, float, float]:
    total = float(inv.invoice_total() or 0.0)
    due = float(inv.amount_due() or 0.0)
    if is_estimate:
        return total, due, 0.0
    late_fee = _invoice_late_fee_amount(inv, owner)
    if late_fee > 0.0:
        total += late_fee
        due += late_fee
    return round(total, 2), round(max(0.0, due), 2), round(late_fee, 2)


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


def _wrap_text_preserve_spaces(text, font, size, max_width):
    raw = str(text or "")
    if raw == "":
        return [""]
    if max_width <= 0:
        return [raw]

    lines: list[str] = []
    current = ""

    def _append_token(token: str):
        nonlocal current
        if token == "":
            return
        if stringWidth(token, font, size) > max_width:
            if current:
                lines.append(current)
                current = ""
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
                lines.append(remaining[:fit])
                remaining = remaining[fit:]
            return

        test = current + token
        if current and stringWidth(test, font, size) > max_width:
            lines.append(current)
            current = token
        else:
            current = test

    # Keep runs of spaces as real tokens so user spacing is preserved.
    for token in re.findall(r"\s+|\S+", raw):
        _append_token(token)

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
    return key if key in {"classic", "modern", "split_panel", "strip", "basic", "simple", "blueprint", "luxe"} else "classic"


def _owner_has_pro_pdf_templates(owner: User | None) -> bool:
    if not owner:
        return False
    status = (getattr(owner, "subscription_status", None) or "").strip().lower()
    if status not in ("trialing", "active"):
        return False
    return (getattr(owner, "subscription_tier", None) or "").strip().lower() == "pro"


PRO_ONLY_PDF_TEMPLATES = {"basic", "simple", "blueprint", "luxe"}


def _resolve_imported_asset_token_path(src_raw: str) -> str:
    raw = str(src_raw or "").strip()
    prefix = "imported_asset:"
    if not raw.startswith(prefix):
        return ""
    rel = raw[len(prefix):].strip().lstrip("/")
    if not rel:
        return ""
    if ".." in rel.replace("\\", "/").split("/"):
        return ""
    base = (Path("instance") / "uploads" / "imported_templates").resolve()
    abs_path = (base / rel).resolve()
    if not str(abs_path).startswith(str(base) + os.sep):
        return ""
    if not abs_path.exists() or not abs_path.is_file():
        return ""
    return str(abs_path)


def _builder_template_vars(
    inv: Invoice,
    owner: User | None,
    customer: Customer | None,
    *,
    is_estimate: bool,
    cfg: dict | None = None,
) -> dict[str, str]:
    due_line = _invoice_due_date_line(inv, owner, is_estimate=is_estimate)
    due_date = ""
    if due_line:
        due_date = due_line.replace("Payment due date:", "").strip()
    business_name = (_business_header_name(owner) or "InvoiceRunner").strip()
    owner_phone = _format_phone((getattr(owner, "phone", None) or "").strip()) if _show_business_phone(owner) else ""
    owner_addr = "\n".join(_owner_address_lines(owner)) if _show_business_address(owner) else ""
    owner_email = (getattr(owner, "email", None) or "").strip() if _show_business_email(owner) else ""
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
    total_with_fees, due_with_fees, late_fee = _invoice_pdf_amounts(inv, owner, is_estimate=is_estimate)
    cfg = cfg or {}
    return {
        "doc_label": "ESTIMATE" if is_estimate else "INVOICE",
        "invoice_number": str(getattr(inv, "display_number", None) or inv.invoice_number or ""),
        "date": str(inv.date_in or ""),
        "due_date": due_date,
        "business_name": business_name,
        "business_phone": owner_phone,
        "business_address": owner_addr,
        "business_email": owner_email,
        "customer_name": customer_name,
        "customer_email": customer_email,
        "customer_phone": customer_phone,
        "job": str(getattr(inv, "vehicle", None) or ""),
        "rate": _money(rate_val),
        "hours": str(getattr(inv, "hours", 0.0) or 0.0),
        "labor_lines": "\n".join(labor_lines),
        "parts_lines": "\n".join(parts_lines),
        "notes_text": notes_text,
        "labor_table": "{{labor_table}}",
        "parts_table": "{{parts_table}}",
        "parts_total": _money(inv.parts_total()),
        "labor_total": _money(inv.labor_total()),
        "shop_supplies_amount": _money(float(getattr(inv, "shop_supplies", 0.0) or 0.0)),
        "shop_supplies": _money(float(getattr(inv, "shop_supplies", 0.0) or 0.0)),
        "tax": _money(inv.tax_amount()),
        "total": _money(total_with_fees),
        "late_fee": _money(late_fee),
        "paid": _money(getattr(inv, "paid", 0.0) or 0.0),
        "amount_due": _money(due_with_fees),
        "labor_title": str(cfg.get("labor_title") or "Labor"),
        "parts_title": str(cfg.get("parts_title") or "Parts"),
        "job_label": str(cfg.get("job_label") or "Job"),
        "shop_supplies_label": str(cfg.get("shop_supplies_label") or "Shop Supplies"),
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
    cfg: dict | None = None,
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

    vars_map = _builder_template_vars(inv, owner, customer, is_estimate=is_estimate, cfg=cfg)
    owner_logo_blob = getattr(owner, "logo_blob", None) if owner else None
    owner_logo_abs = ""
    owner_logo_rel = (getattr(owner, "logo_path", None) or "").strip() if owner else ""
    if owner_logo_rel:
        owner_logo_abs = str((Path("instance") / owner_logo_rel).resolve())
    logo_reader = None
    if owner_logo_blob:
        try:
            logo_reader = ImageReader(io.BytesIO(owner_logo_blob))
        except Exception:
            logo_reader = None
    if logo_reader is None and owner_logo_abs and os.path.exists(owner_logo_abs):
        try:
            logo_reader = ImageReader(owner_logo_abs)
        except Exception:
            logo_reader = None
    elements = design_obj.get("elements") if isinstance(design_obj, dict) else []
    if not isinstance(elements, list):
        elements = []
    elements_by_id: dict[str, dict] = {}
    for _el in elements:
        if isinstance(_el, dict) and _el.get("id") is not None:
            elements_by_id[str(_el.get("id"))] = _el

    def _map_y(y: float, h: float) -> float:
        return PAGE_H - ((y + h) * scale_y)

    def _builder_font_name(font_family: str, font_weight: int, font_style: str) -> str:
        fam = (font_family or "Helvetica").strip().lower()
        style = (font_style or "normal").strip().lower()
        is_bold = int(font_weight or 500) >= 600
        is_italic = style == "italic"

        if fam in {"times", "times-roman", "times new roman"}:
            if is_bold and is_italic:
                return "Times-BoldItalic"
            if is_bold:
                return "Times-Bold"
            if is_italic:
                return "Times-Italic"
            return "Times-Roman"
        if fam in {"courier", "courier new"}:
            if is_bold and is_italic:
                return "Courier-BoldOblique"
            if is_bold:
                return "Courier-Bold"
            if is_italic:
                return "Courier-Oblique"
            return "Courier"

        if is_bold and is_italic:
            return "Helvetica-BoldOblique"
        if is_bold:
            return "Helvetica-Bold"
        if is_italic:
            return "Helvetica-Oblique"
        return "Helvetica"

    def _inline_font_name_from_face(face: str) -> str:
        raw = (face or "").strip().lower()
        if "times" in raw:
            return "Times-Roman"
        if "courier" in raw or "mono" in raw:
            return "Courier"
        return "Helvetica"

    def _span_style_to_tags(style_text: str, inner_html: str) -> str:
        style_map: dict[str, str] = {}
        for part in (style_text or "").split(";"):
            if ":" not in part:
                continue
            k, v = part.split(":", 1)
            key = k.strip().lower()
            val = v.strip()
            if key:
                style_map[key] = val

        open_tags: list[str] = []
        close_tags: list[str] = []
        font_attrs: list[str] = []

        fam = style_map.get("font-family", "")
        if fam:
            font_attrs.append(f"name='{_inline_font_name_from_face(fam)}'")

        size_raw = style_map.get("font-size", "")
        if size_raw:
            m_size = re.search(r"(\d+(?:\.\d+)?)", size_raw)
            if m_size:
                font_attrs.append(f"size='{m_size.group(1)}'")

        color_raw = style_map.get("color", "")
        if color_raw:
            m_color = re.search(r"(#[0-9a-fA-F]{6})", color_raw)
            if m_color:
                font_attrs.append(f"color='{m_color.group(1)}'")

        if font_attrs:
            open_tags.append(f"<font {' '.join(font_attrs)}>")
            close_tags.insert(0, "</font>")

        if "bold" in style_map.get("font-weight", "").lower():
            open_tags.append("<b>")
            close_tags.insert(0, "</b>")

        if "italic" in style_map.get("font-style", "").lower():
            open_tags.append("<i>")
            close_tags.insert(0, "</i>")

        if "underline" in style_map.get("text-decoration", "").lower():
            open_tags.append("<u>")
            close_tags.insert(0, "</u>")

        return "".join(open_tags) + inner_html + "".join(close_tags)

    def _builder_table_rows(kind: str) -> tuple[list[str], list[float], list[list[str]]]:
        if kind == "labor":
            headers = ["Service Description", "Time", "Line Total"]
            rows = []
            rate = float(getattr(inv, "price_per_hour", 0.0) or 0.0)
            for li in getattr(inv, "labor_items", []) or []:
                t = float(getattr(li, "labor_time_hours", 0.0) or 0.0)
                line_total = t * rate
                rows.append([
                    (getattr(li, "labor_desc", "") or "").strip(),
                    f"{t:g} hrs" if t else "",
                    _money(line_total) if line_total else "",
                ])
            if not rows and float(getattr(inv, "hours", 0.0) or 0.0):
                t = float(getattr(inv, "hours", 0.0) or 0.0)
                rows.append(["Labor", f"{t:g} hrs", _money(t * rate)])
            col_fracs = [0.62, 0.18, 0.20]
        else:
            headers = ["Part / Material", "Price"]
            rows = []
            for p in getattr(inv, "parts", []) or []:
                name = (getattr(p, "part_name", "") or "").strip()
                price = float(getattr(p, "part_price", 0.0) or 0.0)
                rows.append([name, _money(inv.part_price_with_markup(price)) if price else ""])
            col_fracs = [0.78, 0.22]

        if not rows:
            rows = [["No items", ""]] if kind != "labor" else [["No labor items", "", ""]]
        return headers, col_fracs, rows

    def _draw_table_box(kind: str, box_x: float, box_y: float, box_w: float, box_h: float, draw_rows: list[list[str]], *, cont: bool) -> int:
        pad_x = 6 * scale_x
        row_h = max(16.0, 18.0 * min(scale_x, scale_y))
        body_font = max(8.0, 10.0 * min(scale_x, scale_y))
        header_font = max(8.5, 10.5 * min(scale_x, scale_y))
        headers, col_fracs, _rows = _builder_table_rows(kind)

        left = box_x + pad_x
        right = box_x + box_w - pad_x
        top = box_y + box_h - (8 * scale_y)
        col_widths = [box_w * f for f in col_fracs]
        col_x = [left]
        for cw in col_widths[:-1]:
            col_x.append(col_x[-1] + cw)

        header_label = f"{headers[0]} (cont.)" if cont else headers[0]
        header_cells = [header_label] + headers[1:]

        pdf.setFillColor(colors.HexColor("#f3f4f6"))
        pdf.rect(box_x + 1, top - row_h + 1, box_w - 2, row_h, stroke=0, fill=1)
        pdf.setFillColor(colors.black)
        pdf.setStrokeColor(colors.HexColor("#d1d5db"))
        pdf.setFont("Helvetica-Bold", header_font)
        for idx, head in enumerate(header_cells):
            if idx == 0:
                pdf.drawString(col_x[idx] + 4, top - (row_h * 0.72), head)
            elif idx == len(header_cells) - 1:
                tw = stringWidth(head, "Helvetica-Bold", header_font)
                pdf.drawString(right - tw - 4, top - (row_h * 0.72), head)
            else:
                mid = col_x[idx] + (col_widths[idx] / 2.0)
                tw = stringWidth(head, "Helvetica-Bold", header_font)
                pdf.drawString(mid - (tw / 2.0), top - (row_h * 0.72), head)
        pdf.line(box_x + 1, top - row_h, box_x + box_w - 1, top - row_h)

        available_h = max(0.0, (top - (box_y + 4.0)) - row_h)
        fit_rows = max(1, int((available_h / row_h) + 0.35))
        row_count = min(len(draw_rows), fit_rows)
        y_cursor = top - row_h
        pdf.setFont("Helvetica", body_font)
        for row in draw_rows[:row_count]:
            row_top = y_cursor
            row_bottom = y_cursor - row_h
            text_y = row_top - (row_h * 0.68)
            first = str(row[0] if len(row) > 0 else "")
            second = str(row[1] if len(row) > 1 else "")
            third = str(row[2] if len(row) > 2 else "")
            pdf.drawString(col_x[0] + 4, text_y, first[:120])
            if len(header_cells) == 3:
                mid = col_x[1] + (col_widths[1] / 2.0)
                tw2 = stringWidth(second, "Helvetica", body_font)
                pdf.drawString(mid - (tw2 / 2.0), text_y, second)
                tw3 = stringWidth(third, "Helvetica", body_font)
                pdf.drawString(right - tw3 - 4, text_y, third)
            else:
                tw2 = stringWidth(second, "Helvetica", body_font)
                pdf.drawString(right - tw2 - 4, text_y, second)
            # Draw separators below text baseline so they do not cross through text.
            pdf.line(box_x + 1, row_bottom + 1.5, box_x + box_w - 1, row_bottom + 1.5)
            y_cursor -= row_h
        return row_count

    def _draw_builder_table(kind: str, x0: float, y0: float, w0: float, h0: float, *, auto_grow: bool = True) -> list[list[str]]:
        row_h = max(16.0, 18.0 * min(scale_x, scale_y))
        bottom_margin = 36.0
        _headers, _col_fracs, rows = _builder_table_rows(kind)

        # Grow within page when possible.
        if auto_grow:
            needed_h = (len(rows) + 1) * row_h + (12 * scale_y)
            if needed_h > h0:
                growth = needed_h - h0
                y0 = max(bottom_margin, y0 - growth)
                h0 = needed_h

        consumed = _draw_table_box(kind, x0, y0, w0, h0, rows, cont=False)
        return rows[consumed:]

    def _table_row_count(kind: str) -> int:
        if kind == "labor":
            rows = len(getattr(inv, "labor_items", []) or [])
            if rows == 0 and float(getattr(inv, "hours", 0.0) or 0.0):
                rows = 1
            return max(1, rows)
        rows = len(getattr(inv, "parts", []) or [])
        return max(1, rows)

    def _table_needed_canvas_h(kind: str) -> float:
        row_h = max(16.0, 18.0 * min(scale_x, scale_y))
        rows = _table_row_count(kind)
        needed_h_page = (rows + 1) * row_h + (12 * scale_y)
        return max(10.0, needed_h_page / max(0.001, scale_y))

    def _text_needed_canvas_h(_el: dict) -> float:
        text_raw = str(_el.get("text") or "")
        rich_raw = str(_el.get("richText") or "")
        if "{{labor_table}}" in text_raw or "{{labor_table}}" in rich_raw or "{{parts_table}}" in text_raw or "{{parts_table}}" in rich_raw:
            return max(10.0, float(_el.get("h") or 10.0))

        for k, v in vars_map.items():
            text_raw = text_raw.replace(f"{{{{{k}}}}}", str(v))
        font_size = max(7.0, min(64.0, float(_el.get("fontSize") or 14.0))) * min(scale_x, scale_y)
        raw_font_weight = _el.get("fontWeight")
        try:
            font_weight = int(raw_font_weight or 500)
        except Exception:
            fw_txt = str(raw_font_weight or "").strip().lower()
            if "bold" in fw_txt:
                font_weight = 700
            elif "light" in fw_txt:
                font_weight = 300
            elif "regular" in fw_txt or "normal" in fw_txt:
                font_weight = 400
            else:
                font_weight = 500
        font_family = str(_el.get("fontFamily") or "Helvetica")
        font_style = str(_el.get("fontStyle") or "normal")
        font_name = _builder_font_name(font_family, font_weight, font_style)
        line_spacing = max(0.8, min(3.0, float(_el.get("lineSpacing") or 1.2)))
        line_h = max(8.0, font_size * line_spacing)

        w_canvas = max(1.0, float(_el.get("w") or 10.0))
        max_w = max(8.0, (w_canvas * scale_x) - (12 * scale_x))
        line_count = 0
        for ln in str(text_raw).splitlines() or [""]:
            wrapped = _wrap_text_preserve_spaces(str(ln), font_name, font_size, max_w)
            line_count += max(1, len(wrapped))
        needed_page_h = (line_count * line_h) + (6 * scale_y)
        return max(10.0, needed_page_h / max(0.001, scale_y))

    effective_h: dict[str, float] = {}
    effective_y: dict[str, float] = {}
    base_h: dict[str, float] = {}
    base_y: dict[str, float] = {}
    base_x: dict[str, float] = {}
    base_w: dict[str, float] = {}
    grow_parent_by_target: dict[str, str] = {}
    for _el in elements:
        if not isinstance(_el, dict) or _el.get("id") is None:
            continue
        el_id = str(_el.get("id"))
        if str(_el.get("type") or "").lower() == "box":
            target_id = str(_el.get("growWithId") or "").strip()
            if target_id:
                grow_parent_by_target[target_id] = el_id
        h0 = max(10.0, float(_el.get("h") or 10.0))
        y0 = float(_el.get("y") or 0.0)
        x0 = float(_el.get("x") or 0.0)
        w0 = max(1.0, float(_el.get("w") or 1.0))
        base_h[el_id] = h0
        base_y[el_id] = y0
        base_x[el_id] = x0
        base_w[el_id] = w0
        effective_y[el_id] = y0
        text_raw = str(_el.get("text") or "")
        rich_raw = str(_el.get("richText") or "")
        if "{{labor_table}}" in text_raw or "{{labor_table}}" in rich_raw:
            effective_h[el_id] = max(h0, _table_needed_canvas_h("labor"))
        elif "{{parts_table}}" in text_raw or "{{parts_table}}" in rich_raw:
            effective_h[el_id] = max(h0, _table_needed_canvas_h("parts"))
        elif bool(_el.get("autoGrow", False)):
            min_h = max(10.0, float(_el.get("minH") or h0))
            effective_h[el_id] = max(h0, min_h, _text_needed_canvas_h(_el))
        else:
            effective_h[el_id] = h0

    def _x_overlap(a_id: str, b_id: str) -> bool:
        a = elements_by_id.get(a_id) or {}
        b = elements_by_id.get(b_id) or {}
        ax1 = float(a.get("x") or 0.0)
        ax2 = ax1 + max(1.0, float(a.get("w") or 1.0))
        bx1 = float(b.get("x") or 0.0)
        bx2 = bx1 + max(1.0, float(b.get("w") or 1.0))
        return ax1 < bx2 and bx1 < ax2

    def _is_contained(inner_id: str, outer_id: str) -> bool:
        inner = elements_by_id.get(inner_id) or {}
        outer = elements_by_id.get(outer_id) or {}
        ix = float(inner.get("x") or 0.0)
        iy = float(effective_y.get(inner_id, base_y.get(inner_id, float(inner.get("y") or 0.0))))
        iw = max(1.0, float(inner.get("w") or 1.0))
        ih = max(1.0, float(effective_h.get(inner_id, base_h.get(inner_id, float(inner.get("h") or 1.0)))))
        ox = float(outer.get("x") or 0.0)
        oy = float(effective_y.get(outer_id, base_y.get(outer_id, float(outer.get("y") or 0.0))))
        ow = max(1.0, float(outer.get("w") or 1.0))
        oh = max(1.0, float(effective_h.get(outer_id, base_h.get(outer_id, float(outer.get("h") or 1.0)))))
        return ix >= ox and iy >= oy and (ix + iw) <= (ox + ow) and (iy + ih) <= (oy + oh)

    def _is_base_contained(inner_id: str, outer_id: str) -> bool:
        ix = float(base_x.get(inner_id, float((elements_by_id.get(inner_id) or {}).get("x") or 0.0)))
        iy = float(base_y.get(inner_id, float((elements_by_id.get(inner_id) or {}).get("y") or 0.0)))
        iw = max(1.0, float(base_w.get(inner_id, float((elements_by_id.get(inner_id) or {}).get("w") or 1.0))))
        ih = max(1.0, float(base_h.get(inner_id, float((elements_by_id.get(inner_id) or {}).get("h") or 1.0))))
        ox = float(base_x.get(outer_id, float((elements_by_id.get(outer_id) or {}).get("x") or 0.0)))
        oy = float(base_y.get(outer_id, float((elements_by_id.get(outer_id) or {}).get("y") or 0.0)))
        ow = max(1.0, float(base_w.get(outer_id, float((elements_by_id.get(outer_id) or {}).get("w") or 1.0))))
        oh = max(1.0, float(base_h.get(outer_id, float((elements_by_id.get(outer_id) or {}).get("h") or 1.0))))
        return ix >= ox and iy >= oy and (ix + iw) <= (ox + ow) and (iy + ih) <= (oy + oh)

    def _nearest_below_y(el_id: str, *, ignore_ids: set[str] | None = None) -> float | None:
        y = float(effective_y.get(el_id, base_y.get(el_id, 0.0)))
        best = None
        ignored = set(ignore_ids or set())
        for other_id in elements_by_id.keys():
            if other_id == el_id:
                continue
            if other_id in ignored:
                continue
            if _is_contained(other_id, el_id):
                continue
            if not _x_overlap(el_id, other_id):
                continue
            oy = float(effective_y.get(other_id, base_y.get(other_id, 0.0)))
            if oy <= y:
                continue
            if best is None or oy < best:
                best = oy
        return best

    anchored_targets = {
        target_id
        for target_id, parent_id in grow_parent_by_target.items()
        if target_id in elements_by_id and parent_id in elements_by_id and _is_base_contained(target_id, parent_id)
    }

    for _ in range(6):
        changed = False
        # Lock-follow Y position first so growth caps can account for moved elements.
        for _el in elements:
            if not isinstance(_el, dict) or _el.get("id") is None:
                continue
            el_id = str(_el.get("id"))
            lock_to = str(_el.get("lockToId") or "").strip()
            if not lock_to or lock_to not in elements_by_id or lock_to == el_id:
                continue
            mode = str(_el.get("lockMode") or "below").strip().lower()
            offset = float(_el.get("lockOffset") or 0.0)
            anchor_y = float(effective_y.get(lock_to, base_y.get(lock_to, 0.0)))
            anchor_h = float(effective_h.get(lock_to, base_h.get(lock_to, 10.0)))
            self_h = float(effective_h.get(el_id, base_h.get(el_id, 10.0)))
            if mode == "above":
                new_y = anchor_y - self_h + offset
            elif mode == "align_top":
                new_y = anchor_y + offset
            elif mode == "align_bottom":
                new_y = anchor_y + anchor_h - self_h + offset
            else:
                new_y = anchor_y + anchor_h + offset
            new_y = max(0.0, round(new_y))
            if abs(float(effective_y.get(el_id, base_y.get(el_id, 0.0))) - new_y) > 0.1:
                effective_y[el_id] = new_y
                changed = True

        # Cap table growth at collision to avoid creating large dead space.
        for _el in elements:
            if not isinstance(_el, dict) or _el.get("id") is None:
                continue
            el_id = str(_el.get("id"))
            text_raw = str(_el.get("text") or "")
            rich_raw = str(_el.get("richText") or "")
            wanted = None
            if "{{labor_table}}" in text_raw or "{{labor_table}}" in rich_raw:
                wanted = max(base_h.get(el_id, 10.0), _table_needed_canvas_h("labor"))
            elif "{{parts_table}}" in text_raw or "{{parts_table}}" in rich_raw:
                wanted = max(base_h.get(el_id, 10.0), _table_needed_canvas_h("parts"))
            elif bool(_el.get("autoGrow", False)):
                wanted = max(base_h.get(el_id, 10.0), _text_needed_canvas_h(_el))
            if wanted is None:
                continue
            min_h = max(10.0, float(_el.get("minH") or base_h.get(el_id, 10.0)))
            wanted = max(min_h, wanted)
            if abs(float(effective_h.get(el_id, base_h.get(el_id, 10.0))) - wanted) > 0.1:
                effective_h[el_id] = wanted
                changed = True

        # Box growth that follows a target element, collision-capped.
        for _el in elements:
            if not isinstance(_el, dict) or str(_el.get("type") or "").lower() != "box" or _el.get("id") is None:
                continue
            el_id = str(_el.get("id"))
            target_id = str(_el.get("growWithId") or "").strip()
            if not target_id or target_id not in elements_by_id:
                continue
            target_h = float(effective_h.get(target_id, max(10.0, float(elements_by_id[target_id].get("h") or 10.0))))
            delta = _el.get("growDelta")
            if delta is None:
                h_box = max(10.0, float(_el.get("h") or 10.0))
                h_target = max(10.0, float(elements_by_id[target_id].get("h") or 10.0))
                delta = h_box - h_target
            min_h = max(10.0, float(base_h.get(el_id, 10.0)))
            desired = max(min_h, float(target_h) + float(delta))
            if abs(effective_h.get(el_id, 0.0) - desired) > 0.1:
                effective_h[el_id] = desired
                changed = True

        # IMPORTANT: Keep custom-template element Y positions faithful to editor state.
        # The builder UI is drag-and-place; auto-flowing lower elements here causes
        # rendered output to drift from what the user positioned in the editor.
        # Dynamic growth/locks still apply, but no implicit cascading reflow.

        if not changed:
            break

    table_overflow_jobs: list[dict] = []
    for el in elements:
        if not isinstance(el, dict):
            continue
        x = float(el.get("x") or 0.0)
        y = float(effective_y.get(str(el.get("id") or ""), float(el.get("y") or 0.0)))
        w = max(10.0, float(el.get("w") or 10.0))
        el_id = str(el.get("id") or "")
        h = max(10.0, float(effective_h.get(el_id, float(el.get("h") or 10.0))))
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
        if etype == "image":
            has_fill = re.fullmatch(r"#[0-9a-fA-F]{6}", fill_color or "") is not None
            has_stroke = re.fullmatch(r"#[0-9a-fA-F]{6}", border_color or "") is not None
            if has_fill:
                pdf.setFillColor(colors.HexColor(fill_color))
                pdf.roundRect(rx, ry, rw, rh, max(0.0, radius), stroke=0, fill=1)
            if has_stroke:
                pdf.setStrokeColor(colors.HexColor(border_color))
                pdf.roundRect(rx, ry, rw, rh, max(0.0, radius), stroke=1, fill=0)

            src_raw = str(el.get("src") or el.get("text") or "").strip()
            is_business_logo = bool(re.search(r"\{\{\s*business_logo\s*\}\}", src_raw, flags=re.I))
            img_reader = logo_reader if is_business_logo else None
            if img_reader is None and src_raw:
                imported_asset_path = _resolve_imported_asset_token_path(src_raw)
                if imported_asset_path:
                    try:
                        img_reader = ImageReader(imported_asset_path)
                    except Exception:
                        img_reader = None
            if img_reader is None and src_raw:
                try:
                    if src_raw.lower().startswith(("http://", "https://")):
                        img_reader = ImageReader(src_raw)
                    elif os.path.exists(src_raw):
                        img_reader = ImageReader(src_raw)
                except Exception:
                    img_reader = None
            if img_reader is not None:
                pad = 3.0
                pdf.drawImage(
                    img_reader,
                    rx + pad,
                    ry + pad,
                    width=max(1.0, rw - (2 * pad)),
                    height=max(1.0, rh - (2 * pad)),
                    preserveAspectRatio=True,
                    anchor="c",
                    mask="auto",
                )
            continue

        text_raw = str(el.get("text") or "")
        rich_raw = str(el.get("richText") or "")
        if "{{labor_table}}" in text_raw or "{{labor_table}}" in rich_raw:
            parent_id = grow_parent_by_target.get(el_id)
            if parent_id and el_id in anchored_targets:
                rel_y = float(base_y.get(el_id, 0.0)) - float(base_y.get(parent_id, 0.0))
                y = float(effective_y.get(parent_id, base_y.get(parent_id, 0.0))) + rel_y
                ry = _map_y(y, h)
            remaining = _draw_builder_table("labor", rx, ry, rw, rh, auto_grow=True)
            if remaining:
                table_overflow_jobs.append({"kind": "labor", "rows": remaining, "x": rx, "w": rw})
            continue
        if "{{parts_table}}" in text_raw or "{{parts_table}}" in rich_raw:
            parent_id = grow_parent_by_target.get(el_id)
            if parent_id and el_id in anchored_targets:
                rel_y = float(base_y.get(el_id, 0.0)) - float(base_y.get(parent_id, 0.0))
                y = float(effective_y.get(parent_id, base_y.get(parent_id, 0.0))) + rel_y
                ry = _map_y(y, h)
            remaining = _draw_builder_table("parts", rx, ry, rw, rh, auto_grow=True)
            if remaining:
                table_overflow_jobs.append({"kind": "parts", "rows": remaining, "x": rx, "w": rw})
            continue
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
            if rich_raw:
                rich_raw = rich_raw.replace(f"{{{{{k}}}}}", str(v))
        font_size = max(7.0, min(64.0, float(el.get("fontSize") or 14.0))) * min(scale_x, scale_y)
        raw_font_weight = el.get("fontWeight")
        try:
            font_weight = int(raw_font_weight or 500)
        except Exception:
            fw_txt = str(raw_font_weight or "").strip().lower()
            if "bold" in fw_txt:
                font_weight = 700
            elif "light" in fw_txt:
                font_weight = 300
            elif "regular" in fw_txt or "normal" in fw_txt:
                font_weight = 400
            else:
                font_weight = 500
        font_family = str(el.get("fontFamily") or "Helvetica")
        font_style = str(el.get("fontStyle") or "normal")
        text_align = str(el.get("textAlign") or "left").strip().lower()
        underline = bool(el.get("underline", False))
        font_name = _builder_font_name(font_family, font_weight, font_style)
        if re.fullmatch(r"#[0-9a-fA-F]{6}", text_color or ""):
            pdf.setFillColor(colors.HexColor(text_color))
        else:
            pdf.setFillColor(colors.black)
        if re.fullmatch(r"#[0-9a-fA-F]{6}", text_color or ""):
            pdf.setStrokeColor(colors.HexColor(text_color))
        else:
            pdf.setStrokeColor(colors.black)
        pdf.setFont(font_name, font_size)
        line_spacing = float(el.get("lineSpacing") or 1.2)
        line_spacing = max(0.8, min(3.0, line_spacing))
        line_h = max(8.0, font_size * line_spacing)
        # Imported templates often have tight OCR/native boxes. Ensure at least one line can render.
        draw_h = max(float(rh), line_h + 4.0)
        left_x = rx + (6 * scale_x)
        right_x = rx + rw - (6 * scale_x)
        center_x = rx + (rw / 2.0)
        ty = ry + draw_h - line_h
        max_w = rw - (12 * scale_x)
        if rich_raw.strip():
            rich = rich_raw
            rich = re.sub(r"</?(div|p)[^>]*>", "<br/>", rich, flags=re.I)
            # Convert span inline styles into reportlab-friendly tags.
            for _ in range(8):
                new_rich = re.sub(
                    r"<span\b([^>]*)>(.*?)</span>",
                    lambda m: _span_style_to_tags(
                        re.search(r"style=['\"]([^'\"]*)['\"]", m.group(1) or "", flags=re.I).group(1)
                        if re.search(r"style=['\"]([^'\"]*)['\"]", m.group(1) or "", flags=re.I)
                        else "",
                        m.group(2) or "",
                    ),
                    rich,
                    flags=re.I | re.S,
                )
                if new_rich == rich:
                    break
                rich = new_rich
            rich = re.sub(r"<span[^>]*>", "", rich, flags=re.I)
            rich = re.sub(r"</span>", "", rich, flags=re.I)
            # Preserve user-entered spacing in rich text.
            rich = rich.replace("&nbsp;", "&#160;")
            rich = re.sub(r" {2,}", lambda m: (" " + ("&#160;" * (len(m.group(0)) - 1))), rich)
            rich = re.sub(r"<font\s+face=['\"]?([^'\">]+)['\"]?>", lambda m: f"<font name='{_inline_font_name_from_face(m.group(1))}'>", rich, flags=re.I)
            rich = re.sub(r"<br\s*>", "<br/>", rich, flags=re.I)
            if underline and "<u>" not in rich.lower():
                rich = f"<u>{rich}</u>"
            align_code = 0 if text_align == "left" else (1 if text_align == "center" else 2)
            pstyle = ParagraphStyle(
                "builder_rich",
                fontName=font_name,
                fontSize=font_size,
                leading=line_h,
                textColor=colors.HexColor(text_color) if re.fullmatch(r"#[0-9a-fA-F]{6}", text_color or "") else colors.black,
                alignment=align_code,
            )
            try:
                para = Paragraph(rich, pstyle)
                _w, h_used = para.wrap(max_w, max(10.0, draw_h - 4))
                draw_y = ry + draw_h - h_used - 2
                para.drawOn(pdf, left_x, max(ry + 2, draw_y))
                continue
            except Exception:
                pass
        for ln in str(text_raw).splitlines():
            for wrapped in _wrap_text_preserve_spaces(ln, font_name, font_size, max_w):
                if ty < ry + 2:
                    break
                if text_align == "right":
                    text_w = stringWidth(wrapped, font_name, font_size)
                    draw_x = max(left_x, right_x - text_w)
                elif text_align == "center":
                    text_w = stringWidth(wrapped, font_name, font_size)
                    draw_x = max(left_x, center_x - (text_w / 2.0))
                else:
                    draw_x = left_x
                    text_w = stringWidth(wrapped, font_name, font_size)
                pdf.drawString(draw_x, ty, wrapped)
                if underline and wrapped:
                    underline_y = ty - max(1.0, font_size * 0.08)
                    pdf.line(draw_x, underline_y, draw_x + text_w, underline_y)
                ty -= line_h

    # Keep first-page layout stable; render table overflow on continuation pages after all elements.
    for job in table_overflow_jobs:
        kind = str(job.get("kind") or "parts")
        remaining = list(job.get("rows") or [])
        box_x = float(job.get("x") or 24.0)
        box_w = float(job.get("w") or (PAGE_W - 48.0))
        while remaining:
            pdf.showPage()
            pdf.setFillColor(colors.HexColor(canvas_bg))
            pdf.rect(0, 0, PAGE_W, PAGE_H, stroke=0, fill=1)
            cont_x = max(24.0, min(box_x, PAGE_W - box_w - 24.0))
            cont_w = min(box_w, PAGE_W - 48.0)
            cont_y = 48.0
            cont_h = PAGE_H - (cont_y + 42.0)
            used = _draw_table_box(kind, cont_x, cont_y, cont_w, cont_h, remaining, cont=True)
            remaining = remaining[used:]

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

    header_name = _business_header_name(owner)
    header_info_lines = _business_header_info_lines(owner)

    left_x = M + (logo_w + 10 if logo_w else 0)
    pdf.setFillColor(colors.white)
    pdf.setFont("Helvetica-Bold", 14)
    header_display_name = _business_header_display_name(owner, "InvoiceRunner")
    if header_display_name:
        pdf.drawString(left_x, PAGE_H - 0.55 * inch, header_display_name)

    pdf.setFont("Helvetica", 9)
    info_lines = []
    for ln in header_info_lines:
        info_lines.extend(_wrap_text(ln, "Helvetica", 9, 3.6 * inch))
    info_y = PAGE_H - 0.82 * inch
    for ln in info_lines[:4]:
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
    def _start_cont_page():
        pdf.showPage()
        pdf.setFont("Helvetica-Bold", 12)
        pdf.setFillColor(colors.black)
        pdf.drawString(M, PAGE_H - M, f"{doc_label} (cont.)")
        pdf.setFont("Helvetica", 9)
        pdf.setFillColor(colors.HexColor("#64748b"))
        pdf.drawString(M, PAGE_H - M - 14, f"{display_no}  •  Generated: {generated_str}")
        pdf.setFillColor(colors.black)
        return PAGE_H - M - 34

    def draw_table(title, x, y_top, col_titles, rows, col_widths, money_cols=None):
        money_cols = set(money_cols or [])
        base_row_h = 14 if builder_compact_mode else 16
        title_gap = 18
        min_content_y = 1.0 * inch

        def draw_table_header(header_title: str, y_top_local: float):
            pdf.setFont("Helvetica-Bold", 12)
            pdf.setFillColor(colors.black)
            pdf.drawString(x, y_top_local, header_title)
            y_local = y_top_local - title_gap

            table_w_local = sum(col_widths)
            pdf.setFillColor(brand_dark)
            pdf.roundRect(x, y_local - base_row_h + 9, table_w_local, base_row_h + 2, 6, stroke=0, fill=1)
            pdf.setFont("Helvetica-Bold", 9)
            pdf.setFillColor(colors.white)

            cx_local = x
            for i, h in enumerate(col_titles):
                if i in money_cols:
                    right_text(cx_local + col_widths[i] - 8, y_local + 1, h, "Helvetica-Bold", 9, colors.white)
                else:
                    pdf.drawString(cx_local + 8, y_local + 1, h)
                cx_local += col_widths[i]
            return y_local - base_row_h

        table_w = sum(col_widths)
        y_cursor = draw_table_header(title, y_top)
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

            if (y_cursor - row_height) < min_content_y:
                next_top = _start_cont_page()
                y_cursor = draw_table_header(f"{title} (cont.)", next_top)
                pdf.setFont("Helvetica", 10)
                pdf.setFillColor(colors.black)

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

    has_labor_rows = any((row[0] or row[1] or row[2]) for row in labor_rows)
    if show_labor and has_labor_rows:
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
    notes_floor = 2.2 * inch + M
    if (body_y - 10) < notes_floor:
        body_y = _start_cont_page()
    notes_y_top = max(body_y - 10, notes_floor)

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
    total_price, price_owed, late_fee_amount = _invoice_pdf_amounts(inv, owner, is_estimate=is_estimate)
    tax_amount = inv.tax_amount()

    right_edge = sum_x + sum_w - 12
    y = notes_y_top - 44

    if show_parts and has_parts_rows and total_parts:
        label_right_value(sum_x + 12, right_edge, y, f"{cfg['parts_title']}:", _money(total_parts)); y -= 16
    if show_labor and has_labor_rows and total_labor:
        label_right_value(sum_x + 12, right_edge, y, f"{cfg['labor_title']}:", _money(total_labor)); y -= 16
    if show_shop_supplies and inv.shop_supplies:
        label_right_value(sum_x + 12, right_edge, y, f"{cfg['shop_supplies_label']}:", _money(inv.shop_supplies)); y -= 16
    if tax_amount:
        label_right_value(sum_x + 12, right_edge, y, f"{_tax_label(inv)}:", _money(tax_amount)); y -= 16
    if late_fee_amount > 0 and not is_estimate:
        label_right_value(sum_x + 12, right_edge, y, "Late Fee:", _money(late_fee_amount)); y -= 16

    pdf.setStrokeColor(line_color)
    pdf.line(sum_x + 12, y + 4, sum_x + sum_w - 12, y + 4)
    pdf.setStrokeColor(colors.black)
    y -= 10

    label = "Estimated Total:" if is_estimate else "Total:"
    label_right_value(sum_x + 12, right_edge, y, label, _money(total_price)); y -= 18

    paid_amount = float(inv.paid or 0.0)
    if not is_estimate and paid_amount:
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

    header_name = _business_header_name(owner) or "InvoiceRunner"
    header_info_lines = _business_header_info_lines(owner)

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
    for ln in header_info_lines:
        rail_info.extend(_wrap_text(ln, "Helvetica", 8, rail_w - 20))
    info_y = name_y - 6
    for ln in rail_info[:4]:
        pdf.drawString(rail_x + 12, info_y, ln)
        info_y -= 11

    # Summary block on rail
    pdf.setFont("Helvetica-Bold", 10)
    pdf.setFillColor(rail_text)
    pdf.drawString(rail_x + 12, rail_y + 145, "SUMMARY")

    total_parts = inv.parts_total() if show_parts else 0.0
    total_labor = inv.labor_total() if show_labor else 0.0
    total_price, price_owed, late_fee_amount = _invoice_pdf_amounts(inv, owner, is_estimate=is_estimate)
    tax_amount = inv.tax_amount()

    pdf.setFont("Helvetica", 9)
    y = rail_y + 125
    if show_labor and total_labor:
        pdf.drawString(rail_x + 12, y, f"{cfg['labor_title']}: {_money(total_labor)}"); y -= 14
    if show_parts and total_parts:
        pdf.drawString(rail_x + 12, y, f"{cfg['parts_title']}: {_money(total_parts)}"); y -= 14
    if show_shop_supplies and inv.shop_supplies:
        pdf.drawString(rail_x + 12, y, f"{cfg['shop_supplies_label']}: {_money(inv.shop_supplies)}"); y -= 14
    if tax_amount:
        pdf.drawString(rail_x + 12, y, f"{_tax_label(inv)}: {_money(tax_amount)}"); y -= 14
    if late_fee_amount > 0 and not is_estimate:
        pdf.drawString(rail_x + 12, y, f"Late Fee: {_money(late_fee_amount)}"); y -= 14

    y -= 4
    pdf.setFont("Helvetica-Bold", 10)
    label = "Est. Total" if is_estimate else "Total"
    pdf.drawString(rail_x + 12, y, f"{label}: {_money(total_price)}"); y -= 16
    paid_amount = float(inv.paid or 0.0)
    if not is_estimate:
        pdf.setFont("Helvetica", 9)
        if paid_amount:
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
    def _start_cont_page():
        pdf.showPage()
        pdf.setFont("Helvetica-Bold", 12)
        pdf.setFillColor(colors.black)
        pdf.drawString(M, PAGE_H - M, f"{doc_label} (cont.)")
        pdf.setFont("Helvetica", 9)
        pdf.setFillColor(colors.HexColor("#64748b"))
        pdf.drawString(M, PAGE_H - M - 14, f"{display_no}  •  Generated: {generated_str}")
        pdf.setFillColor(colors.black)
        return PAGE_H - M - 34

    def draw_table(title, x, y_top, col_titles, rows, col_widths, money_cols=None):
        money_cols = set(money_cols or [])
        base_row_h = 14 if builder_compact_mode else 16
        title_gap = 18
        min_content_y = 1.0 * inch

        def draw_table_header(header_title: str, y_top_local: float):
            pdf.setFont("Helvetica-Bold", 12)
            pdf.setFillColor(colors.black)
            pdf.drawString(x, y_top_local, header_title)
            y_local = y_top_local - title_gap

            table_w_local = sum(col_widths)
            pdf.setFillColor(accent)
            pdf.roundRect(x, y_local - base_row_h + 9, table_w_local, base_row_h + 2, 6, stroke=0, fill=1)
            pdf.setFont("Helvetica-Bold", 9)
            pdf.setFillColor(colors.white)

            cx_local = x
            for i, h in enumerate(col_titles):
                if i in money_cols:
                    right_text(cx_local + col_widths[i] - 8, y_local + 1, h, "Helvetica-Bold", 9, colors.white)
                else:
                    pdf.drawString(cx_local + 8, y_local + 1, h)
                cx_local += col_widths[i]
            return y_local - base_row_h

        table_w = sum(col_widths)
        y_cursor = draw_table_header(title, y_top)
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

            if (y_cursor - row_height) < min_content_y:
                next_top = _start_cont_page()
                y_cursor = draw_table_header(f"{title} (cont.)", next_top)
                pdf.setFont("Helvetica", 10)
                pdf.setFillColor(colors.black)

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

    has_labor_rows = any((row[0] or row[1] or row[2]) for row in labor_rows)
    if show_labor and has_labor_rows:
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
    notes_floor = 2.2 * inch + M
    if (body_y - 8) < notes_floor:
        body_y = _start_cont_page()
    notes_y_top = max(body_y - 8, notes_floor)

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

    header_name = _business_header_name(owner) or "InvoiceRunner"
    pdf.setFont("Helvetica-Bold", 12)
    pdf.setFillColor(header_text)
    pdf.drawString(M + logo_w, PAGE_H - 0.38 * inch, header_name)

    pdf.setFont("Helvetica", 9)
    pdf.setFillColor(header_muted)
    header_info_lines = _business_header_info_lines(owner)
    info_lines = []
    for ln in header_info_lines:
        info_lines.extend(_wrap_text(ln, "Helvetica", 9, 3.6 * inch))
    info_y = PAGE_H - 0.60 * inch
    for ln in info_lines[:4]:
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

    total_price, price_owed, late_fee_amount = _invoice_pdf_amounts(inv, owner, is_estimate=is_estimate)
    pdf.setFillColor(accent_dark)
    pdf.rect(strip_x + (2 * box_w), strip_y - strip_h, box_w * 2, strip_h, stroke=0, fill=1)
    pdf.setFillColor(colors.white)
    pdf.setFont("Helvetica-Bold", 9)
    pdf.drawString(strip_x + (2 * box_w) + 10, strip_y - 16, "Total due")
    pdf.setFont("Helvetica-Bold", 14)
    pdf.drawString(strip_x + (2 * box_w) + 10, strip_y - 36, _money(total_price))

    # Table
    def _start_cont_page():
        pdf.showPage()
        pdf.setFont("Helvetica-Bold", 12)
        pdf.setFillColor(colors.black)
        pdf.drawString(M, PAGE_H - M, f"{doc_label} (cont.)")
        pdf.setFont("Helvetica", 9)
        pdf.setFillColor(muted)
        pdf.drawString(M, PAGE_H - M - 14, f"{display_no}  •  Generated: {generated_str}")
        pdf.setFillColor(colors.black)
        return PAGE_H - M - 34

    def draw_table(title, x, y_top, col_titles, rows, col_widths, money_cols=None):
        money_cols = set(money_cols or [])
        base_row_h = 14 if builder_compact_mode else 16
        title_gap = 18
        min_content_y = 1.0 * inch

        def draw_table_header(header_title: str, y_top_local: float):
            pdf.setFont("Helvetica-Bold", 11)
            pdf.setFillColor(colors.black)
            pdf.drawString(x, y_top_local, header_title)
            y_local = y_top_local - title_gap

            table_w_local = sum(col_widths)
            pdf.setStrokeColor(line_color)
            pdf.setLineWidth(1)
            pdf.line(x, y_local - 6, x + table_w_local, y_local - 6)

            pdf.setFont("Helvetica-Bold", 9)
            pdf.setFillColor(colors.black)
            cx_local = x
            for i, h in enumerate(col_titles):
                if i in money_cols:
                    right_text(cx_local + col_widths[i] - 6, y_local, h, "Helvetica-Bold", 9)
                else:
                    pdf.drawString(cx_local + 6, y_local, h)
                cx_local += col_widths[i]
            return y_local - base_row_h

        table_w = sum(col_widths)
        y_cursor = draw_table_header(title, y_top)
        pdf.setFont("Helvetica", 10)

        for row in rows:
            wrapped_cells = []
            row_height = base_row_h
            for i, cell in enumerate(row):
                max_w = col_widths[i] - 12
                lines = _wrap_text(cell, "Helvetica", 10, max_w)
                wrapped_cells.append(lines)
                row_height = max(row_height, len(lines) * base_row_h)

            if (y_cursor - row_height) < min_content_y:
                next_top = _start_cont_page()
                y_cursor = draw_table_header(f"{title} (cont.)", next_top)
                pdf.setFont("Helvetica", 10)
                pdf.setFillColor(colors.black)

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

    has_labor_rows = any((row[0] or row[1] or row[2]) for row in labor_rows)
    if show_labor and has_labor_rows:
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
    notes_floor = 2.0 * inch + M
    if (body_y - 8) < notes_floor:
        body_y = _start_cont_page()
    if show_notes and (inv.notes or "").strip():
        notes_x = M
        notes_w = max(2.2 * inch, PAGE_W - (3 * M) - 2.5 * inch)
        notes_y_top = max(body_y - 8, notes_floor)
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

    sum_w = 2.5 * inch
    sum_x = PAGE_W - M - sum_w
    row_count = 1  # total
    if show_labor and has_labor_rows and total_labor:
        row_count += 1
    if show_parts and has_parts_rows and total_parts:
        row_count += 1
    if show_shop_supplies and inv.shop_supplies:
        row_count += 1
    if tax_amount:
        row_count += 1
    paid_amount = float(inv.paid or 0.0)
    if not is_estimate and paid_amount:
        row_count += 1  # paid
    if not is_estimate:
        row_count += 1  # amount due
    sum_h = max(1.2 * inch, (0.48 * inch + (row_count * 0.22 * inch)))
    sum_y = max(body_y - 12, (sum_h + 0.4 * inch))
    pdf.setStrokeColor(line_color)
    pdf.roundRect(sum_x, sum_y - sum_h, sum_w, sum_h, 8, stroke=1, fill=0)
    pdf.setFont("Helvetica-Bold", 10)
    pdf.drawString(sum_x + 10, sum_y - 16, "Summary")

    y = sum_y - 36
    right_edge = sum_x + sum_w - 10
    if show_labor and has_labor_rows and total_labor:
        label_right_value(sum_x + 10, right_edge, y, f"{cfg['labor_title']}:", _money(total_labor)); y -= 14
    if show_parts and has_parts_rows and total_parts:
        label_right_value(sum_x + 10, right_edge, y, f"{cfg['parts_title']}:", _money(total_parts)); y -= 14
    if show_shop_supplies and inv.shop_supplies:
        label_right_value(sum_x + 10, right_edge, y, f"{cfg['shop_supplies_label']}:", _money(inv.shop_supplies)); y -= 14
    if tax_amount:
        label_right_value(sum_x + 10, right_edge, y, f"{_tax_label(inv)}:", _money(tax_amount)); y -= 14
    if late_fee_amount > 0 and not is_estimate:
        label_right_value(sum_x + 10, right_edge, y, "Late Fee:", _money(late_fee_amount)); y -= 14

    pdf.setStrokeColor(line_color)
    pdf.line(sum_x + 10, y + 4, sum_x + sum_w - 10, y + 4)
    pdf.setStrokeColor(colors.black)
    y -= 8
    label = "Estimated Total:" if is_estimate else "Total:"
    label_right_value(sum_x + 10, right_edge, y, label, _money(total_price)); y -= 16
    if not is_estimate and paid_amount:
        label_right_value(sum_x + 10, right_edge, y, "Paid:", _money(inv.paid)); y -= 16
    if not is_estimate:
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


def _render_basic_pdf(
    *,
    session,
    inv: Invoice,
    owner: User | None,
    customer: Customer | None,
    customer_email: str,
    customer_phone: str,
    customer_address: str,
    cfg: dict,
    pdf_path: str,
    display_no: str,
    doc_label: str,
    generated_dt: datetime,
    generated_str: str,
    is_estimate: bool,
):
    PAGE_W, PAGE_H = LETTER
    M = 0.5 * inch
    pdf = canvas.Canvas(pdf_path, pagesize=LETTER)
    pdf.setTitle(f"{doc_label.title()} - {display_no}")
    total_with_fees, due_with_fees, late_fee_amount = _invoice_pdf_amounts(inv, owner, is_estimate=is_estimate)

    def right_text(x, y, text, font="Helvetica", size=10):
        pdf.setFont(font, size)
        pdf.drawRightString(x, y, str(text or ""))

    header_name = ((_business_header_name(owner) or "").strip()) if owner else ""
    header_lines = [ln for ln in _business_header_info_lines(owner) if ln]
    header_phone = _format_phone((getattr(owner, "phone", None) or "").strip()) if owner else ""

    left_x = M
    top_y = PAGE_H - M

    pdf.setFont("Helvetica-Bold", 14)
    header_display_name = _business_header_display_name(owner, "[Company Name]")
    if header_display_name:
        pdf.drawString(left_x, top_y - 4, header_display_name)
    pdf.setFont("Helvetica", 9)
    line_y = top_y - 22
    for ln in (header_lines[:4] or ["[Street Address]", "[City, ST ZIP]"]):
        pdf.drawString(left_x, line_y, ln)
        line_y -= 12
    if not header_lines and _show_business_phone(owner):
        pdf.drawString(left_x, line_y, f"Phone: {header_phone or '(000) 000-0000'}")

    pdf.setFont("Helvetica-Bold", 30)
    right_text(PAGE_W - M, top_y - 8, doc_label)

    info_w = 2.7 * inch
    info_h = 0.44 * inch
    info_x = PAGE_W - M - info_w
    info_y = top_y - 74
    pdf.setLineWidth(1)
    pdf.rect(info_x, info_y - info_h, info_w, info_h, stroke=1, fill=0)
    mid_x = info_x + (info_w / 2.0)
    mid_y = info_y - (info_h / 2.0)
    pdf.line(mid_x, info_y, mid_x, info_y - info_h)
    pdf.line(info_x, mid_y, info_x + info_w, mid_y)
    pdf.setFont("Helvetica-Bold", 8)
    right_text(mid_x - 6, info_y - 10, "INVOICE #")
    right_text(info_x + info_w - 6, info_y - 10, "DATE")
    pdf.setFont("Helvetica", 8)
    right_text(mid_x - 6, info_y - 24, display_no)
    right_text(info_x + info_w - 6, info_y - 24, generated_str)

    if not is_estimate and due_with_fees > 0.0:
        due_line = _invoice_due_date_line(inv, owner, is_estimate=is_estimate)
        if due_line:
            pdf.setFont("Helvetica", 9)
            pdf.drawString(info_x, info_y - 50, due_line)

    bill_y = info_y - info_h - 0.42 * inch
    bill_w = PAGE_W - 2 * M - 0.2 * inch
    label_h = 0.2 * inch
    pdf.rect(left_x, bill_y - label_h, 1.7 * inch, label_h, stroke=1, fill=0)
    pdf.setFont("Helvetica-Bold", 10)
    pdf.drawString(left_x + 6, bill_y - 11, "BILL TO")

    bill_text_y = bill_y - label_h - 12
    pdf.setFont("Helvetica", 9)
    pdf.drawString(left_x + 2, bill_text_y, (inv.name or "").strip() or "[Name]")
    bill_text_y -= 12
    for line in (_wrap_text(customer_address or "", "Helvetica", 9, 2.6 * inch)[:2] or ["[Street Address]", "[City, ST ZIP]"]):
        pdf.drawString(left_x + 2, bill_text_y, line)
        bill_text_y -= 11
    pdf.drawString(left_x + 2, bill_text_y, f"Phone: {customer_phone or '[Phone]'}")
    bill_text_y -= 11
    pdf.drawString(left_x + 2, bill_text_y, f"Email: {customer_email or '[Email Address]'}")

    table_top = bill_y - 1.7 * inch
    table_x = left_x
    table_w = PAGE_W - 2 * M
    table_h = 4.3 * inch
    header_h = 0.24 * inch
    amount_w = 1.7 * inch
    desc_w = table_w - amount_w
    pdf.rect(table_x, table_top - table_h, table_w, table_h, stroke=1, fill=0)
    pdf.line(table_x + desc_w, table_top, table_x + desc_w, table_top - table_h)
    pdf.line(table_x, table_top - header_h, table_x + table_w, table_top - header_h)
    desc_header = cfg.get("labor_desc_label", "Description")
    amount_header = cfg.get("labor_total_label", "Amount")
    pdf.setFont("Helvetica-Bold", 9)
    pdf.drawString(table_x + 6, table_top - 14, desc_header)
    right_text(table_x + table_w - 6, table_top - 14, amount_header)

    items: list[tuple[str, float]] = []
    rate = float(inv.price_per_hour or 0.0)
    for li in inv.labor_items:
        try:
            t = float(li.labor_time_hours or 0.0)
        except Exception:
            t = 0.0
        if t <= 0:
            continue
        items.append((f"{cfg.get('labor_title', 'Labor')}: {li.labor_desc or 'Service'} ({t:g} hr @ {_money(rate)}/hr)", t * rate))
    for p in inv.parts:
        price = float(inv.part_price_with_markup(p.part_price or 0.0) or 0.0)
        if price > 0:
            items.append((p.part_name or "Part", price))
    if show_shop := bool(cfg.get("show_shop_supplies", True)):
        if float(inv.shop_supplies or 0.0):
            items.append((cfg.get("shop_supplies_label", "Additional Fees"), float(inv.shop_supplies or 0.0)))
    tax_amt = float(inv.tax_amount() or 0.0)
    if late_fee_amount > 0 and not is_estimate:
        items.append(("Late Fee", late_fee_amount))
    if tax_amt:
        items.append((_tax_label(inv), tax_amt))

    row_y = table_top - header_h - 14
    line_h = 12
    max_desc_w = desc_w - 10
    for desc, amount in items:
        wrapped = _wrap_text(desc, "Helvetica", 9, max_desc_w)[:3] or [desc]
        needed = line_h * len(wrapped)
        if row_y - needed < (table_top - table_h + 26):
            break
        pdf.setFont("Helvetica", 9)
        for ln in wrapped:
            pdf.drawString(table_x + 6, row_y, ln)
            row_y -= line_h
        right_text(table_x + table_w - 8, row_y + line_h, _money(amount), "Helvetica", 9)
        row_y -= 2

    total_row_y = table_top - table_h + 20
    pdf.line(table_x, total_row_y + 10, table_x + table_w, total_row_y + 10)
    pdf.setFont("Helvetica-BoldOblique", 10)
    pdf.drawCentredString(table_x + (desc_w / 2.0), total_row_y - 4, "Thank you for your business!")
    pdf.setFont("Helvetica-Bold", 12)
    pdf.drawString(table_x + desc_w + 10, total_row_y - 4, "TOTAL")
    right_text(table_x + table_w - 8, total_row_y - 4, _money(total_with_fees), "Helvetica-Bold", 12)

    foot_y = M + 18
    pdf.setFont("Helvetica", 8)
    contact = (header_name or "[Name]")
    email = ((getattr(owner, "email", None) or "").strip()) if owner else ""
    phone = header_phone or "[Phone]"
    pdf.drawCentredString(PAGE_W / 2.0, foot_y + 14, f"If you have any questions about this {doc_label.lower()}, please contact")
    pdf.drawCentredString(PAGE_W / 2.0, foot_y + 2, f"{contact}, {phone}{(', ' + email) if email else ''}")

    pdf.save()
    inv.pdf_path = pdf_path
    inv.pdf_generated_at = generated_dt
    session.add(inv)
    session.commit()
    return pdf_path


def _render_simple_pdf(
    *,
    session,
    inv: Invoice,
    owner: User | None,
    customer: Customer | None,
    customer_email: str,
    customer_phone: str,
    customer_address: str,
    cfg: dict,
    pdf_path: str,
    display_no: str,
    doc_label: str,
    generated_dt: datetime,
    generated_str: str,
    owner_logo_abs: str,
    owner_logo_blob: bytes | None,
    is_estimate: bool,
):
    PAGE_W, PAGE_H = LETTER
    M = 0.65 * inch
    accent = colors.HexColor("#1d4ed8")
    text_dark = colors.HexColor("#1f2937")
    muted = colors.HexColor("#6b7280")

    pdf = canvas.Canvas(pdf_path, pagesize=LETTER)
    pdf.setTitle(f"{doc_label.title()} - {display_no}")
    total_with_fees, due_with_fees, late_fee_amount = _invoice_pdf_amounts(inv, owner, is_estimate=is_estimate)

    def right_text(x, y, text, font="Helvetica", size=10, color=text_dark):
        pdf.setFont(font, size)
        pdf.setFillColor(color)
        pdf.drawRightString(x, y, str(text or ""))

    header_name = ((_business_header_name(owner) or "").strip()) if owner else ""
    addr_lines = [ln for ln in _business_header_info_lines(owner) if ln]
    phone_txt = _format_phone((getattr(owner, "phone", None) or "").strip()) if owner else ""

    top_y = PAGE_H - M
    icon_w = 1.24 * inch
    icon_h = 0.95 * inch
    icon_x = M
    icon_y = top_y - 0.80 * inch
    icon_cx = icon_x + (icon_w / 2.0)
    icon_cy = icon_y + (icon_h / 2.0)
    logo_drawn = False
    logo_x = icon_cx - 0.34 * inch
    logo_y = icon_cy - 0.34 * inch
    logo_w = 0.68 * inch
    logo_h = 0.68 * inch
    if owner_logo_blob:
        try:
            img = ImageReader(io.BytesIO(owner_logo_blob))
            pdf.drawImage(img, logo_x, logo_y, width=logo_w, height=logo_h, mask="auto")
            logo_drawn = True
        except Exception:
            logo_drawn = False
    elif owner_logo_abs and os.path.exists(owner_logo_abs):
        try:
            img = ImageReader(owner_logo_abs)
            pdf.drawImage(img, logo_x, logo_y, width=logo_w, height=logo_h, mask="auto")
            logo_drawn = True
        except Exception:
            logo_drawn = False

    pdf.setFillColor(text_dark)
    pdf.setFont("Helvetica-Bold", 14)
    pdf.drawCentredString(icon_cx, icon_y - 12, "Estimate" if is_estimate else "Invoice")

    comp_x = PAGE_W - M
    comp_y = top_y - 10
    header_display_name = _business_header_display_name(owner, "YOUR COMPANY")
    if header_display_name:
        right_text(comp_x, comp_y, header_display_name.upper(), "Helvetica-Bold", 12)
    pdf.setFont("Helvetica", 11)
    y = comp_y - 20
    for ln in (addr_lines[:4] or ["1234 Your Street", "City, ST 90210", "United States"]):
        right_text(comp_x, y, ln, "Helvetica", 11, text_dark)
        y -= 16
    if not addr_lines and _show_business_phone(owner):
        right_text(comp_x, y, phone_txt or "1-888-123-4567", "Helvetica", 11, text_dark)

    bill_x = M
    bill_y = top_y - 2.15 * inch
    pdf.setFillColor(accent)
    pdf.setFont("Helvetica", 12)
    pdf.drawString(bill_x, bill_y, "Billed To")
    pdf.setFillColor(text_dark)
    pdf.setFont("Helvetica-Bold", 11)
    pdf.drawString(bill_x, bill_y - 18, (inv.name or getattr(customer, "name", None) or "Your Client"))
    pdf.setFont("Helvetica", 11)
    y = bill_y - 36
    for ln in _wrap_text(customer_address or "1234 Clients Street\nCity, ST 90210", "Helvetica", 11, 2.8 * inch)[:3]:
        pdf.drawString(bill_x, y, ln)
        y -= 15
    pdf.drawString(bill_x, y, customer_phone or "1-888-123-8910")
    y -= 15
    if customer_email:
        pdf.drawString(bill_x, y, customer_email)

    meta_x = PAGE_W - M - 3.25 * inch
    meta_y = bill_y
    due_val = max(0.0, due_with_fees)
    left_pairs = [
        ("Date Issued", generated_str),
    ]
    if not is_estimate:
        due_line = _invoice_due_date_line(inv, owner, is_estimate=is_estimate)
        if due_line:
            left_pairs.append(("Due Date", due_line.replace("Payment due date:", "").strip()))
    right_pairs = [
        ("Invoice Number" if not is_estimate else "Estimate Number", display_no),
        ("Amount Due" if not is_estimate else "Estimate Total", _money(due_val if not is_estimate else total_with_fees)),
    ]
    pdf.setFont("Helvetica", 11)
    ly = meta_y
    for label, value in left_pairs:
        pdf.setFillColor(accent)
        pdf.drawString(meta_x, ly, label)
        pdf.setFillColor(text_dark)
        pdf.drawString(meta_x, ly - 16, value)
        ly -= 44
    ry = meta_y
    for idx, (label, value) in enumerate(right_pairs):
        col_x = meta_x + 1.72 * inch
        pdf.setFillColor(accent)
        pdf.drawString(col_x, ry, label)
        pdf.setFillColor(text_dark)
        pdf.setFont("Helvetica-Bold" if idx == 1 else "Helvetica", 12 if idx == 1 else 11)
        pdf.drawString(col_x, ry - 16, value)
        pdf.setFont("Helvetica", 11)
        ry -= 44

    table_top = top_y - 4.05 * inch
    pdf.setStrokeColor(accent)
    pdf.setLineWidth(2)
    pdf.line(M, table_top, PAGE_W - M, table_top)
    pdf.setLineWidth(1)

    table_w = PAGE_W - 2 * M
    col_desc = table_w * 0.60
    col_rate = table_w * 0.13
    col_qty = table_w * 0.10
    col_amt = table_w - col_desc - col_rate - col_qty
    x_desc = M
    x_rate = x_desc + col_desc
    x_qty = x_rate + col_rate
    x_amt = x_qty + col_qty

    def draw_table_header(y_top: float):
        desc_header = cfg.get("labor_desc_label", "Description")
        rate_header = cfg.get("job_rate_label", "Rate")
        qty_header = cfg.get("labor_time_label", "Qty")
        amount_header = cfg.get("labor_total_label", "Amount")
        pdf.setFillColor(accent)
        pdf.setFont("Helvetica", 11)
        pdf.drawString(x_desc, y_top - 20, str(desc_header).upper())
        right_text(x_rate + col_rate - 4, y_top - 20, str(rate_header).upper(), "Helvetica", 11, accent)
        right_text(x_qty + col_qty - 4, y_top - 20, str(qty_header).upper(), "Helvetica", 11, accent)
        right_text(x_amt + col_amt - 4, y_top - 20, str(amount_header).upper(), "Helvetica", 11, accent)
        return y_top - 34

    row_y = draw_table_header(table_top)
    min_y = 2.15 * inch

    def next_page() -> float:
        pdf.showPage()
        pdf.setTitle(f"{doc_label.title()} - {display_no}")
        pdf.setStrokeColor(accent)
        pdf.setLineWidth(2)
        new_top = PAGE_H - M
        pdf.line(M, new_top, PAGE_W - M, new_top)
        pdf.setLineWidth(1)
        return draw_table_header(new_top)

    rows: list[tuple[str, str, str, float, str]] = []
    rate = float(inv.price_per_hour or 0.0)
    if bool(cfg.get("show_labor", True)):
        for li in inv.labor_items:
            hrs = float(li.labor_time_hours or 0.0)
            if hrs <= 0:
                continue
            amt = hrs * rate
            rows.append(((li.labor_desc or "Service"), _money(rate), f"{hrs:g}", amt, cfg.get("labor_title", "Labor")))
    if bool(cfg.get("show_parts", True)):
        for p in inv.parts:
            unit = float(inv.part_price_with_markup(p.part_price or 0.0) or 0.0)
            rows.append(((p.part_name or "Part"), _money(unit), "1", unit, cfg.get("parts_title", "Parts")))
    if bool(cfg.get("show_shop_supplies", True)) and float(inv.shop_supplies or 0.0):
        amt = float(inv.shop_supplies or 0.0)
        rows.append((cfg.get("shop_supplies_label", "Additional Fees"), _money(amt), "1", amt, "Additional charge"))

    pdf.setFont("Helvetica", 11)
    for desc, rate_txt, qty_txt, amount_num, subdesc in rows:
        wrapped = _wrap_text(desc, "Helvetica", 11, col_desc - 8)[:2] or [desc]
        needed = 18 + (len(wrapped) * 14)
        if row_y - needed < min_y:
            row_y = next_page()
        pdf.setFillColor(text_dark)
        yy = row_y
        pdf.setFont("Helvetica-Bold", 11)
        for ln in wrapped:
            pdf.drawString(x_desc, yy, ln)
            yy -= 14
        pdf.setFont("Helvetica", 10)
        pdf.setFillColor(muted)
        pdf.drawString(x_desc, yy, subdesc)
        pdf.setFillColor(text_dark)
        right_text(x_rate + col_rate - 4, row_y, rate_txt, "Helvetica", 10, text_dark)
        right_text(x_qty + col_qty - 4, row_y, qty_txt, "Helvetica", 10, text_dark)
        right_text(x_amt + col_amt - 4, row_y, _money(amount_num), "Helvetica", 10, text_dark)
        pdf.setStrokeColor(colors.HexColor("#d1d5db"))
        pdf.line(M, yy - 8, PAGE_W - M, yy - 8)
        row_y = yy - 24

    subtotal = float((inv.labor_total() if bool(cfg.get("show_labor", True)) else 0.0) + (inv.parts_total() if bool(cfg.get("show_parts", True)) else 0.0))
    if bool(cfg.get("show_shop_supplies", True)):
        subtotal += float(inv.shop_supplies or 0.0)
    tax = float(inv.tax_amount() or 0.0)
    total = float(total_with_fees or 0.0)
    paid = float(inv.paid or 0.0)
    amount_due = float(due_with_fees or 0.0)

    sum_right = PAGE_W - M
    sum_left = sum_right - 2.85 * inch
    sy = max(row_y, 1.8 * inch)
    pdf.setStrokeColor(colors.HexColor("#d1d5db"))
    pdf.line(sum_left, sy + 8, sum_right, sy + 8)
    pdf.setFillColor(text_dark)
    pdf.setFont("Helvetica", 12)
    sum_y = sy - 10
    right_text(sum_left + 1.35 * inch, sum_y, "Subtotal", "Helvetica", 12, text_dark)
    right_text(sum_right, sum_y, _money(subtotal), "Helvetica", 12, text_dark)
    if tax > 0:
        sum_y -= 20
        right_text(sum_left + 1.35 * inch, sum_y, _tax_label(inv), "Helvetica", 12, text_dark)
        right_text(sum_right, sum_y, _money(tax), "Helvetica", 12, text_dark)
    if late_fee_amount > 0 and not is_estimate:
        sum_y -= 20
        right_text(sum_left + 1.35 * inch, sum_y, "Late Fee", "Helvetica", 12, text_dark)
        right_text(sum_right, sum_y, _money(late_fee_amount), "Helvetica", 12, text_dark)
    pdf.setStrokeColor(colors.HexColor("#d1d5db"))
    divider_y = sum_y - 12
    pdf.line(sum_left, divider_y, sum_right, divider_y)
    total_y = divider_y - 20
    right_text(sum_left + 1.35 * inch, total_y, "Total", "Helvetica-Bold", 13, text_dark)
    right_text(sum_right, total_y, _money(total), "Helvetica-Bold", 13, text_dark)
    if not is_estimate and paid:
        paid_y = total_y - 20
        right_text(sum_left + 1.35 * inch, paid_y, "Paid", "Helvetica", 12, text_dark)
        right_text(sum_right, paid_y, _money(paid), "Helvetica", 12, text_dark)
        pdf.setStrokeColor(accent)
        pdf.setLineWidth(2)
        paid_divider_y = paid_y - 10
        pdf.line(sum_left, paid_divider_y, sum_right, paid_divider_y)
        pdf.setLineWidth(1)
        due_y = paid_divider_y - 20
        right_text(sum_left + 1.35 * inch, due_y, "Amount Due", "Helvetica-Bold", 13, text_dark)
        right_text(sum_right, due_y, _money(amount_due), "Helvetica-Bold", 13, text_dark)
    elif not is_estimate:
        due_y = total_y - 20
        right_text(sum_left + 1.35 * inch, due_y, "Amount Due", "Helvetica-Bold", 13, text_dark)
        right_text(sum_right, due_y, _money(amount_due), "Helvetica-Bold", 13, text_dark)

    notes_y = M + 0.95 * inch
    pdf.setFillColor(accent)
    pdf.setFont("Helvetica", 12)
    pdf.drawString(M, notes_y, "Notes")
    pdf.setFillColor(text_dark)
    pdf.setFont("Helvetica", 11)
    note_lines = _wrap_text((inv.notes or "Thank you for your business!"), "Helvetica", 11, 4.6 * inch)[:3]
    n_y = notes_y - 18
    for ln in note_lines:
        pdf.drawString(M, n_y, ln)
        n_y -= 14

    pdf.setFillColor(accent)
    pdf.setFont("Helvetica", 12)
    pdf.drawString(M, M + 0.25 * inch, "Terms")
    pdf.setFillColor(text_dark)
    pdf.setFont("Helvetica", 10)
    terms_text = (inv.useful_info or "").strip()
    if not terms_text:
        due_days = int(getattr(owner, "payment_due_days", 30) or 30) if owner else 30
        terms_text = f"Please pay within {max(0, due_days)} days using the link in your invoice email."
    pdf.drawString(M, M + 0.05 * inch, _wrap_text(terms_text, "Helvetica", 10, 5.4 * inch)[0])

    pdf.save()
    inv.pdf_path = pdf_path
    inv.pdf_generated_at = generated_dt
    session.add(inv)
    session.commit()
    return pdf_path


def _render_blueprint_pdf(
    *,
    session,
    inv: Invoice,
    owner: User | None,
    customer: Customer | None,
    customer_email: str,
    customer_phone: str,
    customer_address: str,
    cfg: dict,
    pdf_path: str,
    display_no: str,
    doc_label: str,
    generated_dt: datetime,
    generated_str: str,
    owner_logo_abs: str,
    owner_logo_blob: bytes | None,
    is_estimate: bool,
):
    PAGE_W, PAGE_H = LETTER
    M = 0.48 * inch
    rail_w = 2.02 * inch
    dark = colors.HexColor("#0b1220")
    dark2 = colors.HexColor("#121a2b")
    accent = colors.HexColor("#06b6d4")
    ink = colors.HexColor("#0f172a")
    muted = colors.HexColor("#475569")
    card_bg = colors.HexColor("#f8fafc")
    line = colors.HexColor("#d6dee7")

    pdf = canvas.Canvas(pdf_path, pagesize=LETTER)
    pdf.setTitle(f"{doc_label.title()} - {display_no}")
    total_with_fees, due_with_fees, late_fee_amount = _invoice_pdf_amounts(inv, owner, is_estimate=is_estimate)

    right_x0 = M + rail_w + 0.28 * inch
    right_w = PAGE_W - right_x0 - M

    def right_text(x, y, text, font="Helvetica", size=10, color=ink):
        pdf.setFont(font, size)
        pdf.setFillColor(color)
        pdf.drawRightString(x, y, str(text or ""))

    def page_chrome():
        pdf.setFillColor(colors.white)
        pdf.rect(0, 0, PAGE_W, PAGE_H, stroke=0, fill=1)
        pdf.setFillColor(dark)
        pdf.rect(M, M, rail_w, PAGE_H - (2 * M), stroke=0, fill=1)
        pdf.setFillColor(dark2)
        pdf.rect(M, PAGE_H - M - 1.9 * inch, rail_w, 0.5 * inch, stroke=0, fill=1)
        pdf.setFillColor(accent)
        accent_h = 0.06 * inch
        accent_y = M + ((PAGE_H - (2 * M)) / 2.0) - (accent_h / 2.0)
        pdf.rect(M, accent_y, rail_w, accent_h, stroke=0, fill=1)

    page_chrome()

    header_name = ((_business_header_name(owner) or "").strip()) if owner else ""
    info_lines_all = _business_header_info_lines(owner)
    rail_center = M + rail_w / 2.0

    # Logo area (only render when a real logo exists)
    logo_box_w = rail_w - 0.5 * inch
    logo_box_h = 1.05 * inch
    logo_x = M + (rail_w - logo_box_w) / 2.0
    logo_y = PAGE_H - M - 1.20 * inch
    logo_drawn = False
    if owner_logo_blob:
        try:
            img = ImageReader(io.BytesIO(owner_logo_blob))
            pdf.setFillColor(colors.white)
            pdf.roundRect(logo_x, logo_y - logo_box_h, logo_box_w, logo_box_h, 10, stroke=0, fill=1)
            pdf.drawImage(img, logo_x + 12, logo_y - logo_box_h + 12, width=logo_box_w - 24, height=logo_box_h - 24, preserveAspectRatio=True, mask="auto", anchor="c")
            logo_drawn = True
        except Exception:
            logo_drawn = False
    elif owner_logo_abs and os.path.exists(owner_logo_abs):
        try:
            img = ImageReader(owner_logo_abs)
            pdf.setFillColor(colors.white)
            pdf.roundRect(logo_x, logo_y - logo_box_h, logo_box_w, logo_box_h, 10, stroke=0, fill=1)
            pdf.drawImage(img, logo_x + 12, logo_y - logo_box_h + 12, width=logo_box_w - 24, height=logo_box_h - 24, preserveAspectRatio=True, mask="auto", anchor="c")
            logo_drawn = True
        except Exception:
            logo_drawn = False

    # Rail identity
    pdf.setFillColor(colors.white)
    pdf.setFont("Helvetica-Bold", 11)
    # If no logo exists, move identity block up so no empty logo space remains.
    name_y = PAGE_H - M - (2.62 * inch if logo_drawn else 2.30 * inch)
    header_display_name = _business_header_display_name(owner, "Your Business")
    if header_display_name:
        pdf.drawCentredString(rail_center, name_y, header_display_name)
    pdf.setFont("Helvetica", 9)
    yy = name_y - 0.20 * inch
    for ln in (info_lines_all[:4] or ["123 Work St", "Somewhere, ST 00000"]):
        pdf.drawCentredString(rail_center, yy, ln)
        yy -= 12

    # Header cards (right side)
    top = PAGE_H - M - 0.22 * inch
    pdf.setFillColor(ink)
    pdf.setFont("Helvetica-Bold", 24)
    pdf.drawString(right_x0, top - 8, doc_label)
    pdf.setFont("Helvetica", 10)
    pdf.setFillColor(muted)
    pdf.drawString(right_x0, top - 24, f"Generated {generated_str}")

    card_y = top - 44
    c_h = 54
    c_w = (right_w - 18) / 2.0
    # left card
    pdf.setFillColor(card_bg)
    pdf.setStrokeColor(line)
    pdf.roundRect(right_x0, card_y - c_h, c_w, c_h, 8, stroke=1, fill=1)
    pdf.setFillColor(muted)
    pdf.setFont("Helvetica-Bold", 9)
    pdf.drawString(right_x0 + 10, card_y - 14, "Document Number")
    pdf.setFillColor(ink)
    pdf.setFont("Helvetica-Bold", 14)
    pdf.drawString(right_x0 + 10, card_y - 34, display_no)
    # right card
    x2 = right_x0 + c_w + 18
    pdf.setFillColor(card_bg)
    pdf.setStrokeColor(line)
    pdf.roundRect(x2, card_y - c_h, c_w, c_h, 8, stroke=1, fill=1)
    pdf.setFillColor(muted)
    pdf.setFont("Helvetica-Bold", 9)
    pdf.drawString(x2 + 10, card_y - 14, "Amount Due" if not is_estimate else "Estimate Total")
    pdf.setFillColor(ink)
    pdf.setFont("Helvetica-Bold", 14)
    pdf.drawString(x2 + 10, card_y - 34, _money(due_with_fees if not is_estimate else total_with_fees))

    # Bill to + date panel
    block_top = card_y - c_h - 14
    left_w = right_w * 0.58
    right_block_w = right_w - left_w - 12
    bill_h = 114
    pdf.setFillColor(colors.white)
    pdf.setStrokeColor(line)
    pdf.roundRect(right_x0, block_top - bill_h, left_w, bill_h, 8, stroke=1, fill=1)
    pdf.setFillColor(accent)
    pdf.setFont("Helvetica-Bold", 10)
    pdf.drawString(right_x0 + 10, block_top - 14, "BILLED TO")
    pdf.setFillColor(ink)
    pdf.setFont("Helvetica-Bold", 12)
    pdf.drawString(right_x0 + 10, block_top - 32, (inv.name or getattr(customer, "name", None) or "Customer"))
    pdf.setFont("Helvetica", 10)
    by = block_top - 48
    for ln in _wrap_text(customer_address or "", "Helvetica", 10, left_w - 20)[:2]:
        pdf.drawString(right_x0 + 10, by, ln)
        by -= 13
    if customer_phone:
        pdf.drawString(right_x0 + 10, by, customer_phone)
        by -= 13
    if customer_email:
        pdf.drawString(right_x0 + 10, by, customer_email)

    pdf.setFillColor(colors.white)
    pdf.setStrokeColor(line)
    pdf.roundRect(right_x0 + left_w + 12, block_top - bill_h, right_block_w, bill_h, 8, stroke=1, fill=1)
    pdf.setFillColor(muted)
    pdf.setFont("Helvetica-Bold", 9)
    px = right_x0 + left_w + 22
    py = block_top - 16
    pdf.drawString(px, py, "Date Issued")
    pdf.setFillColor(ink)
    pdf.setFont("Helvetica-Bold", 11)
    pdf.drawString(px, py - 14, generated_str)
    py -= 36
    if not is_estimate:
        due_line = _invoice_due_date_line(inv, owner, is_estimate=is_estimate)
        if due_line:
            due_date_txt = due_line.replace("Payment due date:", "").strip()
            pdf.setFillColor(muted)
            pdf.setFont("Helvetica-Bold", 9)
            pdf.drawString(px, py, "Due Date")
            pdf.setFillColor(ink)
            pdf.setFont("Helvetica-Bold", 11)
            pdf.drawString(px, py - 14, due_date_txt)
            py -= 32
    pdf.setFillColor(muted)
    pdf.setFont("Helvetica-Bold", 9)
    pdf.drawString(px, py, "Status")
    pdf.setFillColor(ink)
    paid_flag = float(due_with_fees or 0.0) <= 0.0
    pdf.setFont("Helvetica-Bold", 11)
    pdf.drawString(px, py - 14, "Paid" if paid_flag else "Open")

    # Table
    table_top = block_top - bill_h - 18
    pdf.setFillColor(dark)
    pdf.rect(right_x0, table_top - 24, right_w, 24, stroke=0, fill=1)
    col_desc = right_w * 0.58
    col_qty = right_w * 0.10
    col_rate = right_w * 0.14
    col_amt = right_w - col_desc - col_qty - col_rate
    x_desc = right_x0
    x_qty = x_desc + col_desc
    x_rate = x_qty + col_qty
    x_amt = x_rate + col_rate
    row_desc_header = cfg.get("labor_desc_label", "Description")
    row_qty_header = cfg.get("labor_time_label", "Qty")
    row_rate_header = "Rate"
    row_amount_header = cfg.get("labor_total_label", "Amount")
    pdf.setFillColor(colors.white)
    pdf.setFont("Helvetica-Bold", 9)
    pdf.drawString(x_desc + 10, table_top - 16, str(row_desc_header).upper())
    right_text(x_qty + col_qty - 8, table_top - 16, str(row_qty_header).upper(), "Helvetica-Bold", 9, colors.white)
    right_text(x_rate + col_rate - 8, table_top - 16, str(row_rate_header).upper(), "Helvetica-Bold", 9, colors.white)
    right_text(x_amt + col_amt - 8, table_top - 16, str(row_amount_header).upper(), "Helvetica-Bold", 9, colors.white)

    row_y = table_top - 38
    min_y = 1.92 * inch

    def table_cont_page() -> float:
        pdf.showPage()
        page_chrome()
        top2 = PAGE_H - M - 0.25 * inch
        pdf.setFillColor(ink)
        pdf.setFont("Helvetica-Bold", 20)
        pdf.drawString(right_x0, top2 - 8, f"{doc_label} (cont.)")
        table_top2 = top2 - 34
        pdf.setFillColor(dark)
        pdf.rect(right_x0, table_top2 - 24, right_w, 24, stroke=0, fill=1)
        pdf.setFillColor(colors.white)
        pdf.setFont("Helvetica-Bold", 9)
        pdf.drawString(x_desc + 10, table_top2 - 16, str(row_desc_header).upper())
        right_text(x_qty + col_qty - 8, table_top2 - 16, str(row_qty_header).upper(), "Helvetica-Bold", 9, colors.white)
        right_text(x_rate + col_rate - 8, table_top2 - 16, str(row_rate_header).upper(), "Helvetica-Bold", 9, colors.white)
        right_text(x_amt + col_amt - 8, table_top2 - 16, str(row_amount_header).upper(), "Helvetica-Bold", 9, colors.white)
        return table_top2 - 38

    rows: list[tuple[str, str, str, float, str]] = []
    if bool(cfg.get("show_labor", True)):
        rate = float(inv.price_per_hour or 0.0)
        for li in inv.labor_items:
            hrs = float(li.labor_time_hours or 0.0)
            if hrs <= 0:
                continue
            amount = hrs * rate
            rows.append((li.labor_desc or "Service labor", f"{hrs:g}", _money(rate), amount, cfg.get("labor_title", "Labor")))
    if bool(cfg.get("show_parts", True)):
        for p in inv.parts:
            price = float(inv.part_price_with_markup(p.part_price or 0.0) or 0.0)
            rows.append((p.part_name or "Part", "1", _money(price), price, cfg.get("parts_title", "Parts")))
    if bool(cfg.get("show_shop_supplies", True)) and float(inv.shop_supplies or 0.0):
        amt = float(inv.shop_supplies or 0.0)
        rows.append((cfg.get("shop_supplies_label", "Additional Fees"), "1", _money(amt), amt, "Fees"))

    pdf.setStrokeColor(line)
    for desc, qty, rate_txt, amount, kind in rows:
        desc_lines = _wrap_text(desc, "Helvetica", 10, col_desc - 22)[:2] or [desc]
        needed = 18 + len(desc_lines) * 12
        if row_y - needed < min_y:
            row_y = table_cont_page()
        pdf.setFillColor(ink)
        pdf.setFont("Helvetica-Bold", 10)
        pdf.drawString(x_desc + 10, row_y, desc_lines[0])
        yy = row_y - 12
        if len(desc_lines) > 1:
            pdf.setFont("Helvetica", 9)
            pdf.setFillColor(muted)
            pdf.drawString(x_desc + 10, yy, desc_lines[1])
            yy -= 12
        pdf.setFont("Helvetica", 9)
        pdf.setFillColor(muted)
        pdf.drawString(x_desc + 10, yy, kind)
        pdf.setFillColor(ink)
        right_text(x_qty + col_qty - 8, row_y, qty, "Helvetica", 10, ink)
        right_text(x_rate + col_rate - 8, row_y, rate_txt, "Helvetica", 10, ink)
        right_text(x_amt + col_amt - 8, row_y, _money(amount), "Helvetica-Bold", 10, ink)
        pdf.setStrokeColor(line)
        pdf.line(right_x0, yy - 8, right_x0 + right_w, yy - 8)
        row_y = yy - 22

    # Totals
    subtotal = float((inv.labor_total() if bool(cfg.get("show_labor", True)) else 0.0) + (inv.parts_total() if bool(cfg.get("show_parts", True)) else 0.0))
    if bool(cfg.get("show_shop_supplies", True)):
        subtotal += float(inv.shop_supplies or 0.0)
    tax = float(inv.tax_amount() or 0.0)
    total = float(total_with_fees or 0.0)
    paid = float(inv.paid or 0.0)
    amount_due = float(due_with_fees or 0.0)

    if row_y < 2.2 * inch:
        row_y = table_cont_page()
    sum_w = 2.6 * inch
    sx = right_x0 + right_w - sum_w
    sy = row_y - 2
    pdf.setFillColor(card_bg)
    pdf.setStrokeColor(line)
    pdf.roundRect(sx, sy - 116, sum_w, 116, 8, stroke=1, fill=1)
    pdf.setFillColor(ink)
    pdf.setFont("Helvetica", 11)
    sum_y = sy - 18
    right_text(sx + sum_w - 10, sum_y, f"Subtotal   {_money(subtotal)}", "Helvetica", 11, ink)
    if tax > 0:
        sum_y -= 18
        right_text(sx + sum_w - 10, sum_y, f"{_tax_label(inv)}   {_money(tax)}", "Helvetica", 11, ink)
    if late_fee_amount > 0 and not is_estimate:
        sum_y -= 18
        right_text(sx + sum_w - 10, sum_y, f"Late Fee   {_money(late_fee_amount)}", "Helvetica", 11, ink)
    pdf.setStrokeColor(line)
    divider_y = sum_y - 10
    pdf.line(sx + 10, divider_y, sx + sum_w - 10, divider_y)
    total_y = divider_y - 18
    right_text(sx + sum_w - 10, total_y, f"Total   {_money(total)}", "Helvetica-Bold", 12, ink)
    if not is_estimate and paid:
        paid_y = total_y - 18
        right_text(sx + sum_w - 10, paid_y, f"Paid   {_money(paid)}", "Helvetica", 11, ink)
        pdf.setStrokeColor(accent)
        pdf.setLineWidth(2)
        paid_divider_y = paid_y - 8
        pdf.line(sx + 10, paid_divider_y, sx + sum_w - 10, paid_divider_y)
        pdf.setLineWidth(1)
        due_y = paid_divider_y - 18
        right_text(sx + sum_w - 10, due_y, f"Amount Due   {_money(amount_due)}", "Helvetica-Bold", 12, ink)
    elif not is_estimate:
        due_y = total_y - 18
        right_text(sx + sum_w - 10, due_y, f"Amount Due   {_money(amount_due)}", "Helvetica-Bold", 12, ink)

    # Notes strip
    notes_y = M + 0.75 * inch
    pdf.setFillColor(colors.white)
    pdf.setStrokeColor(line)
    pdf.roundRect(right_x0, notes_y - 0.65 * inch, right_w, 0.65 * inch, 8, stroke=1, fill=1)
    pdf.setFillColor(accent)
    pdf.setFont("Helvetica-Bold", 10)
    pdf.drawString(right_x0 + 10, notes_y - 16, "NOTES")
    pdf.setFillColor(ink)
    pdf.setFont("Helvetica", 9)
    note_text = (inv.notes or "Thank you for your business.").strip()
    note_line = _wrap_text(note_text, "Helvetica", 9, right_w - 24)[0]
    pdf.drawString(right_x0 + 10, notes_y - 31, note_line)

    pdf.save()
    inv.pdf_path = pdf_path
    inv.pdf_generated_at = generated_dt
    session.add(inv)
    session.commit()
    return pdf_path


def _render_luxe_pdf(
    *,
    session,
    inv: Invoice,
    owner: User | None,
    customer: Customer | None,
    customer_email: str,
    customer_phone: str,
    customer_address: str,
    cfg: dict,
    pdf_path: str,
    display_no: str,
    doc_label: str,
    generated_dt: datetime,
    generated_str: str,
    owner_logo_abs: str,
    owner_logo_blob: bytes | None,
    is_estimate: bool,
):
    PAGE_W, PAGE_H = LETTER
    M = 0.55 * inch
    bg = colors.HexColor("#f8f5ef")
    navy = colors.HexColor("#1f2933")
    royal = colors.HexColor("#8b5e34")
    cyan = colors.HexColor("#d4a373")
    ink = colors.HexColor("#111827")
    muted = colors.HexColor("#6b7280")
    line = colors.HexColor("#d9cfbf")
    card = colors.HexColor("#fffdf8")

    pdf = canvas.Canvas(pdf_path, pagesize=LETTER)
    pdf.setTitle(f"{doc_label.title()} - {display_no}")
    total_with_fees, due_with_fees, late_fee_amount = _invoice_pdf_amounts(inv, owner, is_estimate=is_estimate)

    def right_text(x, y, text, font="Helvetica", size=10, color=ink):
        pdf.setFont(font, size)
        pdf.setFillColor(color)
        pdf.drawRightString(x, y, str(text or ""))

    def draw_header_band():
        pdf.setFillColor(bg)
        pdf.rect(0, 0, PAGE_W, PAGE_H, stroke=0, fill=1)
        pdf.setFillColor(navy)
        pdf.roundRect(M, PAGE_H - M - 1.55 * inch, PAGE_W - (2 * M), 1.55 * inch, 14, stroke=0, fill=1)
        pdf.setFillColor(royal)
        pdf.roundRect(M, PAGE_H - M - 1.72 * inch, PAGE_W - (2 * M), 0.17 * inch, 8, stroke=0, fill=1)

    draw_header_band()

    # logo chip (only render when a real logo exists)
    chip_x = M + 16
    chip_y_top = PAGE_H - M - 16
    chip_w = 1.2 * inch
    chip_h = 1.1 * inch
    logo_drawn = False
    if owner_logo_blob:
        try:
            img = ImageReader(io.BytesIO(owner_logo_blob))
            pdf.setFillColor(colors.white)
            pdf.roundRect(chip_x, chip_y_top - chip_h, chip_w, chip_h, 10, stroke=0, fill=1)
            pdf.drawImage(img, chip_x + 8, chip_y_top - chip_h + 8, width=chip_w - 16, height=chip_h - 16, preserveAspectRatio=True, mask="auto", anchor="c")
            logo_drawn = True
        except Exception:
            logo_drawn = False
    elif owner_logo_abs and os.path.exists(owner_logo_abs):
        try:
            img = ImageReader(owner_logo_abs)
            pdf.setFillColor(colors.white)
            pdf.roundRect(chip_x, chip_y_top - chip_h, chip_w, chip_h, 10, stroke=0, fill=1)
            pdf.drawImage(img, chip_x + 8, chip_y_top - chip_h + 8, width=chip_w - 16, height=chip_h - 16, preserveAspectRatio=True, mask="auto", anchor="c")
            logo_drawn = True
        except Exception:
            logo_drawn = False

    # header text
    business = (_business_header_name(owner) or "").strip()
    owner_info_lines = _business_header_info_lines(owner)
    header_x = chip_x + (chip_w + 16 if logo_drawn else 0)
    pdf.setFillColor(colors.white)
    pdf.setFont("Helvetica-Bold", 23)
    pdf.drawString(header_x, PAGE_H - M - 34, doc_label)
    pdf.setFillColor(colors.HexColor("#eadac6"))
    pdf.setFont("Helvetica-Bold", 11)
    header_display_name = _business_header_display_name(owner, "Your Business")
    if header_display_name:
        pdf.drawString(header_x, PAGE_H - M - 52, header_display_name)
    pdf.setFont("Helvetica", 10)
    yy = PAGE_H - M - 68
    for ln in owner_info_lines[:4]:
        pdf.drawString(header_x, yy, ln)
        yy -= 12

    right_text(PAGE_W - M - 14, PAGE_H - M - 28, f"#{display_no}", "Helvetica-Bold", 14, colors.white)
    right_text(PAGE_W - M - 14, PAGE_H - M - 46, f"Issued: {generated_str}", "Helvetica", 10, colors.HexColor("#bfdbfe"))
    if not is_estimate:
        due_line = _invoice_due_date_line(inv, owner, is_estimate=is_estimate)
        if due_line:
            right_text(PAGE_W - M - 14, PAGE_H - M - 60, due_line.replace("Payment due date:", "Due:"), "Helvetica", 10, colors.HexColor("#bfdbfe"))

    # customer + summary cards
    cards_top = PAGE_H - M - 1.95 * inch
    left_w = (PAGE_W - 2 * M) * 0.6
    right_w = (PAGE_W - 2 * M) - left_w - 12
    left_x = M
    right_x = left_x + left_w + 12
    card_h = 108
    for x, w in ((left_x, left_w), (right_x, right_w)):
        pdf.setFillColor(card)
        pdf.setStrokeColor(line)
        pdf.roundRect(x, cards_top - card_h, w, card_h, 12, stroke=1, fill=1)
    pdf.setFillColor(royal)
    pdf.setFont("Helvetica-Bold", 10)
    pdf.drawString(left_x + 12, cards_top - 16, "BILLED TO")
    pdf.setFillColor(ink)
    pdf.setFont("Helvetica-Bold", 12)
    pdf.drawString(left_x + 12, cards_top - 34, (inv.name or getattr(customer, "name", None) or "Customer"))
    pdf.setFont("Helvetica", 10)
    by = cards_top - 49
    for ln in _wrap_text(customer_address or "", "Helvetica", 10, left_w - 24)[:2]:
        pdf.drawString(left_x + 12, by, ln)
        by -= 12
    if customer_phone:
        pdf.drawString(left_x + 12, by, customer_phone); by -= 12
    if customer_email:
        pdf.drawString(left_x + 12, by, customer_email)

    due_amt = due_with_fees if not is_estimate else total_with_fees
    pdf.setFillColor(royal)
    pdf.setFont("Helvetica-Bold", 10)
    pdf.drawString(right_x + 12, cards_top - 16, "TOTAL DUE" if not is_estimate else "ESTIMATE TOTAL")
    pdf.setFillColor(ink)
    pdf.setFont("Helvetica-Bold", 20)
    pdf.drawString(right_x + 12, cards_top - 42, _money(due_amt))
    pdf.setFont("Helvetica", 10)
    pdf.setFillColor(muted)
    paid_state = "Paid" if float(due_with_fees or 0.0) <= 0.0 else "Open"
    tax_amount = float(inv.tax_amount() or 0.0)
    right_status_y = cards_top - 60
    pdf.drawString(right_x + 12, right_status_y, f"Status: {paid_state}")
    next_meta_y = right_status_y - 14
    if tax_amount > 0:
        pdf.drawString(right_x + 12, next_meta_y, f"Tax: {_money(tax_amount)}")
        next_meta_y -= 14
    if late_fee_amount > 0 and not is_estimate:
        pdf.drawString(right_x + 12, next_meta_y, f"Late Fee: {_money(late_fee_amount)}")
        next_meta_y -= 14
    paid_amount = float(inv.paid or 0.0)
    if not is_estimate and paid_amount:
        pdf.drawString(right_x + 12, next_meta_y, f"Paid: {_money(inv.paid)}")

    def table_panel(x: float, y_top: float, w: float, title: str):
        panel_h = 24
        pdf.setFillColor(navy)
        pdf.roundRect(x, y_top - panel_h, w, panel_h, 8, stroke=0, fill=1)
        pdf.setFillColor(cyan)
        pdf.setFont("Helvetica-Bold", 11)
        pdf.drawString(x + 10, y_top - 16, title)
        return y_top - panel_h - 14

    def draw_rows(
        x: float,
        y: float,
        w: float,
        headers: list[str],
        rows: list[tuple[str, str, str]],
        min_bottom: float,
        start_index: int = 0,
    ):
        col1 = w * 0.60
        col2 = w * 0.18
        col3 = w - col1 - col2
        hx = [x, x + col1, x + col1 + col2]
        pdf.setFillColor(muted)
        pdf.setFont("Helvetica-Bold", 9)
        pdf.drawString(hx[0] + 8, y, headers[0])
        right_text(hx[1] + col2 - 8, y, headers[1], "Helvetica-Bold", 9, muted)
        right_text(hx[2] + col3 - 8, y, headers[2], "Helvetica-Bold", 9, muted)
        y -= 10
        pdf.setStrokeColor(line)
        pdf.line(x, y, x + w, y)
        y -= 8
        idx = max(0, int(start_index))
        while idx < len(rows):
            c1, c2, c3 = rows[idx]
            desc_lines = _wrap_text(c1, "Helvetica", 10, col1 - 16)[:2] or [c1]
            needed = len(desc_lines) * 10 + 10
            if y - needed < min_bottom:
                return y, idx
            pdf.setFillColor(ink)
            pdf.setFont("Helvetica", 10)
            y0 = y
            for ln in desc_lines:
                pdf.drawString(hx[0] + 8, y, ln); y -= 10
            right_text(hx[1] + col2 - 8, y0, c2, "Helvetica", 10, ink)
            right_text(hx[2] + col3 - 8, y0, c3, "Helvetica-Bold", 10, ink)
            row_bottom = y0 - (len(desc_lines) * 10)
            line_y = row_bottom + 2
            pdf.setStrokeColor(line)
            pdf.line(x, line_y, x + w, line_y)
            # Keep item spacing stable; only adjust where separator lines are drawn.
            y = row_bottom - 10
            idx += 1
        return y, idx

    tables_top = cards_top - card_h - 18
    table_w = PAGE_W - 2 * M
    min_bottom = 2.1 * inch

    def new_cont_page():
        pdf.showPage()
        draw_header_band()
        pdf.setFillColor(colors.white)
        pdf.setFont("Helvetica-Bold", 18)
        pdf.drawString(chip_x + chip_w + 16, PAGE_H - M - 34, f"{doc_label} (cont.)")
        return PAGE_H - M - 1.85 * inch

    # Labor table
    labor_rows: list[tuple[str, str, str]] = []
    if bool(cfg.get("show_labor", True)):
        rate = float(inv.price_per_hour or 0.0)
        for li in inv.labor_items:
            hrs = float(li.labor_time_hours or 0.0)
            if hrs <= 0:
                continue
            labor_rows.append((li.labor_desc or "Service labor", f"{hrs:g} hrs", _money(hrs * rate)))
    labor_title = (cfg.get("labor_title") or "Labor").upper()
    labor_desc_label = cfg.get("labor_desc_label", "Description")
    labor_time_label = cfg.get("labor_time_label", "Hours")
    labor_total_label = cfg.get("labor_total_label", "Line Total")
    has_labor_rows = bool(labor_rows)
    y = tables_top - 8
    if has_labor_rows:
        y = table_panel(M, tables_top, table_w, labor_title)
        labor_idx = 0
        y, labor_idx = draw_rows(
            M, y, table_w, [labor_desc_label, labor_time_label, labor_total_label], labor_rows, min_bottom, labor_idx
        )
        while labor_idx < len(labor_rows):
            tables_top = new_cont_page()
            y = table_panel(M, tables_top, table_w, f"{labor_title} (CONT.)")
            y, labor_idx = draw_rows(
                M, y, table_w, [labor_desc_label, labor_time_label, labor_total_label], labor_rows, min_bottom, labor_idx
            )

    # Parts table (separate always)
    parts_rows: list[tuple[str, str, str]] = []
    if bool(cfg.get("show_parts", True)):
        for p in inv.parts:
            price = float(inv.part_price_with_markup(p.part_price or 0.0) or 0.0)
            parts_rows.append((p.part_name or "Part", "1", _money(price)))
    has_parts_rows = bool(parts_rows)
    parts_top = y - 8
    if has_parts_rows and parts_top < (min_bottom + 110):
        parts_top = new_cont_page()
    parts_title = (cfg.get("parts_title") or "Parts").upper()
    parts_name_label = cfg.get("parts_name_label", "Part / Material")
    parts_price_label = cfg.get("parts_price_label", "Price")
    y2 = y
    if has_parts_rows:
        y2 = table_panel(M, parts_top, table_w, parts_title)
        parts_idx = 0
        y2, parts_idx = draw_rows(
            M, y2, table_w, [parts_name_label, "Qty", parts_price_label], parts_rows, min_bottom, parts_idx
        )
        while parts_idx < len(parts_rows):
            parts_top = new_cont_page()
            y2 = table_panel(M, parts_top, table_w, f"{parts_title} (CONT.)")
            y2, parts_idx = draw_rows(
                M, y2, table_w, [parts_name_label, "Qty", parts_price_label], parts_rows, min_bottom, parts_idx
            )

    # Footer totals strip
    subtotal = float((inv.labor_total() if bool(cfg.get("show_labor", True)) else 0.0) + (inv.parts_total() if bool(cfg.get("show_parts", True)) else 0.0))
    if bool(cfg.get("show_shop_supplies", True)):
        subtotal += float(inv.shop_supplies or 0.0)
    tax = float(inv.tax_amount() or 0.0)
    total = float(total_with_fees or 0.0)
    due = float(due_with_fees or 0.0)

    notes_h = 0.56 * inch
    notes_bottom = M + 0.16 * inch
    notes_top = notes_bottom + notes_h
    fy = max(notes_top + 58, y2 - 4)
    pdf.setFillColor(navy)
    pdf.roundRect(M, fy - 46, table_w, 46, 10, stroke=0, fill=1)
    pdf.setFillColor(colors.white)
    pdf.setFont("Helvetica", 11)
    subtotal_line = f"Subtotal {_money(subtotal)}"
    if tax > 0:
        subtotal_line = f"{subtotal_line}   •   {_tax_label(inv)} {_money(tax)}"
    if late_fee_amount > 0 and not is_estimate:
        subtotal_line = f"{subtotal_line}   •   Late Fee {_money(late_fee_amount)}"
    pdf.drawString(M + 12, fy - 18, subtotal_line)
    right_text(PAGE_W - M - 12, fy - 18, f"Total {_money(total)}", "Helvetica-Bold", 12, colors.white)
    if not is_estimate:
        pdf.setFillColor(colors.HexColor("#eadac6"))
        pdf.setFont("Helvetica-Bold", 12)
        right_text(PAGE_W - M - 12, fy - 35, f"Amount Due {_money(due)}", "Helvetica-Bold", 12, colors.HexColor("#eadac6"))

    # Notes field at bottom
    pdf.setFillColor(card)
    pdf.setStrokeColor(line)
    pdf.roundRect(M, notes_bottom, table_w, notes_h, 10, stroke=1, fill=1)
    pdf.setFillColor(royal)
    pdf.setFont("Helvetica-Bold", 10)
    pdf.drawString(M + 10, notes_top - 15, "NOTES")
    note_text = (inv.notes or "Thank you for your business.").strip()
    note_line = _wrap_text(note_text, "Helvetica", 9, table_w - 22)[0]
    pdf.setFillColor(ink)
    pdf.setFont("Helvetica", 9)
    pdf.drawString(M + 10, notes_top - 30, note_line)

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
    invoice_builder_design_override: dict | None = None,
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
    if pdf_template_key in PRO_ONLY_PDF_TEMPLATES and not _owner_has_pro_pdf_templates(owner):
        pdf_template_key = "classic"

    # Determine header identity lines (left side)
    header_name = _business_header_name(owner)
    header_info_lines = _business_header_info_lines(owner)

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
        design_obj = None
        if isinstance(invoice_builder_design_override, dict):
            design_obj = invoice_builder_design_override
        else:
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
                except Exception:
                    design_obj = None
        if isinstance(design_obj, dict):
            try:
                return _render_invoice_builder_pdf(
                    session=session,
                    inv=inv,
                    owner=owner,
                    customer=customer,
                    pdf_path=pdf_path,
                    generated_dt=generated_dt,
                    is_estimate=is_estimate,
                    design_obj=design_obj,
                    cfg=cfg,
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
    if pdf_template_key == "basic":
        return _render_basic_pdf(
            session=session,
            inv=inv,
            owner=owner,
            customer=customer,
            customer_email=customer_email,
            customer_phone=customer_phone,
            customer_address=customer_address,
            cfg=cfg,
            pdf_path=pdf_path,
            display_no=display_no,
            doc_label=doc_label,
            generated_dt=generated_dt,
            generated_str=generated_str,
            is_estimate=is_estimate,
        )
    if pdf_template_key == "simple":
        return _render_simple_pdf(
            session=session,
            inv=inv,
            owner=owner,
            customer=customer,
            customer_email=customer_email,
            customer_phone=customer_phone,
            customer_address=customer_address,
            cfg=cfg,
            pdf_path=pdf_path,
            display_no=display_no,
            doc_label=doc_label,
            generated_dt=generated_dt,
            generated_str=generated_str,
            owner_logo_abs=owner_logo_abs,
            owner_logo_blob=owner_logo_blob,
            is_estimate=is_estimate,
        )
    if pdf_template_key == "blueprint":
        return _render_blueprint_pdf(
            session=session,
            inv=inv,
            owner=owner,
            customer=customer,
            customer_email=customer_email,
            customer_phone=customer_phone,
            customer_address=customer_address,
            cfg=cfg,
            pdf_path=pdf_path,
            display_no=display_no,
            doc_label=doc_label,
            generated_dt=generated_dt,
            generated_str=generated_str,
            owner_logo_abs=owner_logo_abs,
            owner_logo_blob=owner_logo_blob,
            is_estimate=is_estimate,
        )
    if pdf_template_key == "luxe":
        return _render_luxe_pdf(
            session=session,
            inv=inv,
            owner=owner,
            customer=customer,
            customer_email=customer_email,
            customer_phone=customer_phone,
            customer_address=customer_address,
            cfg=cfg,
            pdf_path=pdf_path,
            display_no=display_no,
            doc_label=doc_label,
            generated_dt=generated_dt,
            generated_str=generated_str,
            owner_logo_abs=owner_logo_abs,
            owner_logo_blob=owner_logo_blob,
            is_estimate=is_estimate,
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
    header_display_name = _business_header_display_name(owner, "InvoiceRunner")
    if header_display_name:
        pdf.drawString(left_x, PAGE_H - 0.55 * inch, header_display_name)

    pdf.setFont("Helvetica", 9)
    info_lines = []
    for ln in header_info_lines:
        info_lines.extend(_wrap_text(ln, "Helvetica", 9, 3.6 * inch))
    info_y = PAGE_H - 0.82 * inch
    for ln in info_lines[:4]:
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
    def _start_cont_page():
        pdf.showPage()
        pdf.setFillColorRGB(0, 0, 0)
        pdf.setFont("Helvetica-Bold", 14)
        pdf.drawString(M, PAGE_H - M, f"{doc_label} (cont.)")
        pdf.setFont("Helvetica", 10)
        pdf.drawString(M, PAGE_H - M - 16, f"{display_no}  •  Generated: {generated_str}")
        return PAGE_H - M - 36

    def draw_table(title, x, y_top, col_titles, rows, col_widths, money_cols=None):
        money_cols = set(money_cols or [])
        base_row_h = 14 if builder_compact_mode else 16
        title_gap = 16
        min_content_y = 1.0 * inch

        def draw_table_header(header_title: str, y_top_local: float):
            pdf.setFont("Helvetica-Bold", 12)
            pdf.drawString(x, y_top_local, header_title)
            y_local = y_top_local - title_gap

            table_w_local = sum(col_widths)
            pdf.setFillColorRGB(0.95, 0.95, 0.95)
            pdf.rect(x, y_local - base_row_h + 10, table_w_local, base_row_h, stroke=0, fill=1)
            pdf.setFillColorRGB(0, 0, 0)
            pdf.setFont("Helvetica-Bold", 10)

            cx_local = x
            for i, h in enumerate(col_titles):
                if i in money_cols:
                    right_text(cx_local + col_widths[i] - 6, y_local, h, "Helvetica-Bold", 10)
                else:
                    pdf.drawString(cx_local + 6, y_local, h)
                cx_local += col_widths[i]

            pdf.setStrokeColor(colors.black)
            pdf.setLineWidth(0.5)
            pdf.line(x, y_local - 4, x + table_w_local, y_local - 4)
            return y_local - base_row_h

        table_w = sum(col_widths)
        y_cursor = draw_table_header(title, y_top)
        pdf.setFont("Helvetica", 10)

        for row in rows:
            wrapped_cells = []
            row_height = base_row_h

            for i, cell in enumerate(row):
                max_w = col_widths[i] - 12
                lines = _wrap_text(cell, "Helvetica", 10, max_w)
                wrapped_cells.append(lines)
                row_height = max(row_height, len(lines) * base_row_h)

            if (y_cursor - row_height) < min_content_y:
                next_top = _start_cont_page()
                y_cursor = draw_table_header(f"{title} (cont.)", next_top)
                pdf.setFont("Helvetica", 10)
                pdf.setFillColor(colors.black)

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

    has_labor_rows = any((row[0] or row[1] or row[2]) for row in labor_rows)
    if show_labor and has_labor_rows:
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
    notes_floor = 2.2 * inch + M
    if (body_y - 10) < notes_floor:
        body_y = _start_cont_page()
    notes_y_top = max(body_y - 10, notes_floor)

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
    total_price, price_owed, late_fee_amount = _invoice_pdf_amounts(inv, owner, is_estimate=is_estimate)
    tax_amount = inv.tax_amount()

    summary_rows = 1  # Total / Estimated Total
    if show_parts and has_parts_rows and total_parts:
        summary_rows += 1
    if show_labor and has_labor_rows and total_labor:
        summary_rows += 1
    if show_shop_supplies and inv.shop_supplies:
        summary_rows += 1
    if tax_amount:
        summary_rows += 1
    if late_fee_amount > 0 and not is_estimate:
        summary_rows += 1
    paid_amount = float(inv.paid or 0.0)
    if not is_estimate and paid_amount:
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
    if show_labor and has_labor_rows and total_labor:
        label_right_value(sum_x + 10, right_edge, y, f"{cfg['labor_title']}:", _money(total_labor)); y -= 16
    if show_shop_supplies and inv.shop_supplies:
        label_right_value(sum_x + 10, right_edge, y, f"{cfg['shop_supplies_label']}:", _money(inv.shop_supplies)); y -= 16
    if tax_amount:
        label_right_value(sum_x + 10, right_edge, y, f"{_tax_label(inv)}:", _money(tax_amount)); y -= 16
    if late_fee_amount > 0 and not is_estimate:
        label_right_value(sum_x + 10, right_edge, y, "Late Fee:", _money(late_fee_amount)); y -= 16

    pdf.setStrokeColor(colors.HexColor("#DDDDDD"))
    pdf.line(sum_x + 10, y + 4, sum_x + sum_w - 10, y + 4)
    pdf.setStrokeColor(colors.black)
    y -= 10

    label = "Estimated Total:" if is_estimate else "Total:"
    label_right_value(sum_x + 10, right_edge, y, label, _money(total_price)); y -= 18

    if not is_estimate and paid_amount:
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
    business_income: float,
    other_income: float,
    interest_income: float,
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
    right_text(PAGE_W - M, y, _money(business_income), "Helvetica", 10)
    y -= 18
    pdf.drawString(M + 14, y, "Other Income")
    right_text(PAGE_W - M, y, _money(other_income), "Helvetica", 10)
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

    pdf.setFont("Helvetica-Bold", 12)
    pdf.drawString(M, y, "Interest Income")
    y -= 18
    pdf.setFont("Helvetica", 10)
    pdf.drawString(M + 14, y, "Interest Income")
    right_text(PAGE_W - M, y, _money(interest_income), "Helvetica", 10)
    y -= 26

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
