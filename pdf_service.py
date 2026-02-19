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
    return key if key in {"classic", "modern", "split_panel", "strip"} else "classic"


def _builder_template_vars(inv: Invoice, owner: User | None, customer: Customer | None, *, is_estimate: bool) -> dict[str, str]:
    due_line = _invoice_due_date_line(inv, owner, is_estimate=is_estimate)
    due_date = ""
    if due_line:
        due_date = due_line.replace("Payment due date:", "").strip()
    business_name = (getattr(owner, "business_name", None) or getattr(owner, "username", None) or "InvoiceRunner").strip()
    owner_phone = _format_phone((getattr(owner, "phone", None) or "").strip())
    owner_addr = "\n".join(_owner_address_lines(owner))
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
        "business_phone": owner_phone,
        "business_address": owner_addr,
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
        font_weight = int(el.get("fontWeight") or 500)
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
        left_x = rx + (6 * scale_x)
        right_x = rx + rw - (6 * scale_x)
        center_x = rx + (rw / 2.0)
        ty = ry + rh - line_h
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
                _w, h_used = para.wrap(max_w, max(10.0, rh - 4))
                draw_y = ry + rh - h_used - 2
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
