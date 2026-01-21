# pdf_service.py
import os
import re
from datetime import datetime
from pathlib import Path

from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import LETTER
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.lib.utils import ImageReader

from config import Config
from models import Invoice, User, Customer


def _money(x) -> str:
    try:
        return f"${float(x):,.2f}"
    except Exception:
        return f"${x}"


def _safe_filename(name: str) -> str:
    # strip characters not allowed on Windows/mac paths
    return re.sub(r'[\\/*?:"<>|]', "", (name or "")).strip() or "Invoice"


def _wrap_text(text, font, size, max_width):
    words = str(text).split()
    lines = []
    current = ""
    for w in words:
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


def generate_and_store_pdf(session, invoice_id: int) -> str:
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
            "labor_desc_label": "Labor Description",
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
            "labor_desc_label": "Service Description",
            "labor_time_label": "Time",
            "labor_total_label": "Line Total",

            "parts_title": "Materials",
            "parts_name_label": "Material",
            "parts_price_label": "Price",

            "shop_supplies_label": "Supplies / Fees",
        },
        "accountant": {
            "job_label": "Client / Engagement",
            "job_box_title": "ENGAGEMENT DETAILS",
            "job_rate_label": "Hourly Rate",
            "job_hours_label": "Hours Billed",
            "hours_suffix": "hrs",

            "labor_title": "Services",
            "labor_desc_label": "Service / Task",
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
            "labor_desc_label": "Service Description",
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
            "labor_desc_label": "Service Description",
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
            "labor_desc_label": "Sale Description",
            "labor_time_label": "Qty",
            "labor_total_label": "Line Total",

            "parts_title": "Costs",
            "parts_name_label": "Cost Item",
            "parts_price_label": "Amount",

            "shop_supplies_label": "Other Expenses",
        },
    }

    template_key = (getattr(inv, "invoice_template", None) or "").strip() or (
        (getattr(owner, "invoice_template", None) or "").strip() if owner else ""
    )
    if template_key not in TEMPLATE_CFG:
        template_key = "auto_repair"
    cfg = TEMPLATE_CFG[template_key]

    # Determine header identity lines (left side)
    business_name = (getattr(owner, "business_name", None) or "").strip() if owner else ""
    username = (getattr(owner, "username", None) or "").strip() if owner else ""
    header_name = business_name or username or ""

    header_address = (getattr(owner, "address", None) or "").strip() if owner else ""
    header_phone = (getattr(owner, "phone", None) or "").strip() if owner else ""

    # Owner logo (stored relative to instance/)
    owner_logo_rel = (getattr(owner, "logo_path", None) or "").strip() if owner else ""
    owner_logo_abs = ""
    if owner_logo_rel:
        owner_logo_abs = str((Path("instance") / owner_logo_rel).resolve())

    # Ensure parts + labor loaded
    parts = inv.parts
    labor_items = inv.labor_items

    PAGE_W, PAGE_H = LETTER
    M = 0.75 * inch

    generated_dt = datetime.now()
    generated_str = generated_dt.strftime("%B %d, %Y")

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
    pdf = canvas.Canvas(pdf_path, pagesize=LETTER)
    pdf.setTitle(f"{doc_label.title()} - {inv.invoice_number}")

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
        pdf.drawString(M, PAGE_H - M - 16, f"{inv.invoice_number}  •  Generated: {generated_str}")

    # -----------------------------
    # Header (white, low-ink)
    # -----------------------------
    pdf.setFillColorRGB(1, 1, 1)
    pdf.rect(0, PAGE_H - 1.15 * inch, PAGE_W, 1.15 * inch, stroke=0, fill=1)

    pdf.setStrokeColor(colors.HexColor("#CCCCCC"))
    pdf.setLineWidth(1)
    pdf.line(M, PAGE_H - 1.30 * inch, PAGE_W - M, PAGE_H - 1.30 * inch)

    pdf.setFillColorRGB(0, 0, 0)
    pdf.setFont("Helvetica-Bold", 20)
    pdf.drawString(M, PAGE_H - 0.75 * inch, doc_label)

    # Optional logo on far left
    logo_w = 0
    if owner_logo_abs and os.path.exists(owner_logo_abs):
        try:
            img = ImageReader(owner_logo_abs)
            iw, ih = img.getSize()

            max_h = 0.4 * inch
            max_w = 1.05 * inch

            scale = min(max_w / float(iw), max_h / float(ih))
            w = float(iw) * scale
            h = float(ih) * scale

            x = M
            y = (PAGE_H - .81 * inch) - h

            pdf.drawImage(img, x, y, width=w, height=h, mask="auto")
            logo_w = w + 10  # spacing after logo
        except Exception:
            logo_w = 0


    # Business info (left)
    pdf.setFont("Helvetica", 10)
    business_x = M + logo_w


    left_lines = []
    if header_name:
        left_lines.append(header_name)

    if header_address:
        max_w = (PAGE_W / 2) - M
        addr_lines = _wrap_text(header_address, "Helvetica", 10, max_w)
        left_lines.extend(addr_lines[:2])

    if header_phone:
        left_lines.append(header_phone)

    y_positions = [PAGE_H - 0.98 * inch, PAGE_H - 1.12 * inch, PAGE_H - 1.26 * inch]
    for i, y in enumerate(y_positions):
        if i < len(left_lines):
            pdf.drawString(business_x, y, left_lines[i])


    # Meta (right)
    meta_x = PAGE_W - M
    meta_y = PAGE_H - 0.78 * inch
    right_text(meta_x, meta_y, f"{doc_label.title()} #: {inv.invoice_number}", "Helvetica", 10)
    right_text(meta_x, meta_y - 14, f"{doc_label.title()} Date: {generated_str}", "Helvetica", 10)
    right_text(meta_x, meta_y - 28, f"Date In: {inv.date_in}", "Helvetica", 10)

    # -----------------------------
    # Bill To + Job Details boxes
    # -----------------------------
    top_y = PAGE_H - 1.45 * inch
    box_h = 1.05 * inch
    box_w = (PAGE_W - 2 * M - 0.35 * inch) / 2

    pdf.setStrokeColor(colors.black)
    pdf.setLineWidth(1)

    # Bill To
    pdf.roundRect(M, top_y - box_h, box_w, box_h, 8, stroke=1, fill=0)
    pdf.setFont("Helvetica-Bold", 11)
    pdf.drawString(M + 10, top_y - 16, "BILL TO")

    # Prefer Customer.name when available (otherwise invoice legacy name parsing)
    if customer and (getattr(customer, "name", None) or "").strip():
        customer_name = (customer.name or "").strip()
    else:
        nameFirst, _, _tail = (inv.name or "").partition(":")
        customer_name = (nameFirst or inv.name or "").strip()

    # Layout: left = contact, right = address
    left_x = M + 10
    right_x = M + (box_w / 2) + 5

    name_y = top_y - 34
    line_step = 12

    # LEFT: Name, Phone, Email
    pdf.setFont("Helvetica", 11)
    pdf.drawString(left_x, name_y, customer_name)

    pdf.setFont("Helvetica", 9)
    y_left = name_y - line_step

    if customer_phone:
        pdf.drawString(left_x, y_left, f"Phone: {customer_phone}")
        y_left -= line_step

    if customer_email:
        max_w_left = (box_w / 2) - 20
        email_lines = _wrap_text(f"Email: {customer_email}", "Helvetica", 9, max_w_left)
        for ln in email_lines[:2]:
            pdf.drawString(left_x, y_left, ln)
            y_left -= line_step

    # RIGHT: Address (stacked)
    pdf.setFont("Helvetica", 9)
    y_right = name_y

    if customer_address:
        max_w_right = (box_w / 2) - 20
        addr_lines = _wrap_text(customer_address, "Helvetica", 9, max_w_right)
        for ln in addr_lines[:4]:
            pdf.drawString(right_x, y_right, ln)
            y_right -= line_step

    
    # Job Details (wrapped job/address line)
    x2 = M + box_w + 0.35 * inch
    pdf.roundRect(x2, top_y - box_h, box_w, box_h, 8, stroke=1, fill=0)

    pdf.setFont("Helvetica-Bold", 11)
    pdf.drawString(x2 + 10, top_y - 18, cfg.get("job_box_title", "JOB DETAILS"))

    pdf.setFont("Helvetica", 10)

    job_text = f"{cfg['job_label']}: {inv.vehicle or ''}"
    max_job_w = box_w - 20
    job_lines = _wrap_text(job_text, "Helvetica", 10, max_job_w)

    y_job = top_y - 32
    line_step = 14

    for ln in job_lines[:2]:  # limit so rate/hours still fit
        pdf.drawString(x2 + 10, y_job, ln)
        y_job -= line_step

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
        base_row_h = 16
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
        parts_rows.append([p.part_name or "", _money(p.part_price or 0.0) if (p.part_price or 0.0) else ""])

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
    all_note_lines = _split_notes_into_lines(inv.notes or "", max_width, font="Helvetica", size=10)

    footer_y = 0.55 * inch
    footer_clearance = 0.20 * inch
    page_bottom_limit = footer_y + footer_clearance

    needed_text_h = 0
    for ln in all_note_lines:
        needed_text_h += SPACER_GAP if ln == "__SPACER__" else line_height
    needed_box_h = header_title_gap + needed_text_h + bottom_padding

    max_box_h_this_page = notes_y_top - page_bottom_limit
    notes_box_h = min(max_box_h_this_page, needed_box_h)

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
    sum_h = 1.8 * inch
    pdf.roundRect(sum_x, notes_y_top - sum_h, sum_w, sum_h, 8, stroke=1, fill=0)

    pdf.setFont("Helvetica-Bold", 11)
    pdf.drawString(sum_x + 10, notes_y_top - 18, "SUMMARY")

    total_parts = inv.parts_total()
    total_labor = inv.labor_total()
    total_price = inv.invoice_total()
    price_owed = inv.amount_due()

    # ✅ Right-align all $ values to the right edge of the summary box
    right_edge = sum_x + sum_w - 12
    y = notes_y_top - 42

    label_right_value(sum_x + 10, right_edge, y, f"{cfg['parts_title']}:", _money(total_parts)); y -= 16
    label_right_value(sum_x + 10, right_edge, y, f"{cfg['labor_title']}:", _money(total_labor)); y -= 16
    label_right_value(sum_x + 10, right_edge, y, f"{cfg['shop_supplies_label']}:", _money(inv.shop_supplies)); y -= 16

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
        pdf.drawString(M, footer_y, "Thank you for your business.")
        pdf.setFillColorRGB(0, 0, 0)

    if remaining_lines:
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
