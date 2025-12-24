# import_csv.py
import csv
import re
from datetime import datetime
from pathlib import Path

from config import Config
from models import (
    Base, make_engine, make_session_factory,
    Invoice, InvoicePart, InvoiceLabor, next_invoice_number
)

CSV_FILE = "invoices.csv"

HEADERS = [
    "Name", "Vehicle", "Hours", "Price Per Hour",
    "Part Name", "Price Per Part",
    "Shop Supplies", "Notes", "Paid", "Date In",
    "Labor", "Labor Time"
]


def _to_float(s, default=0.0) -> float:
    try:
        s = (s or "").strip()
        return float(s) if s else float(default)
    except Exception:
        return float(default)


def _parse_csv_list(s: str):
    return [x.strip() for x in (s or "").split(",") if x.strip()]


def _parse_year_from_datein(date_in: str):
    """
    Tries to extract a 4-digit year from Date In.
    Supports common formats and fallback "contains YYYY".
    """
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


def _notes_csv_to_text(notes_raw: str) -> str:
    """
    Old system stored notes comma-separated. Convert to multi-line text.
    """
    lines = _parse_csv_list(notes_raw)
    return "\n".join(lines).strip()


def main():
    # Ensure instance/ exists for SQLite local dev
    if Config.SQLALCHEMY_DATABASE_URI.startswith("sqlite"):
        Path("instance").mkdir(parents=True, exist_ok=True)

    engine = make_engine(Config.SQLALCHEMY_DATABASE_URI, echo=Config.SQLALCHEMY_ECHO)
    Base.metadata.create_all(engine)
    SessionLocal = make_session_factory(engine)

    csv_path = Path(CSV_FILE)
    if not csv_path.exists():
        raise FileNotFoundError(f"Could not find {CSV_FILE} in: {Path.cwd()}")

    created = 0
    skipped = 0

    with open(csv_path, "r", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        header = next(reader, None)

        # If the CSV has no header or a weird one, still try to import by position.
        # We assume your old layout ordering.
        for row_idx, row in enumerate(reader, start=2):
            row = (row + [""] * len(HEADERS))[:len(HEADERS)]

            name = row[0].strip()
            vehicle = row[1].strip()
            if not name or not vehicle:
                skipped += 1
                continue

            hours = _to_float(row[2], 0.0)
            rate = _to_float(row[3], 0.0)
            part_names = _parse_csv_list(row[4])
            part_prices = _parse_csv_list(row[5])
            supplies = _to_float(row[6], 0.0)
            notes_text = _notes_csv_to_text(row[7])
            paid = _to_float(row[8], 0.0)
            date_in = row[9].strip()

            labor_descs = _parse_csv_list(row[10])
            labor_times = _parse_csv_list(row[11])

            # Determine invoice-number year: prefer Date In year, else current year
            yr = _parse_year_from_datein(date_in) or int(datetime.now().strftime("%Y"))

            with SessionLocal() as s:
                # Optional: skip duplicates (same Name+Vehicle+Date In+Hours+Rate)
                # This prevents importing the same CSV multiple times.
                existing = (
                    s.query(Invoice)
                    .filter(
                        Invoice.name == name,
                        Invoice.vehicle == vehicle,
                        Invoice.date_in == date_in,
                        Invoice.hours == hours,
                        Invoice.price_per_hour == rate,
                    )
                    .first()
                )
                if existing:
                    skipped += 1
                    continue

                inv_no = next_invoice_number(s, year=yr, seq_width=Config.INVOICE_SEQ_WIDTH)

                inv = Invoice(
                    invoice_number=inv_no,
                    name=name,
                    vehicle=vehicle,
                    hours=hours,
                    price_per_hour=rate,
                    shop_supplies=supplies,
                    paid=paid,
                    date_in=date_in,
                    notes=notes_text,
                )

                # Parts (pair by index; if counts mismatch, we keep what we can)
                n_parts = min(len(part_names), len(part_prices))
                for i in range(n_parts):
                    inv.parts.append(
                        InvoicePart(
                            part_name=part_names[i],
                            part_price=_to_float(part_prices[i], 0.0),
                        )
                    )

                # Labor items (pair by index; if counts mismatch, we keep what we can)
                n_labor = min(len(labor_descs), len(labor_times))
                for i in range(n_labor):
                    inv.labor_items.append(
                        InvoiceLabor(
                            labor_desc=labor_descs[i],
                            labor_time_hours=_to_float(labor_times[i], 0.0),
                        )
                    )

                s.add(inv)
                s.commit()
                created += 1

    print("✅ Import complete.")
    print(f"Created: {created}")
    print(f"Skipped: {skipped} (missing fields or duplicate detection)")


if __name__ == "__main__":
    main()

