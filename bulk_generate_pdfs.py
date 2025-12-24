# bulk_generate_pdfs.py
import argparse
import os
from pathlib import Path

from config import Config
from models import Base, make_engine, make_session_factory, Invoice
from pdf_service import generate_and_store_pdf


def main():
    parser = argparse.ArgumentParser(description="Bulk generate invoice PDFs.")
    parser.add_argument("--year", type=str, default="", help="Only generate PDFs for a given year (YYYY).")
    parser.add_argument("--all", action="store_true", help="Regenerate PDFs even if one already exists.")
    args = parser.parse_args()

    # Ensure exports dir exists
    Path(Config.EXPORTS_DIR).mkdir(parents=True, exist_ok=True)

    engine = make_engine(Config.SQLALCHEMY_DATABASE_URI, echo=Config.SQLALCHEMY_ECHO)
    Base.metadata.create_all(engine)
    SessionLocal = make_session_factory(engine)

    target_year = (args.year or "").strip()
    if target_year and not (target_year.isdigit() and len(target_year) == 4):
        raise SystemExit("Year must be 4 digits, e.g. --year 2025")

    with SessionLocal() as s:
        q = s.query(Invoice).order_by(Invoice.created_at.asc())

        if target_year:
            q = q.filter(Invoice.invoice_number.startswith(target_year))

        invoices = q.all()

        if not invoices:
            print("No invoices found for the given filter.")
            return

        total = len(invoices)
        generated = 0
        skipped = 0
        failed = 0

        for i, inv in enumerate(invoices, start=1):
            try:
                has_pdf = bool(inv.pdf_path) and os.path.exists(inv.pdf_path or "")
                if has_pdf and not args.all:
                    skipped += 1
                    print(f"[{i}/{total}] SKIP  {inv.invoice_number} (already has PDF)")
                    continue

                path = generate_and_store_pdf(s, inv.id)
                generated += 1
                print(f"[{i}/{total}] DONE  {inv.invoice_number} -> {path}")

            except Exception as e:
                failed += 1
                print(f"[{i}/{total}] FAIL  {inv.invoice_number}  ({e})")

        print("\n✅ Bulk PDF generation complete.")
        print(f"Generated: {generated}")
        print(f"Skipped:   {skipped}")
        print(f"Failed:    {failed}")
        print(f"Exports:   {Config.EXPORTS_DIR}")


if __name__ == "__main__":
    main()

