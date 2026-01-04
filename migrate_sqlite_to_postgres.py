#!/usr/bin/env python3
"""
Migrate legacy invoices from local SQLite DB to Postgres, assigning them to a user.

Usage examples:

  # simplest (uses default sqlite path, prompts via env or args)
  python migrate_sqlite_to_postgres.py \
    --pg-url "postgresql://USER:PASS@HOST/DB?sslmode=require" \
    --username "CharlieMelnarik"

  # specify sqlite path explicitly
  python migrate_sqlite_to_postgres.py \
    --sqlite-path "instance/invoices.db" \
    --pg-url "postgresql://USER:PASS@HOST/DB?sslmode=require" \
    --username "CharlieMelnarik"
"""

import argparse
from datetime import datetime
from typing import Dict, Tuple, Optional

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, selectinload

# Import your app models
from models import Base, User, Invoice, InvoicePart, InvoiceLabor, InvoiceSequence


def _parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--sqlite-path", default="instance/invoices.db", help="Path to legacy SQLite DB")
    p.add_argument("--pg-url", required=True, help="Postgres URL (use EXTERNAL URL from Render for local migration)")
    p.add_argument("--username", required=True, help="Target username in Postgres to own the migrated invoices")
    p.add_argument("--dry-run", action="store_true", help="Do not write anything; just report what would happen")
    return p.parse_args()


def _sqlite_url(path: str) -> str:
    # absolute path is safest
    if path.startswith("sqlite:///"):
        return path
    return f"sqlite:///{path}"


def _normalize_pg_url(url: str) -> str:
    """
    Your app normalizes DATABASE_URL to postgresql+psycopg://.
    For this script we can just use SQLAlchemy's psycopg dialect explicitly.
    """
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    if url.startswith("postgresql+psycopg://"):
        return url
    if url.startswith("postgresql://"):
        return url.replace("postgresql://", "postgresql+psycopg://", 1)
    return url


def _get_or_error_user(pg_sess, username: str) -> User:
    u = pg_sess.query(User).filter(User.username == username).first()
    if not u:
        raise SystemExit(
            f"Target user '{username}' not found in Postgres.\n"
            f"Create it first via your deployed app /register, then rerun."
        )
    return u


def _get_existing_invoice_numbers(pg_sess, user_id: int) -> set:
    rows = pg_sess.execute(
        text("SELECT invoice_number FROM invoices WHERE user_id = :uid"),
        {"uid": user_id}
    ).fetchall()
    return {r[0] for r in rows}


def _year_from_invoice_number(inv_no: str) -> Optional[int]:
    if inv_no and len(inv_no) >= 4 and inv_no[:4].isdigit():
        return int(inv_no[:4])
    return None


def main():
    args = _parse_args()

    sqlite_engine = create_engine(_sqlite_url(args.sqlite_path), future=True)
    pg_engine = create_engine(
        _normalize_pg_url(args.pg_url),
        future=True,
        pool_pre_ping=True,
        pool_recycle=300,
    )

    SQLiteSession = sessionmaker(bind=sqlite_engine, future=True)
    PGSession = sessionmaker(bind=pg_engine, future=True)

    # Ensure Postgres tables exist (safe if already exist)
    Base.metadata.create_all(bind=pg_engine)

    with SQLiteSession() as s_sqlite, PGSession() as s_pg:
        target_user = _get_or_error_user(s_pg, args.username)
        existing_numbers = _get_existing_invoice_numbers(s_pg, target_user.id)

        # Load all legacy invoices + relationships from SQLite
        legacy_invoices = (
            s_sqlite.query(Invoice)
            .options(selectinload(Invoice.parts), selectinload(Invoice.labor_items))
            .order_by(Invoice.created_at.asc())
            .all()
        )

        print(f"Found {len(legacy_invoices)} invoices in SQLite.")
        print(f"User '{args.username}' (id={target_user.id}) currently has {len(existing_numbers)} invoices in Postgres.")

        to_insert = [inv for inv in legacy_invoices if inv.invoice_number not in existing_numbers]
        print(f"Will migrate {len(to_insert)} invoices (skipping {len(legacy_invoices) - len(to_insert)} duplicates by invoice_number).")

        if args.dry_run:
            print("DRY RUN: no changes will be written.")
            return

        # Track max sequence per year so invoice_sequences stays correct
        max_seq_by_year: Dict[int, int] = {}

        migrated_count = 0

        for old in to_insert:
            new_inv = Invoice(
                user_id=target_user.id,
                invoice_number=old.invoice_number,
                name=old.name,
                vehicle=old.vehicle,
                hours=float(old.hours or 0.0),
                price_per_hour=float(old.price_per_hour or 0.0),
                shop_supplies=float(old.shop_supplies or 0.0),
                notes=old.notes or "",
                paid=float(old.paid or 0.0),
                date_in=old.date_in or "",
                pdf_path=getattr(old, "pdf_path", None),
                pdf_generated_at=getattr(old, "pdf_generated_at", None),
                created_at=getattr(old, "created_at", datetime.utcnow()),
                updated_at=getattr(old, "updated_at", datetime.utcnow()),
            )

            # parts
            for p in (old.parts or []):
                new_inv.parts.append(
                    InvoicePart(
                        part_name=p.part_name or "",
                        part_price=float(p.part_price or 0.0),
                    )
                )

            # labor
            for li in (old.labor_items or []):
                new_inv.labor_items.append(
                    InvoiceLabor(
                        labor_desc=li.labor_desc or "",
                        labor_time_hours=float(li.labor_time_hours or 0.0),
                    )
                )

            s_pg.add(new_inv)
            migrated_count += 1

            # update max seq tracking from invoice_number
            yr = _year_from_invoice_number(old.invoice_number)
            if yr is not None:
                # invoice_number format: YYYY###### (width 6)
                tail = old.invoice_number[4:]
                if tail.isdigit():
                    seq = int(tail)
                    max_seq_by_year[yr] = max(max_seq_by_year.get(yr, 0), seq)

        # Write invoices + children
        s_pg.commit()
        print(f"Migrated {migrated_count} invoices into Postgres.")

        # Bring over invoice_sequences so future numbers don't collide
        # We'll set last_seq to max(existing, migrated) for each year
        if max_seq_by_year:
            for yr, max_seq in max_seq_by_year.items():
                row = s_pg.query(InvoiceSequence).filter(InvoiceSequence.year == yr).first()
                if not row:
                    row = InvoiceSequence(year=yr, last_seq=max_seq)
                    s_pg.add(row)
                else:
                    row.last_seq = max(row.last_seq or 0, max_seq)
            s_pg.commit()
            print(f"Updated invoice_sequences for years: {sorted(max_seq_by_year.keys())}")

        print("Done.")


if __name__ == "__main__":
    main()

