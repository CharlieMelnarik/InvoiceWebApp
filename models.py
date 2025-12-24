# models.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from sqlalchemy import (
    create_engine,
    String,
    Integer,
    Float,
    DateTime,
    ForeignKey,
    UniqueConstraint,
    select,
)
from sqlalchemy.orm import (
    DeclarativeBase,
    Mapped,
    mapped_column,
    relationship,
    sessionmaker,
)

# -----------------------------
# SQLAlchemy base
# -----------------------------
class Base(DeclarativeBase):
    pass


# -----------------------------
# Tables
# -----------------------------
class InvoiceSequence(Base):
    """
    Stores the last used sequence number per year.
    Used to generate invoice_number like: YYYY###### (no dash).
    """
    __tablename__ = "invoice_sequences"
    __table_args__ = (UniqueConstraint("year", name="uq_invoice_sequences_year"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    year: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    last_seq: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)


class Invoice(Base):
    """
    Mirrors your CSV fields (plus invoice_number and pdf storage metadata).
    """
    __tablename__ = "invoices"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    # Human-friendly invoice number: YYYY###### (no dash)
    invoice_number: Mapped[str] = mapped_column(String(32), unique=True, nullable=False, index=True)

    # CSV-equivalent fields
    name: Mapped[str] = mapped_column(String(200), nullable=False, index=True)            # Name
    vehicle: Mapped[str] = mapped_column(String(200), nullable=False, index=True)         # Vehicle
    hours: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)              # Hours
    price_per_hour: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)     # Price Per Hour
    shop_supplies: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)      # Shop Supplies
    notes: Mapped[str] = mapped_column(String, nullable=False, default="")                # Notes (we'll store as plain text; UI can show multi-line)
    paid: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)               # Paid
    date_in: Mapped[str] = mapped_column(String(64), nullable=False, default="")          # Date In (text)

    # Stored PDF (Option A: file path on disk)
    pdf_path: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    pdf_generated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    parts: Mapped[list["InvoicePart"]] = relationship(
        back_populates="invoice",
        cascade="all, delete-orphan",
        order_by="InvoicePart.id",
    )
    labor_items: Mapped[list["InvoiceLabor"]] = relationship(
        back_populates="invoice",
        cascade="all, delete-orphan",
        order_by="InvoiceLabor.id",
    )

    # Convenience totals (computed, not stored)
    def parts_total(self) -> float:
        return round(sum((p.part_price or 0.0) for p in self.parts), 2)

    def labor_total(self) -> float:
        return round((self.hours or 0.0) * (self.price_per_hour or 0.0), 2)

    def invoice_total(self) -> float:
        return round(self.parts_total() + self.labor_total() + (self.shop_supplies or 0.0), 2)

    def amount_due(self) -> float:
        return round(self.invoice_total() - (self.paid or 0.0), 2)


class InvoicePart(Base):
    __tablename__ = "invoice_parts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    invoice_id: Mapped[int] = mapped_column(ForeignKey("invoices.id", ondelete="CASCADE"), nullable=False, index=True)

    part_name: Mapped[str] = mapped_column(String(300), nullable=False, default="")
    part_price: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)

    invoice: Mapped["Invoice"] = relationship(back_populates="parts")


class InvoiceLabor(Base):
    __tablename__ = "invoice_labor"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    invoice_id: Mapped[int] = mapped_column(ForeignKey("invoices.id", ondelete="CASCADE"), nullable=False, index=True)

    labor_desc: Mapped[str] = mapped_column(String(500), nullable=False, default="")
    labor_time_hours: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)

    invoice: Mapped["Invoice"] = relationship(back_populates="labor_items")


# -----------------------------
# Engine / Session factory
# -----------------------------
def make_engine(db_url: str, echo: bool = False):
    """
    Create SQLAlchemy engine.
    Note: SQLite path must exist (instance/ folder). We'll create it in setup steps.
    """
    return create_engine(db_url, echo=echo, future=True)


def make_session_factory(engine):
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


# -----------------------------
# Invoice number generator
# -----------------------------
def next_invoice_number(session, year: int, seq_width: int = 6) -> str:
    """
    Returns next invoice number like YYYY###### (no dash).
    Uses a per-year counter in invoice_sequences.

    In Postgres this is safe under concurrency when run inside a transaction.
    In SQLite, writes are serialized, so it's also effectively safe.
    """
    # Try fetch existing counter
    seq_row = session.execute(
        select(InvoiceSequence).where(InvoiceSequence.year == year)
    ).scalar_one_or_none()

    if seq_row is None:
        seq_row = InvoiceSequence(year=year, last_seq=0)
        session.add(seq_row)
        session.flush()  # ensure it has an id

    seq_row.last_seq += 1
    session.flush()

    return f"{year}{seq_row.last_seq:0{seq_width}d}"

