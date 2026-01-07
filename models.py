# models.py
from __future__ import annotations

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
# Users
# -----------------------------
class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    username: Mapped[str] = mapped_column(String(80), unique=True, nullable=False, index=True)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)

    # ✅ Email for password reset
    email: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    # ✅ Invoice template / profession (user default for NEW invoices)
    # Allowed examples:
    #   - "auto_repair"
    #   - "general_service"
    #   - "accountant"
    invoice_template: Mapped[str] = mapped_column(String(50), nullable=False, default="auto_repair")

    # Profile / business info (for PDF header)
    business_name: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    phone: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    address: Mapped[Optional[str]] = mapped_column(String(300), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    invoices: Mapped[list["Invoice"]] = relationship(
        back_populates="user",
        order_by="Invoice.created_at.desc()",
    )


# -----------------------------
# Tables
# -----------------------------
class InvoiceSequence(Base):
    __tablename__ = "invoice_sequences"
    __table_args__ = (UniqueConstraint("year", name="uq_invoice_sequences_year"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    year: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    last_seq: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)


class Invoice(Base):
    __tablename__ = "invoices"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    user_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL"),
        index=True,
        nullable=True
    )

    invoice_number: Mapped[str] = mapped_column(String(32), unique=True, nullable=False, index=True)

    # ✅ Locks in the profession/template at the time the invoice was created
    # Same allowed examples as User.invoice_template.
    invoice_template: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

    # Customer contact
    customer_email: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    customer_phone: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

    # Core fields (labels change by template in UI/PDF)
    name: Mapped[str] = mapped_column(String(200), nullable=False, index=True)
    vehicle: Mapped[str] = mapped_column(String(200), nullable=False, index=True)  # label changes by template
    hours: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    price_per_hour: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    shop_supplies: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    notes: Mapped[str] = mapped_column(String, nullable=False, default="")
    paid: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    date_in: Mapped[str] = mapped_column(String(64), nullable=False, default="")

    pdf_path: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    pdf_generated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)

    user: Mapped[Optional["User"]] = relationship(back_populates="invoices")

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
    return create_engine(
        db_url,
        echo=echo,
        future=True,
        pool_pre_ping=True,
        pool_recycle=300,
        pool_size=5,
        max_overflow=10,
    )


def make_session_factory(engine):
    return sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)


# -----------------------------
# Invoice number generator
# -----------------------------
def next_invoice_number(session, year: int, seq_width: int = 6) -> str:
    seq_row = session.execute(
        select(InvoiceSequence).where(InvoiceSequence.year == year)
    ).scalar_one_or_none()

    if seq_row is None:
        seq_row = InvoiceSequence(year=year, last_seq=0)
        session.add(seq_row)
        session.flush()

    seq_row.last_seq += 1
    session.flush()

    return f"{year}{seq_row.last_seq:0{seq_width}d}"



