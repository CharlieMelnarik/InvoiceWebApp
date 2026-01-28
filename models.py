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
    Boolean,
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

    # Email for password reset
    email: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    # Default invoice template for NEW invoices
    invoice_template: Mapped[str] = mapped_column(String(50), nullable=False, default="auto_repair")

    # Profile / business info (for PDF header)
    business_name: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    phone: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    address: Mapped[Optional[str]] = mapped_column(String(300), nullable=True)

    # Stripe billing fields
    stripe_customer_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    stripe_subscription_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    subscription_status: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    trial_ends_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    current_period_end: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    # one-trial-per-user flag
    trial_used_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    # Security (failed login lockout)
    failed_login_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    password_reset_required: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    last_failed_login: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    # Logo for PDF header (relative path under instance/, e.g. "uploads/logos/user_1.png")
    logo_path: Mapped[Optional[str]] = mapped_column(String(300), nullable=True)

    schedule_summary_frequency: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default="none",
    )  # none|day|week|month
    schedule_summary_time: Mapped[Optional[str]] = mapped_column(String(5), nullable=True)  # HH:MM
    schedule_summary_last_sent: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    schedule_summary_tz_offset_minutes: Mapped[int] = mapped_column(
        Integer,
        nullable=False,
        default=0,
    )

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    customers: Mapped[list["Customer"]] = relationship(
        back_populates="user",
        cascade="all, delete-orphan",
        order_by="Customer.name.asc()",
    )

    invoices: Mapped[list["Invoice"]] = relationship(
        back_populates="user",
        order_by="Invoice.created_at.desc()",
    )

    schedule_events: Mapped[list["ScheduleEvent"]] = relationship(
        back_populates="user",
        cascade="all, delete-orphan",
        order_by="ScheduleEvent.start_dt.asc()",
    )


# -----------------------------
# Schedule / Appointments
# -----------------------------
class ScheduleEvent(Base):
    __tablename__ = "schedule_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    user_id: Mapped[int] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    customer_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("customers.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    title: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    notes: Mapped[Optional[str]] = mapped_column(String(1000), nullable=True)

    start_dt: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)
    end_dt: Mapped[datetime] = mapped_column(DateTime, nullable=False, index=True)

    status: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default="scheduled",
    )  # scheduled|completed|cancelled

    event_type: Mapped[str] = mapped_column(
        String(20),
        nullable=False,
        default="appointment",
    )  # appointment|block

    # ✅ recurring bookkeeping
    is_auto: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    recurring_token: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    user: Mapped["User"] = relationship(back_populates="schedule_events")
    customer: Mapped[Optional["Customer"]] = relationship(back_populates="schedule_events")


# -----------------------------
# Customers (Option B)
# -----------------------------
class Customer(Base):
    __tablename__ = "customers"
    __table_args__ = (
        UniqueConstraint("user_id", "name", name="uq_customers_user_id_name"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    user_id: Mapped[int] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # Recurring service fields
    next_service_dt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True, index=True)
    service_interval_days: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    default_service_minutes: Mapped[int] = mapped_column(Integer, nullable=False, default=60)
    service_title: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    service_notes: Mapped[Optional[str]] = mapped_column(String(1000), nullable=True)
    recurring_horizon_dt: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    # Name is mandatory
    name: Mapped[str] = mapped_column(String(200), nullable=False, index=True)

    # Optional fields
    email: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    phone: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    address: Mapped[Optional[str]] = mapped_column(String(300), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    user: Mapped["User"] = relationship(back_populates="customers")

    invoices: Mapped[list["Invoice"]] = relationship(
        back_populates="customer",
        order_by="Invoice.created_at.desc()",
    )

    # NOTE: no delete-orphan here because ScheduleEvent.customer_id is SET NULL.
    schedule_events: Mapped[list["ScheduleEvent"]] = relationship(
        back_populates="customer",
        order_by="ScheduleEvent.start_dt.asc()",
    )


# -----------------------------
# Invoice sequence
# -----------------------------
class InvoiceSequence(Base):
    __tablename__ = "invoice_sequences"
    __table_args__ = (UniqueConstraint("year", name="uq_invoice_sequences_year"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    year: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    last_seq: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )


# -----------------------------
# Invoices
# -----------------------------
class Invoice(Base):
    __tablename__ = "invoices"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    user_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL"),
        index=True,
        nullable=True,
    )

    customer_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("customers.id", ondelete="SET NULL"),
        index=True,
        nullable=True,
    )

    invoice_number: Mapped[str] = mapped_column(String(32), unique=True, nullable=False, index=True)

    invoice_template: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

    customer_email: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    customer_phone: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

    name: Mapped[str] = mapped_column(String(200), nullable=False, index=True)
    vehicle: Mapped[str] = mapped_column(String(200), nullable=False, index=True)

    hours: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    price_per_hour: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    shop_supplies: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    parts_markup_percent: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)

    notes: Mapped[str] = mapped_column(String, nullable=False, default="")
    useful_info: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    paid: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    date_in: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    is_estimate: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    pdf_path: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    pdf_generated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    user: Mapped[Optional["User"]] = relationship(back_populates="invoices")
    customer: Mapped[Optional["Customer"]] = relationship(back_populates="invoices")

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

    def parts_total_raw(self) -> float:
        return round(sum((p.part_price or 0.0) for p in self.parts), 2)

    def parts_markup_amount(self) -> float:
        markup_percent = self.parts_markup_percent or 0.0
        if not markup_percent:
            return 0.0
        return round(self.parts_total_raw() * (markup_percent / 100.0), 2)

    def parts_total(self) -> float:
        return round(self.parts_total_raw() + self.parts_markup_amount(), 2)

    def part_price_with_markup(self, price: float) -> float:
        markup_percent = self.parts_markup_percent or 0.0
        if not markup_percent:
            return round(price or 0.0, 2)
        return round((price or 0.0) * (1 + markup_percent / 100.0), 2)

    def labor_total(self) -> float:
        return round((self.hours or 0.0) * (self.price_per_hour or 0.0), 2)

    def invoice_total(self) -> float:
        return round(self.parts_total() + self.labor_total() + (self.shop_supplies or 0.0), 2)

    def amount_due(self) -> float:
        return round(self.invoice_total() - (self.paid or 0.0), 2)


class InvoicePart(Base):
    __tablename__ = "invoice_parts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    invoice_id: Mapped[int] = mapped_column(
        ForeignKey("invoices.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    part_name: Mapped[str] = mapped_column(String(300), nullable=False, default="")
    part_price: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)

    invoice: Mapped["Invoice"] = relationship(back_populates="parts")


class InvoiceLabor(Base):
    __tablename__ = "invoice_labor"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    invoice_id: Mapped[int] = mapped_column(
        ForeignKey("invoices.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

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
