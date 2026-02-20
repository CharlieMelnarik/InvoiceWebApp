from __future__ import annotations

from datetime import datetime
from decimal import Decimal, ROUND_HALF_UP
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
    or_,
    LargeBinary,
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
    account_owner_id: Mapped[Optional[int]] = mapped_column(Integer, nullable=True, index=True)
    is_employee: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    # Email for password reset
    email: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)

    # Default invoice template for NEW invoices
    invoice_template: Mapped[str] = mapped_column(String(50), nullable=False, default="auto_repair")
    custom_profession_name: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    custom_job_label: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    custom_labor_title: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    custom_labor_desc_label: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    custom_parts_title: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    custom_parts_name_label: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    custom_shop_supplies_label: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    custom_show_job: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    custom_show_labor: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    custom_show_parts: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    custom_show_shop_supplies: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    custom_show_notes: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    invoice_builder_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    invoice_builder_accent_color: Mapped[str] = mapped_column(String(20), nullable=False, default="#0f172a")
    invoice_builder_header_style: Mapped[str] = mapped_column(String(20), nullable=False, default="classic")
    invoice_builder_compact_mode: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    # Default PDF layout template for NEW invoices/estimates
    pdf_template: Mapped[str] = mapped_column(String(50), nullable=False, default="classic")

    # Default tax rate percentage for NEW invoices/estimates
    tax_rate: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)

    # Default hourly rate and parts markup for NEW invoices/estimates
    default_hourly_rate: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    default_parts_markup: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    payment_fee_auto_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    payment_fee_percent: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    payment_fee_fixed: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    stripe_fee_percent: Mapped[float] = mapped_column(Float, nullable=False, default=2.9)
    stripe_fee_fixed: Mapped[float] = mapped_column(Float, nullable=False, default=0.30)

    # Profile / business info (for PDF header)
    business_name: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    phone: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    address: Mapped[Optional[str]] = mapped_column(String(300), nullable=True)
    address_line1: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    address_line2: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    city: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    state: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    postal_code: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)

    # Stripe billing fields
    stripe_customer_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    stripe_subscription_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    stripe_connect_account_id: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    stripe_connect_charges_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    stripe_connect_payouts_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    stripe_connect_details_submitted: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    stripe_connect_last_synced_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    subscription_status: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    subscription_tier: Mapped[str] = mapped_column(String(20), nullable=False, default="basic")
    trial_ends_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    current_period_end: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    # one-trial-per-user flag
    trial_used_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    trial_used_basic_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    trial_used_pro_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    # Security (failed login lockout)
    failed_login_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    password_reset_required: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    last_failed_login: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    # Logo for PDF header (relative path under instance/, e.g. "uploads/logos/user_1.png")
    logo_path: Mapped[Optional[str]] = mapped_column(String(300), nullable=True)
    # Resized logo image stored in DB (preferred over logo_path for persistence across deploys)
    logo_blob: Mapped[Optional[bytes]] = mapped_column(LargeBinary, nullable=True)
    logo_blob_mime: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

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

    payment_reminders_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    payment_due_days: Mapped[int] = mapped_column(Integer, nullable=False, default=30)
    payment_reminder_before_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    payment_reminder_due_today_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    payment_reminder_after_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    payment_reminder_days_before: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    payment_reminder_days_after: Mapped[int] = mapped_column(Integer, nullable=False, default=3)
    payment_reminder_last_run_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    late_fee_enabled: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    late_fee_mode: Mapped[str] = mapped_column(String(20), nullable=False, default="fixed")  # fixed|percent
    late_fee_fixed: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    late_fee_percent: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    late_fee_frequency_days: Mapped[int] = mapped_column(Integer, nullable=False, default=30)

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
        foreign_keys="Invoice.user_id",
        order_by="Invoice.created_at.desc()",
    )

    business_expenses: Mapped[list["BusinessExpense"]] = relationship(
        back_populates="user",
        cascade="all, delete-orphan",
        order_by="BusinessExpense.sort_order.asc(), BusinessExpense.id.asc()",
    )

    invoice_design_templates: Mapped[list["InvoiceDesignTemplate"]] = relationship(
        back_populates="user",
        cascade="all, delete-orphan",
        order_by="InvoiceDesignTemplate.updated_at.desc(), InvoiceDesignTemplate.id.desc()",
    )
    email_templates: Mapped[list["EmailTemplate"]] = relationship(
        back_populates="user",
        cascade="all, delete-orphan",
        order_by="EmailTemplate.template_key.asc()",
    )
    custom_profession_presets: Mapped[list["CustomProfessionPreset"]] = relationship(
        back_populates="user",
        cascade="all, delete-orphan",
        order_by="CustomProfessionPreset.name.asc(), CustomProfessionPreset.id.asc()",
    )

    schedule_events: Mapped[list["ScheduleEvent"]] = relationship(
        back_populates="user",
        foreign_keys="ScheduleEvent.user_id",
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
    created_by_user_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    customer_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("customers.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    invoice_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("invoices.id", ondelete="SET NULL"),
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

    user: Mapped["User"] = relationship(
        back_populates="schedule_events",
        foreign_keys=[user_id],
    )
    customer: Mapped[Optional["Customer"]] = relationship(back_populates="schedule_events")
    invoice: Mapped[Optional["Invoice"]] = relationship()


class CustomProfessionPreset(Base):
    __tablename__ = "custom_profession_presets"
    __table_args__ = (
        UniqueConstraint("user_id", "name", name="uq_custom_profession_presets_user_name"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    job_label: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    labor_title: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    labor_desc_label: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    parts_title: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    parts_name_label: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    shop_supplies_label: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    show_job: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    show_labor: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    show_parts: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    show_shop_supplies: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    show_notes: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    user: Mapped["User"] = relationship(back_populates="custom_profession_presets")


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
    address_line1: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    address_line2: Mapped[Optional[str]] = mapped_column(String(200), nullable=True)
    city: Mapped[Optional[str]] = mapped_column(String(100), nullable=True)
    state: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    postal_code: Mapped[Optional[str]] = mapped_column(String(20), nullable=True)

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
# Invoice display sequence (per user + type)
# -----------------------------
class InvoiceDisplaySequence(Base):
    __tablename__ = "invoice_display_sequences"
    __table_args__ = (
        UniqueConstraint("user_id", "year", "doc_type", name="uq_invoice_display_seq_user_year_type"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    year: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    doc_type: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
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
    created_by_user_id: Mapped[Optional[int]] = mapped_column(
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
    display_number: Mapped[Optional[str]] = mapped_column(String(32), nullable=True, index=True)

    invoice_template: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    pdf_template: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    tax_rate: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    tax_override: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

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
    converted_from_estimate: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    converted_to_invoice: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    paid: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    paid_processing_fee: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    date_in: Mapped[str] = mapped_column(String(64), nullable=False, default="")
    is_estimate: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)

    pdf_path: Mapped[Optional[str]] = mapped_column(String, nullable=True)
    pdf_generated_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    payment_reminder_before_sent_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    payment_reminder_due_today_sent_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    payment_reminder_after_sent_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    payment_reminder_last_sent_at: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    user: Mapped[Optional["User"]] = relationship(
        back_populates="invoices",
        foreign_keys=[user_id],
    )
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

    @staticmethod
    def _money(value: float | int | str | Decimal) -> Decimal:
        return Decimal(str(value or 0.0)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)

    @staticmethod
    def _dec(value: float | int | str | Decimal) -> Decimal:
        return Decimal(str(value or 0.0))

    def parts_total_raw(self) -> float:
        total = sum(self._dec(p.part_price) for p in self.parts)
        return float(self._money(total))

    def parts_markup_amount(self) -> float:
        markup_percent = self.parts_markup_percent or 0.0
        if not markup_percent:
            return 0.0
        total_with_markup = self.parts_total()
        return float(self._money(self._dec(total_with_markup) - self._dec(self.parts_total_raw())))

    def parts_total(self) -> float:
        markup_percent = self.parts_markup_percent or 0.0
        if not markup_percent:
            return self.parts_total_raw()
        multiplier = self._dec(1) + (self._dec(markup_percent) / self._dec(100))
        line_totals = [self._money(self._dec(p.part_price) * multiplier) for p in self.parts]
        total = sum(line_totals, self._dec(0))
        return float(self._money(total))

    def part_price_with_markup(self, price: float) -> float:
        markup_percent = self.parts_markup_percent or 0.0
        if not markup_percent:
            return float(self._money(price))
        multiplier = self._dec(1) + (self._dec(markup_percent) / self._dec(100))
        return float(self._money(self._dec(price) * multiplier))

    def labor_total(self) -> float:
        total = self._dec(self.hours) * self._dec(self.price_per_hour)
        return float(self._money(total))

    def subtotal_before_tax(self) -> float:
        total = self._dec(self.parts_total()) + self._dec(self.labor_total()) + self._dec(self.shop_supplies)
        return float(self._money(total))

    def tax_amount(self) -> float:
        if self.tax_override is not None:
            return float(self._money(self._dec(self.tax_override)))
        rate = self._dec(self.tax_rate or 0.0) / self._dec(100)
        total = self._dec(self.subtotal_before_tax()) * rate
        return float(self._money(total))

    def invoice_total(self) -> float:
        total = self._dec(self.subtotal_before_tax()) + self._dec(self.tax_amount())
        return float(self._money(total))

    def amount_due(self) -> float:
        total = self._dec(self.invoice_total()) - self._dec(self.paid)
        return float(self._money(total))


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


class InvoiceDesignTemplate(Base):
    __tablename__ = "invoice_design_templates"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    name: Mapped[str] = mapped_column(String(120), nullable=False, default="My Template")
    design_json: Mapped[str] = mapped_column(String, nullable=False, default="{}")
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    user: Mapped["User"] = relationship(back_populates="invoice_design_templates")


class EmailTemplate(Base):
    __tablename__ = "email_templates"
    __table_args__ = (
        UniqueConstraint("user_id", "template_key", name="uq_email_templates_user_key"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    template_key: Mapped[str] = mapped_column(String(64), nullable=False, index=True)
    name: Mapped[str] = mapped_column(String(120), nullable=False)
    subject: Mapped[str] = mapped_column(String(255), nullable=False)
    html_body: Mapped[str] = mapped_column(String(20000), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    user: Mapped["User"] = relationship(back_populates="email_templates")


class BusinessExpense(Base):
    __tablename__ = "business_expenses"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    label: Mapped[str] = mapped_column(String(120), nullable=False, default="")
    amount: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    is_custom: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    sort_order: Mapped[int] = mapped_column(Integer, nullable=False, default=0)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)
    updated_at: Mapped[datetime] = mapped_column(
        DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow
    )

    user: Mapped["User"] = relationship(back_populates="business_expenses")
    entries: Mapped[list["BusinessExpenseEntry"]] = relationship(
        back_populates="expense",
        cascade="all, delete-orphan",
        order_by="BusinessExpenseEntry.created_at.desc(), BusinessExpenseEntry.id.desc()",
    )


class BusinessExpenseEntry(Base):
    __tablename__ = "business_expense_entries"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    expense_id: Mapped[int] = mapped_column(
        ForeignKey("business_expenses.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    user_id: Mapped[int] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    item_desc: Mapped[str] = mapped_column(String(200), nullable=False, default="")
    amount: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    expense: Mapped["BusinessExpense"] = relationship(back_populates="entries")
    split_items: Mapped[list["BusinessExpenseEntrySplit"]] = relationship(
        back_populates="entry",
        cascade="all, delete-orphan",
        order_by="BusinessExpenseEntrySplit.created_at.desc(), BusinessExpenseEntrySplit.id.desc()",
    )


class BusinessExpenseEntrySplit(Base):
    __tablename__ = "business_expense_entry_splits"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    entry_id: Mapped[int] = mapped_column(
        ForeignKey("business_expense_entries.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    user_id: Mapped[int] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    item_desc: Mapped[str] = mapped_column(String(200), nullable=False, default="")
    amount: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow)

    entry: Mapped["BusinessExpenseEntry"] = relationship(back_populates="split_items")


class AuditLog(Base):
    __tablename__ = "audit_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    user_id: Mapped[Optional[int]] = mapped_column(
        ForeignKey("users.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    event: Mapped[str] = mapped_column(String(80), nullable=False, index=True)
    result: Mapped[str] = mapped_column(String(20), nullable=False, index=True)
    method: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    path: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    ip_address: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    user_agent: Mapped[Optional[str]] = mapped_column(String(300), nullable=True)
    username: Mapped[Optional[str]] = mapped_column(String(120), nullable=True)
    email: Mapped[Optional[str]] = mapped_column(String(255), nullable=True)
    details: Mapped[Optional[str]] = mapped_column(String(1000), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, nullable=False, default=datetime.utcnow, index=True)


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


def next_display_number(
    session,
    user_id: int,
    year: int,
    doc_type: str,
    seq_width: int = 6,
) -> str:
    seq_row = session.execute(
        select(InvoiceDisplaySequence).where(
            InvoiceDisplaySequence.user_id == user_id,
            InvoiceDisplaySequence.year == year,
            InvoiceDisplaySequence.doc_type == doc_type,
        )
    ).scalar_one_or_none()

    if seq_row is None:
        year_prefix = str(year)
        is_estimate = (doc_type == "estimate")
        inv_rows = session.execute(
            select(Invoice.display_number, Invoice.invoice_number).where(
                Invoice.user_id == user_id,
                (Invoice.is_estimate.is_(True) if is_estimate else or_(Invoice.is_estimate.is_(False), Invoice.is_estimate.is_(None))),
            )
        ).all()

        max_seq = 0
        for display_no, inv_no in inv_rows:
            candidate = display_no or inv_no or ""
            if not candidate.startswith(year_prefix):
                continue
            suffix = candidate[len(year_prefix):]
            if suffix.isdigit():
                max_seq = max(max_seq, int(suffix))

        seq_row = InvoiceDisplaySequence(
            user_id=user_id,
            year=year,
            doc_type=doc_type,
            last_seq=max_seq,
        )
        session.add(seq_row)
        session.flush()

    seq_row.last_seq += 1
    session.flush()

    return f"{year}{seq_row.last_seq:0{seq_width}d}"
