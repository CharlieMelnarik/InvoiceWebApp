from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy import or_

from app import (
    create_app,
    _normalize_email,
    _looks_like_email,
    _should_send_summary,
    _summary_window,
    _send_schedule_summary_email,
)
from config import Config
from models import Customer, ScheduleEvent, User, make_engine, make_session_factory


def _format_event_line(event: ScheduleEvent, customer: Customer | None) -> str:
    title = (event.title or "").strip() or (customer.name if customer else "Appointment")
    if customer and customer.name and title.lower() != customer.name.lower():
        label = f"{title} - {customer.name}"
    else:
        label = title

    start_label = event.start_dt.strftime("%b %d, %Y %I:%M %p").lstrip("0")
    end_label = event.end_dt.strftime("%b %d, %Y %I:%M %p").lstrip("0")
    return f"- {start_label} → {end_label}: {label}"


def _format_offset_label(offset_minutes: int) -> str:
    sign = "+" if offset_minutes >= 0 else "-"
    abs_val = abs(offset_minutes)
    hh = abs_val // 60
    mm = abs_val % 60
    return f"UTC{sign}{hh:02d}:{mm:02d}"


def main() -> None:
    app = create_app()
    engine = make_engine(Config.SQLALCHEMY_DATABASE_URI, echo=Config.SQLALCHEMY_ECHO)
    SessionLocal = make_session_factory(engine)

    now = datetime.utcnow()

    with app.app_context():
        with SessionLocal() as s:
            users = (
                s.query(User)
                .filter(User.schedule_summary_frequency.isnot(None))
                .all()
            )

            for user in users:
                freq = (user.schedule_summary_frequency or "none").lower().strip()
                if freq == "none":
                    continue
                if not _should_send_summary(user, now):
                    continue

                to_email = _normalize_email(user.email or "")
                if not _looks_like_email(to_email):
                    continue

                offset_minutes = int(user.schedule_summary_tz_offset_minutes or 0)
                now_local = now + timedelta(minutes=offset_minutes)
                start_time = user.schedule_summary_time or "00:00"
                start, end = _summary_window(now_local, freq, start_time)
                events = (
                    s.query(ScheduleEvent)
                    .filter(ScheduleEvent.user_id == user.id)
                    .filter(ScheduleEvent.status == "scheduled")
                    .filter(or_(ScheduleEvent.event_type.is_(None), ScheduleEvent.event_type != "block"))
                    .filter(ScheduleEvent.start_dt < end)
                    .filter(ScheduleEvent.end_dt > start)
                    .order_by(ScheduleEvent.start_dt.asc())
                    .all()
                )

                if not events:
                    continue

                lines = []
                for event in events:
                    customer = s.get(Customer, event.customer_id) if event.customer_id else None
                    lines.append(_format_event_line(event, customer))

                end_display = end - timedelta(seconds=1)
                tz_label = _format_offset_label(offset_minutes)
                subject = f"Upcoming appointments ({freq})"
                body = (
                    f"Here is your upcoming appointment summary (local time, {tz_label}):\n"
                    f"{start:%b %d, %Y %I:%M %p} through {end_display:%b %d, %Y %I:%M %p}\n\n"
                    + "\n".join(lines)
                )

                try:
                    _send_schedule_summary_email(to_email, subject, body)
                except Exception as exc:
                    print(f"[SCHEDULE SUMMARY] Email failed for user={user.id}: {exc!r}", flush=True)
                    continue

                user.schedule_summary_last_sent = now
                s.commit()


if __name__ == "__main__":
    main()
