from __future__ import annotations

from datetime import datetime, timedelta

from sqlalchemy import or_

from app import (
    create_app,
    _normalize_email,
    _looks_like_email,
    _should_send_summary,
    _summary_window_for_user,
    _send_schedule_summary_email,
    _format_event_line,
    _run_automatic_payment_reminders,
)
from config import Config
from models import Customer, ScheduleEvent, User, make_engine, make_session_factory


def main() -> None:
    app = create_app()
    engine = make_engine(Config.SQLALCHEMY_DATABASE_URI, echo=Config.SQLALCHEMY_ECHO)
    SessionLocal = make_session_factory(engine)

    now = datetime.utcnow()

    with app.app_context():
        with SessionLocal() as s:
            print("[PAYMENT REMINDER] cron run start", flush=True)
            reminder_users = s.query(User).all()
            print(f"[PAYMENT REMINDER] cron user_count={len(reminder_users)}", flush=True)
            for user in reminder_users:
                try:
                    print(f"[PAYMENT REMINDER] cron checking user={user.id}", flush=True)
                    _run_automatic_payment_reminders(s, user)
                    s.commit()
                except Exception as exc:
                    print(
                        f"[PAYMENT REMINDER] automatic run failed for user={user.id}: {exc!r}",
                        flush=True,
                    )
                    s.rollback()
            print("[PAYMENT REMINDER] cron run complete", flush=True)

            users = (
                s.query(User)
                .filter(User.schedule_summary_frequency.isnot(None))
                .all()
            )

            for user in users:
                freq = (user.schedule_summary_frequency or "none").lower().strip()
                if freq == "none":
                    print(f"[SCHEDULE SUMMARY] user={user.id} skipped (frequency=none)", flush=True)
                    continue
                if not _should_send_summary(user, now):
                    print(f"[SCHEDULE SUMMARY] user={user.id} skipped (not time yet)", flush=True)
                    continue

                to_email = _normalize_email(user.email or "")
                if not _looks_like_email(to_email):
                    print(f"[SCHEDULE SUMMARY] user={user.id} skipped (invalid email)", flush=True)
                    continue

                start, end, tz_label, _now_local = _summary_window_for_user(user, now)
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
                    print(f"[SCHEDULE SUMMARY] user={user.id} skipped (no events)", flush=True)
                    continue

                lines = []
                for event in events:
                    customer = s.get(Customer, event.customer_id) if event.customer_id else None
                    lines.append(_format_event_line(event, customer))

                end_display = end - timedelta(seconds=1)
                subject = f"Upcoming appointments ({freq})"
                body = (
                    f"Here is your upcoming appointment summary (local time, {tz_label}):\n"
                    f"{start:%b %d, %Y %I:%M %p} through {end_display:%b %d, %Y %I:%M %p}\n\n"
                    + "\n".join(lines)
                )

                try:
                    print(
                        f"[SCHEDULE SUMMARY] user={user.id} sending {len(events)} event(s) to {to_email}",
                        flush=True,
                    )
                    _send_schedule_summary_email(to_email, subject, body)
                    print(f"[SCHEDULE SUMMARY] user={user.id} email sent", flush=True)
                except Exception as exc:
                    print(f"[SCHEDULE SUMMARY] Email failed for user={user.id}: {exc!r}", flush=True)
                    continue

                user.schedule_summary_last_sent = now
                s.commit()


if __name__ == "__main__":
    main()
