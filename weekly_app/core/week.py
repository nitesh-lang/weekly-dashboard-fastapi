from datetime import date, timedelta
from typing import Tuple


def get_week_range(d: date | None = None) -> Tuple[date, date]:
    d = d or date.today()
    days_since_saturday = (d.weekday() - 5) % 7
    week_start = d - timedelta(days=days_since_saturday)
    week_end = week_start + timedelta(days=6)
    return week_start, week_end


def get_current_week() -> dict:
    start, end = get_week_range()
    return {
        "week_start": start,
        "week_end": end,
        "label": f"{start} to {end}",
    }
