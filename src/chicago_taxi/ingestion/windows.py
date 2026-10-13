"""Chicago-local inclusive-start/exclusive-end loading windows."""
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

CHICAGO = ZoneInfo("America/Chicago")


def daily_window(now=None):
    now = now or datetime.now(CHICAGO)
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("now must be timezone-aware")
    end = now.astimezone(CHICAGO).date()
    return (end - timedelta(days=1)).isoformat(), end.isoformat()


def year_window(year, today=None):
    today = today or datetime.now(CHICAGO).date()
    if not 1 <= year < today.year:
        raise ValueError("Choose a completed calendar year")
    return date(year, 1, 1).isoformat(), date(year + 1, 1, 1).isoformat()


def weather_window(today=None):
    today = today or datetime.now(CHICAGO).date()
    end = today - timedelta(days=5)
    return (end - timedelta(days=14)).isoformat(), end.isoformat()
