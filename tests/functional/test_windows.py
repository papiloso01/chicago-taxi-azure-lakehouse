from datetime import date, datetime, timezone
import pytest
from chicago_taxi.ingestion.windows import daily_window, year_window, weather_window


def test_daily_uses_chicago_date_not_utc_date():
    assert daily_window(datetime(2026, 1, 2, 2, tzinfo=timezone.utc)) == ("2025-12-31", "2026-01-01")


def test_leap_day_window():
    assert daily_window(datetime(2024, 3, 1, 12, tzinfo=timezone.utc)) == ("2024-02-29", "2024-03-01")


def test_naive_clock_rejected():
    with pytest.raises(ValueError, match="timezone"):
        daily_window(datetime(2026, 1, 1))


def test_completed_year_end_is_exclusive():
    assert year_window(2025, date(2026, 10, 14)) == ("2025-01-01", "2026-01-01")


@pytest.mark.parametrize("year", [0, 2026, 2027])
def test_incomplete_or_invalid_year_rejected(year):
    with pytest.raises(ValueError):
        year_window(year, date(2026, 10, 14))


def test_weather_is_independent_delayed_overlap():
    assert weather_window(date(2026, 10, 14)) == ("2026-09-25", "2026-10-09")
