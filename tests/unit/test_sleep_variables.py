"""Unit tests for the sleep_variables module."""

import datetime

import polars as pl
import pytest

from noctsleepy.processing import sleep_variables


@pytest.fixture
def create_dummy_data() -> pl.DataFrame:
    """Create a 1-day of dummy Polars DataFrame for testing."""
    dummy_date = datetime.datetime(year=2024, month=5, day=2, hour=10, minute=0)
    dummy_datetime_list = [
        dummy_date + datetime.timedelta(minutes=i) for i in range(1440)
    ]
    return pl.DataFrame(
        {
            "time": dummy_datetime_list,
            "sleep_status": [True] * 1440,
            "sib_periods": [True] * 1440,
            "spt_periods": [True] * 1440,
            "nonwear_status": [False] * 1440,
        }
    )


def test_filter_nights_cross_midnight(create_dummy_data: pl.DataFrame) -> None:
    """Test finding valid nights in the dummy data."""
    night_start = datetime.time(hour=20, minute=0)
    night_end = datetime.time(hour=8, minute=0)
    nw_threshold = 0.2

    valid_nights = sleep_variables._filter_nights(
        create_dummy_data, night_start, night_end, nw_threshold
    )
    time_check = (
        (valid_nights["time"].dt.time() >= night_start)
        | (valid_nights["time"].dt.time() <= night_end)
    ).all()

    assert valid_nights["night_date"].unique().len() == 1, (
        f"Expected 1 valid night, got {valid_nights['night_date'].unique().len()}"
    )
    assert time_check, "Not all timestamps are within the nocturnal interval"


def test_filter_nights_before_midnight(create_dummy_data: pl.DataFrame) -> None:
    """Test finding valid nights in the dummy data."""
    night_start = datetime.time(hour=20, minute=0)
    night_end = datetime.time(hour=23, minute=0)
    nw_threshold = 0.2

    valid_nights = sleep_variables._filter_nights(
        create_dummy_data, night_start, night_end, nw_threshold
    )
    time_check = (
        (valid_nights["time"].dt.time() >= night_start)
        & (valid_nights["time"].dt.time() <= night_end)
    ).all()

    assert valid_nights["night_date"].unique().len() == 1, (
        f"Expected 1 valid night, got {valid_nights['night_date'].unique().len()}"
    )
    assert time_check, "Not all timestamps are within the nocturnal interval"


def test_sleepmetrics_class() -> None:
    """Test the SleepMetrics dataclass."""
    metrics = sleep_variables.SleepMetrics(
        sleep_duration=(8.2, 7.8),
        waso_30=3.2,
        weekday_midpoint=(datetime.time(2, 30), datetime.time(3, 0)),
    )

    assert isinstance(metrics, sleep_variables.SleepMetrics)
    assert metrics.time_in_bed is None, "time_in_bed should be None by default"
    assert metrics.sleep_duration == [8.2, 7.8], "sleep_duration should match input"
    assert metrics.waso_30 == 3.2, "waso_30 should be 3.2"
    assert metrics.weekday_midpoint == [datetime.time(2, 30), datetime.time(3, 0)], (
        "First weekday_midpoint should be 02:30, second should be 03:00"
    )
