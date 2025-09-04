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


def test_sleepmetrics_class(create_dummy_data: pl.DataFrame) -> None:
    """Test the SleepMetrics dataclass."""
    metrics = sleep_variables.SleepMetrics(create_dummy_data)

    assert isinstance(metrics, sleep_variables.SleepMetrics)
    assert metrics._time_in_bed is None, "time_in_bed should be None by default"


def test_sleepmetrics_no_valid_nights() -> None:
    """Test the SleepMetrics dataclass raises ValueError when no valid nights."""
    dummy_date = datetime.datetime(year=2024, month=5, day=2, hour=10, minute=0)
    dummy_datetime_list = [
        dummy_date + datetime.timedelta(minutes=i) for i in range(100)
    ]
    bad_data = pl.DataFrame(
        {
            "time": dummy_datetime_list,
            "sib_periods": [True] * 100,
            "spt_periods": [True] * 100,
            "nonwear_status": [False] * 100,
        }
    )

    with pytest.raises(ValueError, match="No valid nights found in the data."):
        sleep_variables.SleepMetrics(bad_data)


@pytest.mark.parametrize(
    "selected_metrics, expected_values",
    [
        ("sleep_duration", [720]),
        ("time_in_bed", [720]),
        ("sleep_efficiency", [100.0]),
        ("waso", [0.0]),
    ],
)
def test_sleep_metrics_attributes(
    create_dummy_data: pl.DataFrame, selected_metrics: str, expected_values: pl.Series
) -> None:
    """Test the SleepMetrics attributes."""
    metrics = sleep_variables.SleepMetrics(create_dummy_data)

    result = getattr(metrics, selected_metrics)

    assert result.to_list() == expected_values, (
        f"Expected {expected_values.to_list()}, got {result.to_list()}"
    )


def test_sleep_onset(create_dummy_data: pl.DataFrame) -> None:
    """Test the sleep_onset method."""
    metrics = sleep_variables.SleepMetrics(create_dummy_data)
    expected_onset = datetime.time(hour=20, minute=0)

    assert metrics.sleep_onset[0] == expected_onset, (
        f"Expected onset {expected_onset}, got {metrics.sleep_onset[0]}"
    )


def test_sleep_wakeup(create_dummy_data: pl.DataFrame) -> None:
    """Test the sleep_wakeup method."""
    metrics = sleep_variables.SleepMetrics(create_dummy_data)
    expected_wakeup = datetime.time(hour=7, minute=59)

    assert metrics.sleep_wakeup[0] == expected_wakeup, (
        f"Expected wakeup {expected_wakeup}, got {metrics.sleep_wakeup[0]}"
    )


def test_get_night_midpoint() -> None:
    """Test the get_night_midpoint method."""
    sleep_onset = datetime.time(hour=22, minute=0)
    sleep_wakeup = datetime.time(hour=6, minute=10)
    expected_midpoint = datetime.time(hour=2, minute=5)

    midpoint = sleep_variables._get_night_midpoint(sleep_onset, sleep_wakeup)

    assert midpoint == expected_midpoint, (
        f"Expected midpoint {expected_midpoint}, got {midpoint}"
    )


def test_num_awakenings() -> None:
    """Test the num_awakenings attribute."""
    dummy_date = datetime.datetime(year=2024, month=5, day=2, hour=10, minute=0)
    dummy_datetime_list = [
        dummy_date + datetime.timedelta(minutes=i) for i in range(1440)
    ]
    data_with_awakenings = pl.DataFrame(
        {
            "time": dummy_datetime_list,
            "sib_periods": [True] * 800
            + [False] * 100
            + [True] * 100
            + [False] * 100
            + [True] * 340,
            "spt_periods": [True] * 1440,
            "nonwear_status": [False] * 1440,
        }
    )
    metrics = sleep_variables.SleepMetrics(data_with_awakenings)

    assert metrics.num_awakenings.to_list() == [2], (
        f"Expected 2 awakenings, got {metrics.num_awakenings.to_list()}"
    )


def test_num_awakenings_zero(create_dummy_data: pl.DataFrame) -> None:
    """Test the num_awakenings attribute when there are no awakenings."""
    metrics = sleep_variables.SleepMetrics(create_dummy_data)

    assert metrics.num_awakenings.to_list() == [0], (
        f"Expected 0 awakenings, got {metrics.num_awakenings.to_list()}"
    )


def test_waso_30() -> None:
    """Test the waso_30 attribute."""
    dummy_date = datetime.datetime(year=2024, month=5, day=2, hour=10, minute=0)
    dummy_datetime_list = [
        dummy_date + datetime.timedelta(minutes=i) for i in range(1440)
    ]
    data_with_awakenings = pl.DataFrame(
        {
            "time": dummy_datetime_list,
            "sib_periods": [True] * 800
            + [False] * 100
            + [True] * 100
            + [False] * 100
            + [True] * 340,
            "spt_periods": [True] * 1440,
            "nonwear_status": [False] * 1440,
        }
    )
    metrics = sleep_variables.SleepMetrics(data_with_awakenings)

    assert metrics.waso_30 == 30, f"Expected waso_30 = 30, got {metrics.waso_30}"


def test_waso_30_zeero(create_dummy_data: pl.DataFrame) -> None:
    """Test the waso_30 attribute."""
    metrics = sleep_variables.SleepMetrics(create_dummy_data)

    assert metrics.waso_30 == 0.0, (
        f"Expected 0 nights with waso > 30, got {metrics.waso_30}"
    )
