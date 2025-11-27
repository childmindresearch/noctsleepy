"""Unit tests for the sleep_variables module."""

import datetime
import math

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
    timezone = "UTC"

    valid_nights = sleep_variables._filter_nights(
        create_dummy_data,
        night_start,
        night_end,
        nw_threshold,
        timezone,
        sampling_time=60,
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
    timezone = "UTC"

    valid_nights = sleep_variables._filter_nights(
        create_dummy_data,
        night_start,
        night_end,
        nw_threshold,
        timezone,
        sampling_time=60,
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
    metrics = sleep_variables.SleepMetrics(create_dummy_data, timezone="UTC")

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
        sleep_variables.SleepMetrics(bad_data, timezone="UTC")


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
    metrics = sleep_variables.SleepMetrics(create_dummy_data, timezone="UTC")

    result = getattr(metrics, selected_metrics)

    assert result.to_list() == expected_values, (
        f"Expected {expected_values.to_list()}, got {result.to_list()}"
    )


def test_sleep_onset(create_dummy_data: pl.DataFrame) -> None:
    """Test the sleep_onset method."""
    metrics = sleep_variables.SleepMetrics(create_dummy_data, timezone="UTC")
    expected_onset = datetime.time(hour=20, minute=0)

    assert metrics.sleep_onset[0] == expected_onset, (
        f"Expected onset {expected_onset}, got {metrics.sleep_onset[0]}"
    )


def test_sleep_wakeup(create_dummy_data: pl.DataFrame) -> None:
    """Test the sleep_wakeup method."""
    metrics = sleep_variables.SleepMetrics(create_dummy_data, timezone="UTC")
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
    metrics = sleep_variables.SleepMetrics(data_with_awakenings, timezone="UTC")

    assert metrics.num_awakenings.to_list() == [2], (
        f"Expected 2 awakenings, got {metrics.num_awakenings.to_list()}"
    )


def test_num_awakenings_zero(create_dummy_data: pl.DataFrame) -> None:
    """Test the num_awakenings attribute when there are no awakenings."""
    metrics = sleep_variables.SleepMetrics(create_dummy_data, timezone="UTC")

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
    metrics = sleep_variables.SleepMetrics(data_with_awakenings, timezone="UTC")

    assert metrics.waso_30 == 30, f"Expected waso_30 = 30, got {metrics.waso_30}"


def test_waso_30_zero(create_dummy_data: pl.DataFrame) -> None:
    """Test the waso_30 attribute."""
    metrics = sleep_variables.SleepMetrics(create_dummy_data, timezone="UTC")

    assert metrics.waso_30 == 0.0, (
        f"Expected 0 nights with waso > 30, got {metrics.waso_30}"
    )


def test_weekday_midpoint(create_dummy_data: pl.DataFrame) -> None:
    """Test the weekday_midpoint attribute."""
    metrics = sleep_variables.SleepMetrics(create_dummy_data, timezone="UTC")

    assert metrics.weekday_midpoint.to_list() == [
        datetime.time(hour=1, minute=59, second=30)
    ], f"Expected weekday midpoint 01:59:30, got {metrics.weekday_midpoint.to_list()}"


def test_weekend_midpoint() -> None:
    """Test the weekend_midpoint attribute."""
    dummy_date = datetime.datetime(year=2025, month=5, day=3, hour=10, minute=0)
    dummy_datetime_list = [
        dummy_date + datetime.timedelta(minutes=i) for i in range(1440)
    ]
    weekend_data = pl.DataFrame(
        {
            "time": dummy_datetime_list,
            "sib_periods": [True] * 1440,
            "spt_periods": [True] * 1440,
            "nonwear_status": [False] * 1440,
        }
    )

    metrics = sleep_variables.SleepMetrics(weekend_data, timezone="UTC")

    assert metrics.weekend_midpoint.to_list() == [
        datetime.time(hour=1, minute=59, second=30)
    ], f"Expected weekend midpoint 01:59:30, got {metrics.weekend_midpoint.to_list()}"


def test_social_jetlag_missing_data(create_dummy_data: pl.DataFrame) -> None:
    """Test the social_jetlag attribute when no weekend data is present."""
    metrics = sleep_variables.SleepMetrics(create_dummy_data, timezone="UTC")

    assert math.isnan(metrics.social_jetlag), (
        f"Expected nan for social jetlag with no weekend data, "
        f"got {metrics.social_jetlag}"
    )


def test_social_jetlag() -> None:
    """Test the social jetlag attribute."""
    dummy_date = datetime.datetime(year=2025, month=9, day=5, hour=10, minute=0)
    dummy_datetime_list = [
        dummy_date + datetime.timedelta(minutes=15 * i) for i in range(400)
    ]
    data = pl.DataFrame(
        {
            "time": dummy_datetime_list,
            "sib_periods": [True] * 400,
            "spt_periods": [True] * 400,
            "nonwear_status": [False] * 400,
        }
    )
    metrics = sleep_variables.SleepMetrics(data, timezone="UTC")

    assert metrics.social_jetlag == 0.0, (
        f"Expected social jetlag = 0.0, got {metrics.social_jetlag}"
    )


@pytest.mark.parametrize(
    "time1, time2, expected_diff",
    [
        (datetime.time(hour=1, minute=0), datetime.time(hour=2, minute=0), 1.0),
        (datetime.time(hour=23, minute=0), datetime.time(hour=1, minute=0), 2.0),
    ],
)
def test_time_diff(
    time1: datetime.time, time2: datetime.time, expected_diff: float
) -> None:
    """Test the _compute_time_diff function."""
    diff = sleep_variables._time_difference_abs_hours(time1, time2)

    assert diff == expected_diff, f"Expected {expected_diff}, got {diff}"


@pytest.mark.parametrize(
    "timezone, expected_difference",
    [
        ("America/New_York", -4.0),
        ("Europe/London", 1.0),
        ("Asia/Tokyo", 9.0),
    ],
)
def test_utc_conversion(
    create_dummy_data: pl.DataFrame,
    timezone: str,
    expected_difference: float,
) -> None:
    """Test the _convert_to_utc function."""
    data_utc = sleep_variables._convert_to_utc(create_dummy_data, timezone)

    tolerance = 1e-9
    assert (
        (data_utc["utc_offset_hours"] - expected_difference).abs() < tolerance
    ).all(), (
        f"Expected time difference of {expected_difference}, "
        f"got {data_utc['utc_offset_hours'].unique()}"
    )


def test_dst_fall_back_sleep_variables() -> None:
    """Test how sleep_variables are computed after DST transition."""
    dummy_date = datetime.datetime(year=2025, month=11, day=1, hour=17, minute=0)
    dummy_datetime_list = [
        dummy_date + datetime.timedelta(minutes=10 * i) for i in range(288)
    ]
    data = pl.DataFrame(
        {
            "time": dummy_datetime_list,
            "sib_periods": [
                (datetime.time(22, 0) <= t.time() or t.time() < datetime.time(6, 0))
                for t in dummy_datetime_list
            ],
            "spt_periods": [
                (datetime.time(22, 0) <= t.time() or t.time() < datetime.time(6, 0))
                for t in dummy_datetime_list
            ],
            "nonwear_status": [False] * len(dummy_datetime_list),
        }
    )
    timezone = "America/New_York"
    night_start = datetime.time(20, 0)
    night_end = datetime.time(8, 0)
    nw_threshold = 0.2

    result = sleep_variables.SleepMetrics(
        data,
        timezone=timezone,
        night_start=night_start,
        night_end=night_end,
        nw_threshold=nw_threshold,
    )
    assert result.sleep_duration.to_list() == [480, 480], (
        f"Expected [480, 480], got {result.sleep_duration.to_list()}"
    )
    assert result.sleep_midpoint.to_list() == [
        datetime.time(hour=1, minute=25),
        datetime.time(hour=0, minute=55),
    ], f"Expected [1:25, 0:55], got {result.sleep_midpoint.to_list()}"
    assert result.sleep_onset.to_list() == [
        datetime.time(hour=22, minute=0),
        datetime.time(hour=21, minute=0),
    ], f"Expected [22:00, 21:00], got {result.sleep_onset.to_list()}"

    assert result.sleep_wakeup.to_list() == [
        datetime.time(hour=4, minute=50),
        datetime.time(hour=4, minute=50),
    ], f"Expected [4:50, 4:50], got {result.sleep_wakeup.to_list()}"


def test_dst_forward_sleep_variables() -> None:
    """Test how sleep_variables are computed for London spring forward."""
    dummy_date = datetime.datetime(year=2025, month=3, day=8, hour=17, minute=0)
    dummy_datetime_list = [
        dummy_date + datetime.timedelta(minutes=10 * i) for i in range(288)
    ]

    data = pl.DataFrame(
        {
            "time": dummy_datetime_list,
            "sib_periods": [
                (datetime.time(22, 0) <= t.time() or t.time() < datetime.time(6, 0))
                for t in dummy_datetime_list
            ],
            "spt_periods": [
                (datetime.time(22, 0) <= t.time() or t.time() < datetime.time(6, 0))
                for t in dummy_datetime_list
            ],
            "nonwear_status": [False] * len(dummy_datetime_list),
        }
    )

    timezone = "America/New_York"
    night_start = datetime.time(20, 0)
    night_end = datetime.time(8, 0)
    nw_threshold = 0.2

    result = sleep_variables.SleepMetrics(
        data,
        timezone=timezone,
        night_start=night_start,
        night_end=night_end,
        nw_threshold=nw_threshold,
    )
    assert result.sleep_duration.to_list() == [480, 480], (
        f"Expected [480, 480], got {result.sleep_duration.to_list()}"
    )
    assert result.sleep_midpoint.to_list() == [
        datetime.time(hour=2, minute=25),
        datetime.time(hour=2, minute=55),
    ], f"Expected [2:25, 2:55], got {result.sleep_midpoint.to_list()}"

    assert result.sleep_onset.to_list() == [
        datetime.time(hour=22, minute=0),
        datetime.time(hour=23, minute=0),
    ], f"Expected [22:00, 23:00], got {result.sleep_onset.to_list()}"

    assert result.sleep_wakeup.to_list() == [
        datetime.time(hour=6, minute=50),
        datetime.time(hour=6, minute=50),
    ], f"Expected [6:50, 6:50], got {result.sleep_wakeup.to_list()}"


def test_get_simple_statistics(create_dummy_data: pl.DataFrame) -> None:
    """Test the extract_simple_statistics function."""
    metrics = sleep_variables.SleepMetrics(create_dummy_data, timezone="UTC")
    metrics.sleep_duration
    stats = sleep_variables.extract_simple_statistics(metrics)

    expected_stats = {
        "sleep_duration_mean": 720.0,
        "sleep_duration_sd": 0.0,
        "time_in_bed_mean": 720.0,
        "time_in_bed_sd": 0.0,
        "sleep_efficiency_mean": 100.0,
        "sleep_efficiency_sd": 0.0,
    }

    for key, expected_value in expected_stats.items():
        assert stats[key] == expected_value, (
            f"Expected {key} = {expected_value}, got {stats[key]}"
        )
