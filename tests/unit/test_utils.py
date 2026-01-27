"""Unit tests for the utils module."""

import datetime

import polars as pl
import pytest

from noctsleepy.processing import sleep_variables, utils


@pytest.mark.parametrize(
    "input_series, expected_mean",
    [
        (pl.Series([datetime.time(23, 30), datetime.time(0, 30)]), datetime.time(0, 0)),
        (
            pl.Series([datetime.time(0, 0), datetime.time(12, 0)]),
            datetime.time(6, 0),
        ),
        (
            pl.Series(
                [
                    datetime.time(22, 0),
                    datetime.time(23, 0),
                    datetime.time(1, 0),
                    datetime.time(2, 0),
                ]
            ),
            datetime.time(0, 0),
        ),
    ],
)
def test_compute_circular_mean_time(
    input_series: pl.Series, expected_mean: datetime.time
) -> None:
    """Test the compute_circular_mean_time function."""
    mean_time = utils.compute_circular_mean_time(input_series)
    assert mean_time == expected_mean


@pytest.mark.parametrize(
    "input_series, expected_sd",
    [
        (
            pl.Series(
                [datetime.time(10, 0), datetime.time(10, 0), datetime.time(10, 0)]
            ),
            0,
        ),
        (
            pl.Series(
                [datetime.time(23, 45), datetime.time(0, 0), datetime.time(0, 15)]
            ),
            pytest.approx(12.0, abs=1.0),
        ),
        (
            pl.Series(
                [
                    datetime.time(6, 0),
                    datetime.time(12, 0),
                    datetime.time(18, 0),
                    datetime.time(0, 0),
                ]
            ),
            float("inf"),
        ),
    ],
)
def test_compute_circular_sd_time(input_series: pl.Series, expected_sd: float) -> None:
    """Test the compute_circular_sd_time function."""
    sd_time = utils.compute_circular_sd_time(input_series)
    assert sd_time == expected_sd


def test_longest_sleep() -> None:
    """Test the keep_longest_sleep_window function."""
    dummy_date = datetime.datetime(year=2024, month=5, day=2, hour=20, minute=0)
    dummy_datetime_list = [
        dummy_date + datetime.timedelta(minutes=i) for i in range(1440)
    ]
    longest_sleep_length = 500
    short_sleep_length = 120
    data = pl.DataFrame(
        {
            "time": dummy_datetime_list,
            "sib_periods": [True] * longest_sleep_length
            + [False] * 100
            + [True] * short_sleep_length
            + [False] * 720,
            "spt_periods": [True] * longest_sleep_length
            + [False] * 100
            + [True] * short_sleep_length
            + [False] * 720,
            "sleep_status": [True] * longest_sleep_length
            + [False] * 100
            + [True] * short_sleep_length
            + [False] * 720,
            "nonwear_status": [False] * 1440,
        }
    )
    sleep_metrics_init = sleep_variables.SleepMetrics(data, timezone="UTC")

    filtered_data = utils.keep_longest_sleep_window(sleep_metrics_init.night_data)

    assert filtered_data.shape[0] == longest_sleep_length - 1
    assert all(filtered_data["sleep_status"])
