"""Unit tests for the utils module."""

import datetime

import polars as pl
import pytest

from noctsleepy.processing import utils


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
