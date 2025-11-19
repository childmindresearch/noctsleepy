"""Test the main module of noctsleepy."""

import pathlib

import polars as pl
import pytest

from noctsleepy import main
from noctsleepy.processing import sleep_variables


def test_compute_sleep_metrics(sample_csv_data: pathlib.Path) -> None:
    """Test the compute_sleep_metrics function with a sample CSV file."""
    metrics = main.compute_sleep_metrics(sample_csv_data, timezone="America/New_York")

    assert isinstance(metrics, sleep_variables.SleepMetrics)
    assert isinstance(metrics._sleep_duration, pl.Series)
    assert isinstance(metrics._time_in_bed, pl.Series)
    assert isinstance(metrics._sleep_efficiency, pl.Series)
    assert isinstance(metrics._waso, pl.Series)
    assert isinstance(metrics._sleep_onset, pl.Series)
    assert isinstance(metrics._sleep_wakeup, pl.Series)
    assert isinstance(metrics._sleep_midpoint, pl.Series)
    assert isinstance(metrics._num_awakenings, pl.Series)
    assert isinstance(metrics._waso_30, float)
    assert isinstance(metrics._weekday_midpoint, pl.Series)
    assert isinstance(metrics._weekend_midpoint, pl.Series)
    assert isinstance(metrics._social_jetlag, float)


def test_bad_timezone(sample_csv_data: pathlib.Path) -> None:
    """Test that compute_sleep_metrics raises an error for an invalid timezone."""
    with pytest.raises(ValueError, match="Invalid timezone"):
        main.compute_sleep_metrics(sample_csv_data, timezone="fake timezone")
