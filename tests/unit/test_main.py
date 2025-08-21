"""Test the main module of noctsleepy."""

import pathlib

import polars as pl

from noctsleepy.main import compute_sleep_metrics
from noctsleepy.processing import sleep_variables


def test_compute_sleep_metrics(sample_csv_data: pathlib.Path) -> None:
    """Test the compute_sleep_metrics function with a sample CSV file."""
    metrics = compute_sleep_metrics(sample_csv_data)

    assert isinstance(metrics, sleep_variables.SleepMetrics)
    assert isinstance(metrics._sleep_duration, pl.Series)
    assert isinstance(metrics._time_in_bed, pl.Series)
    assert isinstance(metrics._sleep_efficiency, pl.Series)
    assert isinstance(metrics._waso, pl.Series)
    assert isinstance(metrics._sleep_onset, pl.Series)
    assert isinstance(metrics._sleep_wakeup, pl.Series)
    assert isinstance(metrics._sleep_midpoint, pl.Series)
