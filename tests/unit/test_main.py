"""Test the main module of noctsleepy."""

import pathlib

from noctsleepy.main import compute_sleep_metrics
from noctsleepy.processing import sleep_variables


def test_compute_sleep_metrics(sample_csv_data: pathlib.Path) -> None:
    """Test the compute_sleep_metrics function with a sample CSV file."""
    metrics = compute_sleep_metrics(sample_csv_data)

    assert isinstance(metrics, sleep_variables.SleepMetrics)
