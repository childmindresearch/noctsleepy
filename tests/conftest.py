"""Fixtures used by pytest."""

import pathlib

import pytest


@pytest.fixture
def sample_csv_data() -> pathlib.Path:
    """Test data for .gt3x data file."""
    return pathlib.Path(__file__).parent / "sample_data" / "example_data.csv"
