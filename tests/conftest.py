"""Fixtures used by pytest."""

import pathlib

import pytest


@pytest.fixture
def sample_csv_data() -> pathlib.Path:
    """Test data for .csv data file."""
    return pathlib.Path(__file__).parent / "sample_data" / "example_data.csv"


@pytest.fixture
def sample_txt_data() -> pathlib.Path:
    """Test data for .txt data file."""
    return pathlib.Path(__file__).parent / "sample_data" / "example_text.txt"


@pytest.fixture
def sample_incomplete_data() -> pathlib.Path:
    """Test data for .txt data file."""
    return pathlib.Path(__file__).parent / "sample_data" / "incomplete_data.csv"
