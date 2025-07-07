"""Unit tests for the readers module."""

import pathlib

import polars as pl
import pytest

from noctsleepy.io.readers import read_processed_data


def test_read_processed_data_csv(sample_csv_data: pathlib.Path) -> None:
    """Test reading processed data from a CSV file."""
    csv_data = read_processed_data(sample_csv_data)

    assert isinstance(csv_data, pl.DataFrame)
    assert set(csv_data.columns) == {
        "time",
        "sleep_status",
        "sib_periods",
        "spt_periods",
    }


def test_file_not_found() -> None:
    """Test reading a non-existent file raises FileNotFoundError."""
    non_existent_file = pathlib.Path("non_existent_file.csv")

    with pytest.raises(FileNotFoundError):
        read_processed_data(non_existent_file)
