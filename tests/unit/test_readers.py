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
        "nonwear_status",
    }


def test_file_not_found() -> None:
    """Test reading a non-existent file raises FileNotFoundError."""
    non_existent_file = pathlib.Path("non_existent_file.csv")

    with pytest.raises(FileNotFoundError):
        read_processed_data(non_existent_file)


def test_unsupported_file_format(sample_txt_data: pathlib.Path) -> None:
    """Test reading a file with an unsupported format raises ValueError."""
    with pytest.raises(ValueError, match="Unsupported file format: .txt"):
        read_processed_data(sample_txt_data)


def test_missing_required_columns(sample_incomplete_data: pathlib.Path) -> None:
    """Test reading a file with missing required columns raises ValueError."""
    with pytest.raises(
        ValueError,
        match="Missing required columns in the data:",
    ):
        read_processed_data(sample_incomplete_data)
