"""This module contains the functionality to load processed actigraphy data."""

import pathlib

import polars as pl


def read_processed_data(filename: pathlib.Path) -> pl.DataFrame:
    """Read processed actigraphy data from either csv or parquet files.

    Args:
        filename: The path to the file.

    Returns:
        A Polars DataFrame containing the processed data,
            only the relevant columns containing sleep data are returned.

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError: If the file format is not supported.
    """
    if not filename.exists():
        raise FileNotFoundError(f"The file {filename} does not exist.")

    if filename.suffix == ".csv":
        processed_data = pl.read_csv(filename, try_parse_dates=True)
    elif filename.suffix == ".parquet":
        processed_data = pl.read_parquet(filename)
    else:
        raise ValueError(
            (
                f"Unsupported file format: {filename.suffix}. "
                "Supported formats are .csv and .parquet."
            )
        )

    return processed_data.select(
        [
            "time",
            "sleep_status",
            "sib_periods",
            "spt_periods",
        ]
    )
