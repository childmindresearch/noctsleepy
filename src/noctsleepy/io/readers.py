"""This module contains the functionality to load processed actigraphy data."""

import pathlib

import polars as pl


def read_wristpy_data(filename: pathlib.Path) -> pl.DataFrame:
    """Read processed actigraphy data from either csv or parquet files.

    Primarily, data must be saved in a format that agrees with the
    wristpy processing toolbox.

    Args:
        filename: The path to the file.

    Returns:
        A Polars DataFrame containing the processed data,
            only the relevant columns containing sleep data are returned.

    Raises:
        FileNotFoundError: If the file does not exist.
        ValueError:
            If the file format is not supported.
            If required columns are missing in the data.
    """
    if not filename.exists():
        raise FileNotFoundError(f"The file {filename} does not exist.")

    required_columns = [
        "time",
        "sleep_status",
        "sib_periods",
        "spt_periods",
        "nonwear_status",
    ]

    if filename.suffix == ".csv":
        try:
            processed_data = pl.read_csv(
                filename, try_parse_dates=True, columns=required_columns
            )
        except pl.exceptions.ColumnNotFoundError as e:
            raise ValueError(f"Missing required columns in the data: {str(e)}") from e
    elif filename.suffix == ".parquet":
        try:
            processed_data = pl.read_parquet(filename, columns=required_columns)
        except pl.exceptions.ColumnNotFoundError as e:
            raise ValueError(f"Missing required columns in the data: {str(e)}") from e
    else:
        raise ValueError(
            (
                f"Unsupported file format: {filename.suffix}. "
                "Supported formats are .csv and .parquet."
            )
        )

    return processed_data
