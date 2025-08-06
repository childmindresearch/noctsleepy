"""This module contains functions to aid in computing of sleep metrics."""

import datetime

import polars as pl


def filter_nights(
    data: pl.DataFrame,
    night_start: datetime.time = datetime.time(hour=20, minute=0),
    night_end: datetime.time = datetime.time(hour=8, minute=0),
    nw_threshold: float = 0.2,
) -> pl.DataFrame:
    """Find valid nights in the processed actigraphy data.

    A night is defined by the nocturnal interval (default is [20:00 - 08:00) ).
    The processed data is filtered to only include this window and then valid nights
    are chosen when a night has a non-wear percentage
    below the specified threshold.

    Args:
        data: Polars dataframe containing the processed actigraphy data,
            including non-wear time.
        night_start: The start time of the nocturnal interval.
            Default is 20:00 (8 PM).
        night_end: The end time of the nocturnal interval.
            Default is 08:00 (8 AM).
        nw_threshold: A threshold for the non-wear status, below which a night is
            considered valid. Expressed as a percentage (0.0 to 1.0).

    Returns:
        A Polars DataFrame containing only the valid nights.
    """
    if night_start > night_end:
        nocturnal_sleep = data.filter(
            (pl.col("time").dt.time() >= night_start)
            | (pl.col("time").dt.time() < night_end)
        )
        nocturnal_sleep = nocturnal_sleep.with_columns(
            [
                pl.when(pl.col("time").dt.time() >= night_start)
                .then(pl.col("time").dt.date())
                .otherwise(pl.col("time").dt.date() - pl.duration(days=1))
                .alias("night_date")
            ]
        )
    else:
        nocturnal_sleep = data.filter(
            (pl.col("time").dt.time() >= night_start)
            & (pl.col("time").dt.time() < night_end)
        )
        nocturnal_sleep = nocturnal_sleep.with_columns(
            pl.col("time").dt.date().alias("night_date")
        )

    night_stats = (
        nocturnal_sleep.group_by("night_date")
        .agg(
            [
                pl.col("nonwear_status").sum().alias("non_wear_count"),
                pl.col("nonwear_status").count().alias("total_count"),
            ]
        )
        .with_columns(
            [
                (pl.col("non_wear_count") / pl.col("total_count")).alias(
                    "non_wear_percentage"
                )
            ]
        )
    )

    valid_nights = night_stats.filter(
        pl.col("non_wear_percentage") <= nw_threshold
    ).select(["night_date"])

    return nocturnal_sleep.join(valid_nights, on="night_date", how="inner").sort("time")
