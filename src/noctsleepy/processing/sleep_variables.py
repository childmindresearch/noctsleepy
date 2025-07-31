"""This module contains functions to aid in computing of sleep metrics."""

import datetime

import polars as pl


def find_valid_nights(
    data: pl.DataFrame,
    nocturnal_interval: tuple[datetime.time, datetime.time] = (
        datetime.time(hour=20, minute=0),
        datetime.time(hour=8, minute=0),
    ),
    nw_threshold: float = 0.2,
) -> pl.DataFrame:
    """Find valid nights in the processed actigraphy data.

    A night is defined by the nocturnal interval (default is [20:00 - 08:00[ ).
    The processed data is filtered to only include this window and then valid nights
    are chosen when a night has a non-wear percentage
    below the specified threshold.

    Args:
        data: Polars dataframe containing the processed actigraphy data,
            including non-wear time.
        nocturnal_interval: A tuple of two datetime.time objects representing
            the start and end of the user defined nocturnal interval.
        nw_threshold: A threshold for the non-wear status, below which a night is
            considered valid.

    Returns:
        A Polars DataFrame containing only the valid nights.
    """
    night_start, night_end = nocturnal_interval

    if night_start < night_end:
        nocturnal_sleep = data.filter(
            (pl.col("time").dt.time() >= night_start)
            & (pl.col("time").dt.time() <= night_end)
        )
        nocturnal_sleep = nocturnal_sleep.with_columns(
            pl.col("time").dt.date().alias("night_date")
        )

    else:
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

    valid_nights = (
        night_stats.filter(pl.col("non_wear_percentage") <= nw_threshold)
        .sort("night_date")
        .with_columns([pl.arange(pl.len()).alias("night_number") + 1])
        .select(["night_date", "night_number"])
    )

    return (
        nocturnal_sleep.join(valid_nights, on="night_date", how="inner")
        .drop("night_date")
        .sort("time")
    )
