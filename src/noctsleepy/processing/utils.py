"""Utility functions for extracting simple statistics from sleep metrics."""

import datetime

import numpy as np
import polars as pl


def _convert_time_to_minutes(datetime_obj: datetime.time) -> float:
    """Convert a datetime.time object to minutes since midnight.

    Args:
        datetime_obj: A datetime.time object.

    Returns:
        Minutes since midnight as a float.
    """
    return datetime_obj.hour * 60 + datetime_obj.minute + datetime_obj.second / 60


def _convert_minutes_to_time(minutes: float) -> datetime.time:
    """Convert minutes since midnight to a datetime.time object.

    Args:
        minutes: Minutes since midnight.

    Returns:
        A datetime.time object.
    """
    minutes = minutes % 1440
    hours = int(minutes // 60)
    mins = int(minutes % 60)
    seconds = int((minutes % 1) * 60)
    return datetime.time(hours, mins, seconds)


def compute_circular_mean_time(datetime_series: pl.Series) -> datetime.time:
    """Calculate circular mean of time values.

    Uses the circular mean definition from directional statistics to properly
    handle times that cross midnight.
    For example, the mean of 23:30 and 00:30 should be 00:00, not 12:00.

    Args:
        datetime_series: Polars Series containing datetime.time objects.

    Returns:
        Circular mean as a datetime.time object.
    """
    radians_conversion = 2 * np.pi / 1440
    minutes = [_convert_time_to_minutes(t) for t in datetime_series.to_list()]

    angles = [radians_conversion * m for m in minutes]

    sin_sum = sum(np.sin(angle) for angle in angles)
    cos_sum = sum(np.cos(angle) for angle in angles)
    mean_angle = np.arctan2(sin_sum, cos_sum)

    mean_minutes = mean_angle / radians_conversion
    if mean_minutes < 0:
        mean_minutes += 1440

    return _convert_minutes_to_time(mean_minutes)


def compute_circular_sd_time(time_series: pl.Series) -> float:
    """Calculate circular standard deviation of time values in minutes.

    Uses the definition of circular standard deviation from directional statistics.

    Args:
        time_series: Polars Series containing datetime.time objects.

    Returns:
        Circular standard deviation in minutes.
    """
    radian_conversion = 2 * np.pi / 1440
    minutes = [_convert_time_to_minutes(t) for t in time_series.to_list()]

    angles = [radian_conversion * m for m in minutes]
    n = len(angles)
    sin_sum = sum(np.sin(angle) for angle in angles)
    cos_sum = sum(np.cos(angle) for angle in angles)

    r = np.sqrt(sin_sum**2 + cos_sum**2) / n

    if r < 1e-10:
        return float("inf")

    circular_sd = np.sqrt(-2 * np.log(r)) / radian_conversion

    return circular_sd


def keep_longest_sleep_window(night_data: pl.DataFrame) -> pl.DataFrame:
    """Keep only the longest continuous sleep window per night.

    For each night_date, identifies all continuous sleep windows (where
    sleep_status is True) and retains only the rows belonging to the
    longest continuous sleep bout.

    First, we count contiguous sleep bouts by creating a group identifier
    that increments whenever the sleep_status changes from awake to sleep, as well
    as when the night datae changes (in case a window ends with sleep and the next window also starts with sleep).

    Args:
        night_data: DataFrame containing filtered night data with sleep_status column.

    Returns:
        DataFrame containing only the longest sleep window per night_date.
    """
    night_data = night_data.with_columns(
        [
            (
                (
                    pl.col("night_date").diff().dt.total_days() != 0
                )  # due to non-wear filtering this difference can be > 1
                | (pl.col("sleep_status").cast(pl.Int8).diff() == 1)
            )
            .cum_sum()
            .alias("candidate_sleep_bout")
        ]
    )

    bout_lengths = (
        night_data.filter(pl.col("sleep_status"))
        .group_by(["night_date", "candidate_sleep_bout"])
        .agg([pl.len().alias("bout_length")])
    )

    longest_bout_per_night_date = bout_lengths.group_by("night_date").agg(
        [
            pl.col("candidate_sleep_bout")
            .sort_by("bout_length")
            .last()
            .alias("longest_bout")
        ]
    )

    return (
        night_data.join(longest_bout_per_night_date, on="night_date")
        .filter(
            (pl.col("candidate_sleep_bout") == pl.col("longest_bout"))
            & pl.col("sleep_status")
        )
        .drop(["candidate_sleep_bout", "longest_bout"])
    )
