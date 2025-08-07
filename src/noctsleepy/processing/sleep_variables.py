"""This module contains functions to aid in computing of sleep metrics."""

import datetime
from typing import Optional

import polars as pl
import pydantic
from pydantic import dataclasses


@dataclasses.dataclass(config=pydantic.ConfigDict(validate_assignment=True))
class SleepMetrics:
    """Dataclass to hold all potential sleep metrics.

    Attributes:
        sleep_duration: Total sleep duration in minutes, computed from
            the sum of sustained inactivity bouts within the SPT window.
        time_in_bed: The total duration of the SPT window(s) in minutes.
        waso: Wake After Sleep Onset, the total time spent awake after sleep onset.
        sleep_efficiency: The ratio of total sleep time to total sleep window time,
            expressed as a percentage.
        num_awakenings: The number of distinct waking periods during the sleep window.
        waso_30: Number of nights, normalized to a 30-day protocol, where WASO exceeds
            30 minutes.
        sleep_onset: Time when sleep starts each night.
        sleep_wakeup: Time when sleep ends each night.
        sleep_midpoint: The midpoint between sleep onset and wakeup time.
        weekday_midpoint: The midpoint between sleep onset and wakeup time on weekdays.
        weekend_midpoint: The midpoint between sleep onset and wakeup time on weekends.
        social_jetlag: Difference in sleep midpoint between weekends and weekdays,
            in minutes.
        interdaily_stability: The ratio of variance ofthe 24-houraverage activity
            pattern to the total variance in the data.
        interdaily_variability: Variance of consecutive activity levels over
            short time intervals.

    """

    sleep_duration: Optional[tuple[float, ...]] = None
    time_in_bed: Optional[tuple[float, ...]] = None
    waso: Optional[tuple[float, ...]] = None
    sleep_efficiency: Optional[tuple[float, ...]] = None
    num_awakenings: Optional[tuple[int, ...]] = None
    waso_30: Optional[float] = None
    sleep_onset: Optional[tuple[datetime.time, ...]] = None
    sleep_wakeup: Optional[tuple[datetime.time, ...]] = None
    sleep_midpoint: Optional[tuple[datetime.time, ...]] = None
    weekday_midpoint: Optional[tuple[datetime.time, ...]] = None
    weekend_midpoint: Optional[tuple[datetime.time, ...]] = None
    social_jetlag: Optional[float] = None
    interdaily_stability: Optional[float] = None
    interdaily_variability: Optional[float] = None


def _filter_nights(
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

    return nocturnal_sleep.join(valid_nights, on="night_date").sort("time")
