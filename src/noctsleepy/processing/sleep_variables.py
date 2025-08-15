"""This module contains functions to aid in computing of sleep metrics."""

import datetime
from typing import Optional

import polars as pl


class SleepMetrics:
    """Class to hold all potential sleep metrics and methods to compute them.

    Attributes:
        _night_data: Polars DataFrame containing only the filtered nights.
        _sampling_time: The sampling time in seconds,
            default is 5 seconds (from wristpy default output).
        _sleep_duration: Total sleep duration in minutes, computed from
            the sum of sustained inactivity bouts within the SPT window.
        _time_in_bed: The total duration of the SPT window(s) in minutes.
        _sleep_efficiency: The ratio of total sleep time to total sleep window time,
            expressed as a percentage.
        _waso: Wake After Sleep Onset, the total time spent awake after sleep onset.
        _num_awakenings: The number of distinct waking periods during the sleep window.
        _waso_30: Number of nights, normalized to a 30-day protocol, where WASO exceeds
            30 minutes.
        _sleep_onset: Time when sleep starts each night.
        _sleep_wakeup: Time when sleep ends each night.
        _sleep_midpoint: The midpoint between sleep onset and wakeup time.
        _weekday_midpoint: The midpoint between sleep onset and wakeup time on weekdays.
        _weekend_midpoint: The midpoint between sleep onset and wakeup time on weekends.
        _social_jetlag: Difference in sleep midpoint between weekends and weekdays,
            in minutes.
        _interdaily_stability: The ratio of variance ofthe 24-houraverage activity
            pattern to the total variance in the data.
        _interdaily_variability: Variance of consecutive activity levels over
            short time intervals.

    """

    _night_data: pl.DataFrame
    _sampling_time: float = 5.0
    _sleep_duration: Optional[pl.Series] = None
    _time_in_bed: Optional[pl.Series] = None
    _sleep_efficiency: Optional[pl.Series] = None
    _waso: Optional[pl.Series] = None
    _num_awakenings: Optional[tuple[int, ...]] = None
    _waso_30: Optional[float] = None
    _sleep_onset: Optional[tuple[datetime.time, ...]] = None
    _sleep_wakeup: Optional[tuple[datetime.time, ...]] = None
    _sleep_midpoint: Optional[tuple[datetime.time, ...]] = None
    _weekday_midpoint: Optional[tuple[datetime.time, ...]] = None
    _weekend_midpoint: Optional[tuple[datetime.time, ...]] = None
    _social_jetlag: Optional[float] = None
    _interdaily_stability: Optional[float] = None
    _interdaily_variability: Optional[float] = None

    def __init__(
        self,
        data: pl.DataFrame,
        night_start: datetime.datetime,
        night_end: datetime.datetime,
        nw_threshold: float,
    ) -> None:
        """Initialize the SleepMetrics dataclass.

        Stores the filtered night data and computes the sampling time.

        Args:
            data: Polars DataFrame containing the processed actigraphy data.
            night_start: The start time of the nocturnal interval.
            night_end: The end time of the nocturnal interval.
            nw_threshold: A threshold for the non-wear status, below which a night is
                considered valid. Expressed as a fraction (0.0 to 1.0).
        """
        self._night_data = _filter_nights(data, night_start, night_end, nw_threshold)
        self._sampling_time = (
            self._night_data["time"].dt.time().diff()[1].total_seconds()
        )

    @property
    def sleep_duration(self) -> pl.Series:
        """Calculate total sleep duration in minutes."""
        if self._sleep_duration is None:
            self._sleep_duration = (
                self._night_data.group_by("night_date")
                .agg(
                    [
                        (
                            pl.when(pl.col("spt_periods") & pl.col("sib_periods"))
                            .then(1)
                            .otherwise(0)
                            .sum()
                            * (self._sampling_time / 60)
                        ).alias("sib_within_spt"),
                    ]
                )
                .sort("night_date")
                .select("sib_within_spt")
                .to_series()
            )
        return self._sleep_duration

    @property
    def time_in_bed(self) -> pl.Series:
        """Calculate total time in bed in minutes."""
        if self._time_in_bed is None:
            self._time_in_bed = (
                self._night_data.group_by("night_date")
                .agg(
                    [
                        (
                            pl.col("spt_periods").sum() * (self._sampling_time / 60)
                        ).alias("spt_count"),
                    ]
                )
                .sort("night_date")
                .select("spt_count")
                .to_series()
            )
        return self._time_in_bed

    @property
    def waso(self) -> pl.Series:
        """Calculate Wake After Sleep Onset (WASO) in minutes."""
        if self._waso is None:
            self._waso = (self.time_in_bed - self.sleep_duration).alias("waso")
        return self._waso

    @property
    def sleep_efficiency(self) -> Optional[pl.Series]:
        """Calculate sleep efficiency as a percentage.

        Defined as the ratio of total sleep time to total time in bed.
        """
        if not (self._sleep_duration and self._time_in_bed):
            return None
        if self._sleep_efficiency is None:
            self._sleep_efficiency = (self._sleep_duration / self._time_in_bed) * 100
        return self._sleep_efficiency


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
