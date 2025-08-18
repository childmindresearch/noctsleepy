"""This module contains functions to aid in computing of sleep metrics."""

import datetime
import pathlib
import typing

import polars as pl


class SleepMetrics:
    """Class to hold all potential sleep metrics and methods to compute them.

    Attributes:
        night_data: Polars DataFrame containing only the filtered nights.
        sampling_time: The sampling time in seconds,
            default is 5 seconds (from wristpy default output).
        sleep_duration: Calculate te total sleep duration in minutes, computed from
            the sum of sustained inactivity bouts within the SPT window.
        time_in_bed: Calculate the total duration of the SPT window(s) in minutes, this
            is analogus to the time in bed.
        sleep_efficiency: Calculate he ratio of total sleep time to total time in bed,
            per night, expressed as a percentage.
        waso: Calculate the "Wake After Sleep Onset", the total time spent awake
            after sleep onset.
        num_awakenings: Calculate the number of distinct waking periods
            during the sleep window(s), per night.
        waso_30: Find the number of nights, normalized to a 30-day protocol,
            where WASO exceeds 30 minutes.
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

    night_data: pl.DataFrame
    sampling_time: float = 5.0
    _sleep_duration: typing.Optional[pl.Series] = None
    _time_in_bed: typing.Optional[pl.Series] = None
    _sleep_efficiency: typing.Optional[pl.Series] = None
    _waso: typing.Optional[pl.Series] = None
    _num_awakenings: typing.Optional[pl.Series] = None
    _waso_30: typing.Optional[float] = None
    _sleep_onset: typing.Optional[pl.Series] = None
    _sleep_wakeup: typing.Optional[pl.Series] = None
    _sleep_midpoint: typing.Optional[pl.Series] = None
    _weekday_midpoint: typing.Optional[pl.Series] = None
    _weekend_midpoint: typing.Optional[pl.Series] = None
    _social_jetlag: typing.Optional[float] = None
    _interdaily_stability: typing.Optional[float] = None
    _interdaily_variability: typing.Optional[float] = None

    def __init__(
        self,
        data: pl.DataFrame,
        night_start: datetime.time = datetime.time(hour=20, minute=0),
        night_end: datetime.time = datetime.time(hour=8, minute=0),
        nw_threshold: float = 0.2,
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
        self.night_data = _filter_nights(data, night_start, night_end, nw_threshold)
        self.sampling_time = self.night_data["time"].dt.time().diff()[1].total_seconds()

    @property
    def sleep_duration(self) -> pl.Series:
        """Calculate total sleep duration in minutes."""
        if self._sleep_duration is None:
            self._sleep_duration = (
                self.night_data.group_by("night_date")
                .agg(
                    [
                        (
                            pl.when(pl.col("spt_periods") & pl.col("sib_periods"))
                            .then(1)
                            .otherwise(0)
                            .sum()
                            * (self.sampling_time / 60)
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
                self.night_data.group_by("night_date")
                .agg(
                    [
                        (pl.col("spt_periods").sum() * (self.sampling_time / 60)).alias(
                            "spt_count"
                        ),
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
            self._waso = self.time_in_bed - self.sleep_duration
        return self._waso

    @property
    def sleep_efficiency(self) -> pl.Series:
        """Calculate sleep efficiency as a percentage.

        Defined as the ratio of total sleep time to total time in bed.
        """
        if self._sleep_efficiency is None:
            self._sleep_efficiency = (self.sleep_duration / self.time_in_bed) * 100
        return self._sleep_efficiency

    def save_to_csv(self, filename: pathlib.Path) -> None:
        """Save the sleep metrics to a CSV file.

        Args:
            filename: The path to the output CSV file.
        """
        df = pl.DataFrame(
            {
                "sleep_duration": self._sleep_duration,
                "time_in_bed": self._time_in_bed,
                "sleep_efficiency": self._sleep_efficiency,
                "waso": self._waso,
            }
        )
        df.write_csv(filename)


def _filter_nights(
    data: pl.DataFrame,
    night_start: datetime.time,
    night_end: datetime.time,
    nw_threshold: float,
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
