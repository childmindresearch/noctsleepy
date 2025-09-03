"""This module contains functions to aid in computing of sleep metrics."""

import datetime
import json
import pathlib
from typing import Iterable, Optional

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
            after sleep onset, in minutes.
        sleep_onset: Time when the first sleep period starts, within the nocturnal
            window, in HH:MM format, per night.
        sleep_wakeup: Time when the last  in HH:MM format, per night.
        sleep_midpoint: The midpoint of the sleep period, in HH:MM.
        num_awakenings: Calculate the number of awakenings during the sleep period.
        waso_30: Calculate the number of nights where WASO exceeds 30 minutes,
            normalized to a 30-day protocol.
        weekday_midpoint: Average sleep midpoint on weekdays (defaults to Monday -
            Friday night) in HH:MM format.
        weekend_midpoint: Average sleep midpoint on weekends (defaults to Saturday -
            Sunday night) in HH:MM format.
        social_jetlag: Calculate the social jetlag, defined as the absolute difference
            between the weekend and weekday sleep midpoints, in hours.
    """

    night_data: pl.DataFrame
    sampling_time: float = 5.0
    weekday_list: Iterable[int] = (0, 1, 2, 3, 4)
    weekend_list: Iterable[int] = (5, 6)
    _sleep_duration: Optional[pl.Series] = None
    _time_in_bed: Optional[pl.Series] = None
    _sleep_efficiency: Optional[pl.Series] = None
    _waso: Optional[pl.Series] = None
    _num_awakenings: Optional[pl.Series] = None
    _waso_30: Optional[float] = None
    _sleep_onset: Optional[pl.Series] = None
    _sleep_wakeup: Optional[pl.Series] = None
    _sleep_midpoint: Optional[pl.Series] = None
    _weekday_midpoint: Optional[pl.Series] = None
    _weekend_midpoint: Optional[pl.Series] = None
    _social_jetlag: Optional[float] = None
    _interdaily_stability: Optional[float] = None
    _interdaily_variability: Optional[float] = None

    def __init__(
        self,
        data: pl.DataFrame,
        night_start: datetime.time = datetime.time(hour=20, minute=0),
        night_end: datetime.time = datetime.time(hour=8, minute=0),
        weekday_list: Iterable[int] = [0, 1, 2, 3, 4],
        weekend_list: Iterable[int] = [5, 6],
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
            weekday_list: List of integers (0=Monday, 6=Sunday) representing weekdays.
                Default is [0, 1, 2, 3, 4] (Monday to Friday).
            weekend_list: List of integers representing weekend days
                Default is [5, 6] (Saturday and Sunday).

        Raises:
            ValueError: If there are no valid nights in the data.
        """
        self.night_data = _filter_nights(data, night_start, night_end, nw_threshold)
        if self.night_data.is_empty():
            raise ValueError("No valid nights found in the data.")
        self.sampling_time = self.night_data["time"].dt.time().diff()[1].total_seconds()
        self.weekdays = weekday_list
        self.weekend = weekend_list

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

    @property
    def sleep_onset(self) -> pl.Series:
        """Calculate the sleep onset time in HH:MM format per night.

        This is defined as the time when the first sleep period starts,
        within the nocturnal window.
        """
        if self._sleep_onset is None:
            self._sleep_onset = (
                self.night_data.filter(pl.col("spt_periods"))
                .group_by("night_date")
                .agg(pl.col("time").min().alias("sleep_onset"))
                .sort("night_date")
                .select("sleep_onset")
                .to_series()
                .dt.time()
            )
        return self._sleep_onset

    @property
    def sleep_wakeup(self) -> pl.Series:
        """Calculate the wakeup time in HH:MM format per night.

        Defined as the time when the last sleep period ends,
        within the nocturnal window.
        """
        if self._sleep_wakeup is None:
            self._sleep_wakeup = (
                self.night_data.filter(pl.col("spt_periods"))
                .group_by("night_date")
                .agg(pl.col("time").max().alias("sleep_wakeup"))
                .sort("night_date")
                .select("sleep_wakeup")
                .to_series()
                .dt.time()
            )
        return self._sleep_wakeup

    @property
    def sleep_midpoint(self) -> pl.Series:
        """Calculate the midpoint of the sleep period in HH:MM format per night."""
        if self._sleep_midpoint is None:
            self._sleep_midpoint = pl.Series(
                name="sleep_midpoint",
                values=[
                    _get_night_midpoint(start, end)
                    for start, end in zip(
                        self.sleep_onset, self.sleep_wakeup, strict=True
                    )
                ],
            )
        return self._sleep_midpoint

    @property
    def num_awakenings(self) -> pl.Series:
        """Calculate the number of awakenings during the sleep period."""
        if self._num_awakenings is None:
            self._num_awakenings = (
                self.night_data.filter(pl.col("spt_periods"))
                .group_by("night_date")
                .agg(
                    (pl.col("sib_periods").cast(pl.Int8).diff().eq(-1).sum()).alias(
                        "num_awakenings"
                    )
                )
                .sort("night_date")
                .select("num_awakenings")
                .to_series()
            )

        return self._num_awakenings

    @property
    def waso_30(self) -> float:
        """Calculate the number of nights where WASO exceeds 30 minutes.

        The result is normalized to a 30-day protocol.
        """
        if self._waso_30 is None:
            num_nights = self.night_data["night_date"].n_unique()
            self._waso_30 = ((self.waso > 30).sum() / num_nights) * 30

        return self._waso_30

    @property
    def weekday_midpoint(self) -> pl.Series:
        """Calculate the average sleep midpoint on weekdays in HH:MM format."""
        if self._weekday_midpoint is None:
            weekday_data = self.night_data.filter(
                pl.col("night_date").dt.weekday().is_in(self.weekdays)
            )
            if weekday_data.is_empty():
                self._weekday_midpoint = pl.Series(name="weekday_midpoint", values=[])
            else:
                weekday_onset = (
                    weekday_data.filter(pl.col("spt_periods"))
                    .group_by("night_date")
                    .agg(pl.col("time").min().alias("weekday_sleep_onset"))
                    .sort("night_date")
                    .select("weekday_sleep_onset")
                    .to_series()
                    .dt.time()
                )
                weekday_wakeup = (
                    weekday_data.filter(pl.col("spt_periods"))
                    .group_by("night_date")
                    .agg(pl.col("time").max().alias("weekday_sleep_wakeup"))
                    .sort("night_date")
                    .select("weekday_sleep_wakeup")
                    .to_series()
                    .dt.time()
                )
                self._weekday_midpoint = pl.Series(
                    name="weekday_midpoint",
                    values=[
                        _get_night_midpoint(start, end)
                        for start, end in zip(
                            weekday_onset, weekday_wakeup, strict=True
                        )
                    ],
                )

        return self._weekday_midpoint

    @property
    def weekend_midpoint(self) -> pl.Series:
        """Calculate the average sleep midpoint on weekends in HH:MM format."""
        if self._weekend_midpoint is None:
            weekend_data = self.night_data.filter(
                pl.col("night_date").dt.weekday().is_in(self.weekend)
            )
            if weekend_data.is_empty():
                self._weekend_midpoint = pl.Series(name="weekend_midpoint", values=[])
            else:
                weekend_onset = (
                    weekend_data.filter(pl.col("spt_periods"))
                    .group_by("night_date")
                    .agg(pl.col("time").min().alias("weekend_sleep_onset"))
                    .sort("night_date")
                    .select("weekend_sleep_onset")
                    .to_series()
                    .dt.time()
                )
                weekend_wakeup = (
                    weekend_data.filter(pl.col("spt_periods"))
                    .group_by("night_date")
                    .agg(pl.col("time").max().alias("weekend_sleep_wakeup"))
                    .sort("night_date")
                    .select("weekend_sleep_wakeup")
                    .to_series()
                    .dt.time()
                )
                self._weekend_midpoint = pl.Series(
                    name="weekend_midpoint",
                    values=[
                        _get_night_midpoint(start, end)
                        for start, end in zip(
                            weekend_onset, weekend_wakeup, strict=True
                        )
                    ],
                )

        return self._weekend_midpoint

    @property
    def social_jetlag(self) -> float:
        """Calculate the social jetlag in hours.

        Defined as the absolute difference between the weekend and weekday sleep
        midpoints.
        """
        if self._social_jetlag is None:
            if self.weekday_midpoint.is_empty() or self.weekend_midpoint.is_empty():
                self._social_jetlag = float("nan")
            else:
                self._social_jetlag = self.weekend_midpoint - self.weekday_midpoint

        return self._social_jetlag

    def save_to_json(
        self, filename: pathlib.Path, requested_metrics: Iterable[str]
    ) -> None:
        """Save the sleep metrics to a json file.

        Args:
            filename: The path to the output CSV file.
            requested_metrics: An iterable of the metric names to compute
                and include in the output.
        """

        def value_to_string(value: pl.Series | float) -> list[str] | str:
            if isinstance(value, pl.Series):
                return [
                    elem.strftime("%H:%M:%S")
                    if isinstance(elem, datetime.time)
                    else elem
                    for elem in value
                ]

            return str(value)

        metrics_dict = {
            key: value_to_string(getattr(self, key)) for key in requested_metrics
        }

        filename.write_text(json.dumps(metrics_dict, indent=2))


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


def _get_night_midpoint(start: datetime.time, end: datetime.time) -> datetime.time:
    """Calculate the midpoint of a nocturnal interval.

    Args:
        start: The start time of the nocturnal interval.
        end: The end time of the nocturnal interval.

    Returns:
        A datetime.time object representing the midpoint of the nocturnal interval.
    """
    start_s = start.hour * 3600 + start.minute * 60 + start.second
    end_s = end.hour * 3600 + end.minute * 60 + end.second

    if end_s < start_s:
        end_s += 24 * 3600

    midpoint_s = (start_s + end_s) // 2

    midpoint_hour = (midpoint_s % (24 * 3600)) // 3600
    midpoint_minute = (midpoint_s % 3600) // 60
    midpoint_second = midpoint_s % 60
    return datetime.time(midpoint_hour, midpoint_minute, midpoint_second)
