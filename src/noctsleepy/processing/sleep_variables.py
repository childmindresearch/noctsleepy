"""This module contains functions to aid in computing of sleep metrics."""

import datetime
import enum
import json
import pathlib
from typing import Iterable, Optional

import polars as pl

from noctsleepy.processing import utils


class DayOfWeek(enum.IntEnum):
    """Class to represent days of the week as integers."""

    MONDAY = 1
    TUESDAY = 2
    WEDNESDAY = 3
    THURSDAY = 4
    FRIDAY = 5
    SATURDAY = 6
    SUNDAY = 7


class SleepMetrics:
    """Class to hold all potential sleep metrics and methods to compute them.

    Attributes:
        night_data: Polars DataFrame containing only the filtered nights.
        sampling_time: The sampling time in seconds.
        sleep_duration: Calculate the total sleep duration in minutes, computed from
            the sum of sustained inactivity bouts within the SPT window.
        time_in_bed: Calculate the total duration of the SPT window(s) in minutes, this
            is analogous to the time in bed.
        sleep_efficiency: Calculate the ratio of total sleep time to total time in bed,
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
    sampling_time: float
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
        timezone: str,
        night_start: datetime.time = datetime.time(hour=20, minute=0),
        night_end: datetime.time = datetime.time(hour=8, minute=0),
        weekday_list: Iterable[DayOfWeek | int] = [
            DayOfWeek.MONDAY,
            DayOfWeek.TUESDAY,
            DayOfWeek.WEDNESDAY,
            DayOfWeek.THURSDAY,
            DayOfWeek.FRIDAY,
        ],
        weekend_list: Iterable[DayOfWeek | int] = [
            DayOfWeek.SATURDAY,
            DayOfWeek.SUNDAY,
        ],
        nw_threshold: float = 0.2,
    ) -> None:
        """Initialize the SleepMetrics dataclass.

        Stores the filtered night data and computes the sampling time.

        Args:
            data: Polars DataFrame containing the processed actigraphy data.
            timezone: The timezone of the input data. User defined based on location.
            night_start: The start time of the nocturnal interval.
            night_end: The end time of the nocturnal interval.
            nw_threshold: A threshold for the non-wear status, below which a night is
                considered valid. Expressed as a fraction (0.0 to 1.0).
            weekday_list: List of integers (1=Monday, 7=Sunday) representing weekdays.
                Default is [1, 2, 3, 4, 5] (Monday to Friday).
            weekend_list: List of integers representing weekend days
                Default is [6, 7] (Saturday and Sunday).

        Raises:
            ValueError: If there are no valid nights in the data.
        """
        self.sampling_time = data["time"].dt.time().diff()[1].total_seconds()
        self.night_data = _filter_nights(
            data, night_start, night_end, nw_threshold, timezone, self.sampling_time
        )
        if self.night_data.is_empty():
            raise ValueError("No valid nights found in the data.")

        self.weekdays = weekday_list
        self.weekend = weekend_list

    @property
    def sleep_duration(self) -> pl.Series:
        """Calculate total sleep duration in minutes.

        Sleep duration is calculated as the sum of sustained inactivity bouts,
        thus it is a measure of the anatomical clock, not based on wall-clock time.
        """
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
                        ).alias("sleep_duration"),
                    ]
                )
                .sort("night_date")
                .select("sleep_duration")
                .to_series()
            )
        return self._sleep_duration

    @property
    def time_in_bed(self) -> pl.Series:
        """Calculate total time in bed in minutes.

        Time in bed is calculated as the total duration of the SPT window(s),
        within the nocturnal window. This calculation is based on the
        anatomical clock, not wall-clock time.
        """
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

        If the nights cross a DST transition, the onset time reflects
        the new local time after the clock change.
        """
        if self._sleep_onset is None:
            self._sleep_onset = _compute_onset(self.night_data)
        return self._sleep_onset

    @property
    def sleep_wakeup(self) -> pl.Series:
        """Calculate the wakeup time in HH:MM format per night.

        Defined as the time when the last sleep period ends,
        within the nocturnal window.

        If the nights cross a DST transition, the wakeup time reflects
        the new local time after the clock change.
        """
        if self._sleep_wakeup is None:
            self._sleep_wakeup = _compute_wakeup(self.night_data)
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
        """The number of nights where WASO (wake after sleep onset) exceeds 30 minutes.

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
                pl.col("night_date").dt.weekday().is_in(list(self.weekdays))
            )
            if weekday_data.is_empty():
                self._weekday_midpoint = pl.Series(name="weekday_midpoint", values=[])
            else:
                weekday_onset = _compute_onset(weekday_data)
                weekday_wakeup = _compute_wakeup(weekday_data)
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
                pl.col("night_date").dt.weekday().is_in(list(self.weekend))
            )
            if weekend_data.is_empty():
                self._weekend_midpoint = pl.Series(name="weekend_midpoint", values=[])
            else:
                weekend_onset = _compute_onset(weekend_data)
                weekend_wakeup = _compute_wakeup(weekend_data)
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
                self._social_jetlag = _time_difference_abs_hours(
                    utils.compute_circular_mean_time(self.weekday_midpoint),  # type: ignore[arg-type] #covered by the is_empty() check above
                    utils.compute_circular_mean_time(self.weekend_midpoint),  # type: ignore[arg-type] #covered by the is_empty() check above
                )
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
    timezone: str,
    sampling_time: float,
) -> pl.DataFrame:
    """Find valid nights in the processed actigraphy data.

    A night is defined by the nocturnal interval (default is [20:00 - 08:00) ).
    Timestamps are first converted to UTC based on the provided timezone.
    The UTC conversion also keeps track of the offset from the initial local timezone.
    This offset is used to shift the nocturnal window hours.
    The processed data is filtered to only include this window and then valid nights
    are chosen when a night has a non-wear percentage below the specified threshold.

    Args:
        data: Polars dataframe containing the processed actigraphy data,
            including non-wear time.
        night_start: The start time of the nocturnal interval.
            Default is 20:00 (8 PM).
        night_end: The end time of the nocturnal interval.
            Default is 08:00 (8 AM).
        nw_threshold: A threshold for the non-wear status, below which a night is
            considered valid. Expressed as a percentage (0.0 to 1.0).
        timezone: The timezone of the input data. User defined based on location.
        sampling_time: The sampling time in seconds.

    Returns:
        A Polars DataFrame containing only the valid nights.

    """
    utc_night_data = _convert_to_utc(data, timezone)
    if utc_night_data["local_time"].is_null().any():
        utc_night_data = _fill_spring_forward_gaps(utc_night_data, sampling_time)
    if utc_night_data["utc_offset_hours"].diff().cast(pl.Int8).eq(-1).any():
        utc_night_data = _fill_fall_back(utc_night_data, sampling_time)

    if night_start > night_end:
        nocturnal_sleep = utc_night_data.filter(
            (pl.col("local_time").dt.time() >= night_start)
            | (pl.col("local_time").dt.time() < night_end)
        )
        nocturnal_sleep = nocturnal_sleep.with_columns(
            [
                pl.when(pl.col("local_time").dt.time() >= night_start)
                .then(pl.col("local_time").dt.date())
                .otherwise(pl.col("local_time").dt.date() - pl.duration(days=1))
                .alias("night_date")
            ]
        )
    else:
        nocturnal_sleep = utc_night_data.filter(
            (pl.col("local_time").dt.time() >= night_start)
            & (pl.col("local_time").dt.time() < night_end)
        )
        nocturnal_sleep = nocturnal_sleep.with_columns(
            pl.col("local_time").dt.date().alias("night_date")
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

    return nocturnal_sleep.join(valid_nights, on="night_date").sort("local_time")


def _convert_to_utc(data: pl.DataFrame, timezone: str) -> pl.DataFrame:
    """Convert local timestamps to UTC using the stored timezone.

    Args:
        data: Polars DataFrame with a 'time' column containing local timestamps.
        timezone: The timezone of the input data. User defined based on location.

    Returns:
        DataFrame with 'time' column converted to UTC. Also adds a column
            'utc_offset_hours' indicating the offset from UTC in hours.
    """
    ms_per_hour = 3_600_000
    data_with_tz = data.with_columns(
        [
            pl.col("time")
            .dt.replace_time_zone(timezone, ambiguous="earliest", non_existent="null")
            .alias("local_time")
        ]
    )

    return data_with_tz.with_columns(
        [
            pl.col("local_time").dt.convert_time_zone("UTC").alias("time"),
            (
                (
                    pl.col("time").dt.timestamp("ms")
                    - pl.col("local_time").dt.timestamp("ms")
                )
                / ms_per_hour
            ).alias("utc_offset_hours"),
        ]
    )


def _fill_spring_forward_gaps(
    utc_data: pl.DataFrame, sampling_time: float
) -> pl.DataFrame:
    """Fill missing timestamps caused by spring forward DST transitions.

    Args:
        utc_data: Night data after conversion to UTC timestamps.
        sampling_time: The expected sampling time in seconds.

    Returns:
        DataFrame with missing timestamps filled in.
    """
    null_mask = utc_data["local_time"].is_null()
    num_null_rows = int(null_mask.sum())

    time_delta = datetime.timedelta(seconds=sampling_time)
    time_columns = ["time", "local_time", "utc_offset_hours"]
    time_df_no_nulls = utc_data.select(time_columns).filter(~null_mask)
    data_df = utc_data.select(
        [col for col in utc_data.columns if col not in time_columns]
    )

    last_time = time_df_no_nulls["time"][-1]
    last_local_time = time_df_no_nulls["local_time"][-1]
    offset_after_dst = time_df_no_nulls["utc_offset_hours"][-1]

    extension_df = pl.DataFrame(
        {
            "time": [last_time + time_delta * i for i in range(1, num_null_rows + 1)],
            "local_time": [
                last_local_time + time_delta * i for i in range(1, num_null_rows + 1)
            ],
            "utc_offset_hours": [offset_after_dst] * num_null_rows,
        }
    )

    time_df_extended = pl.concat([time_df_no_nulls, extension_df])

    return pl.concat([time_df_extended, data_df], how="horizontal")


def _fill_fall_back(utc_data: pl.DataFrame, sampling_time: float) -> pl.DataFrame:
    """Fill the gap created by fall back DST transitions.

    Polars DST handling removes the "ambiguous" hour during fall back transitions,
    resulting in missing timestamps in the UTC data. This function identifies the
    transition point and fills in the missing timestamps to maintain
    consistent sampling intervals.

    One side effect is duplicated local times during the fall back hour,
    but this is necessary to preserve the correct timing in UTC.

    Args:
        utc_data: Night data after conversion to UTC timestamps.
        sampling_time: The expected sampling time in seconds.

    Returns:
        DataFrame with missing UTC timestamps filled in.
    """
    offset_diff = utc_data["utc_offset_hours"].diff().cast(pl.Int8)
    fall_back_idx = offset_diff.eq(-1).arg_true()[0]

    time_columns = ["time", "local_time", "utc_offset_hours"]
    data_columns = [col for col in utc_data.columns if col not in time_columns]

    time_df = utc_data.select(time_columns)
    data_df = utc_data.select(data_columns)

    time_before_gap = utc_data["time"][fall_back_idx - 1]
    time_after_gap = utc_data["time"][fall_back_idx]
    time_delta_seconds = (time_after_gap - time_before_gap).total_seconds()

    n_rows = int((time_delta_seconds - sampling_time) / sampling_time)

    time_delta = datetime.timedelta(seconds=sampling_time)
    last_time_before_gap = time_df["time"][fall_back_idx - 1]
    last_local_time_before_gap = time_df["local_time"][fall_back_idx - 1]
    offset_after_gap = time_df["utc_offset_hours"][fall_back_idx + 1]

    fill_df = pl.DataFrame(
        {
            "time": [
                last_time_before_gap + time_delta * i for i in range(1, n_rows + 1)
            ],
            "local_time": [
                last_local_time_before_gap + time_delta * i
                for i in range(1, n_rows + 1)
            ],
            "utc_offset_hours": [offset_after_gap] * n_rows,
        }
    )

    time_df_before = time_df[:fall_back_idx]
    time_df_after = time_df[fall_back_idx:]
    time_df_filled = pl.concat([time_df_before, fill_df, time_df_after])

    time_df_trimmed = time_df_filled[:-n_rows]

    return pl.concat([time_df_trimmed, data_df], how="horizontal")


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


def _compute_onset(df: pl.DataFrame) -> pl.Series:
    return (
        df.filter(pl.col("spt_periods"))
        .group_by("night_date")
        .agg(pl.col("local_time").min().alias("sleep_onset"))
        .sort("night_date")
        .select("sleep_onset")
        .to_series()
        .dt.time()
    )


def _compute_wakeup(df: pl.DataFrame) -> pl.Series:
    return (
        df.filter(pl.col("spt_periods"))
        .group_by("night_date")
        .agg(pl.col("local_time").max().alias("sleep_wakeup"))
        .sort("night_date")
        .select("sleep_wakeup")
        .to_series()
        .dt.time()
    )


def _time_difference_abs_hours(time1: datetime.time, time2: datetime.time) -> float:
    """Calculate absolute difference between two times in hours.

    Converts datetime.time objects to hours since midnight, then finds the absolute
    difference between the two values.If the difference is greater than 12 hours,
    it is adjusted to reflect the shorter interval across midnight.

    Args:
        time1: A datetime.time object.
        time2: A datetime.time object.

    Returns:
        The absolute difference between the two times in hours, as a float.
    """
    rel_time1 = time1.hour + time1.minute / 60 + time1.second / 3600
    rel_time2 = time2.hour + time2.minute / 60 + time2.second / 3600

    diff = abs(rel_time2 - rel_time1)

    if diff > 12:
        diff = 24 - diff

    return diff


def extract_simple_statistics(
    sleep_metrics: SleepMetrics,
) -> pl.DataFrame:
    """Extract simple statistics from sleep metrics.

    Args:
        sleep_metrics: An instance of SleepMetrics containing computed sleep metrics.

    Returns:
        A DataFrame containing the extracted statistics.
    """
    df = pl.DataFrame(
        {
            "mean_sleep_duration": sleep_metrics.sleep_duration.mean(),
            "sd_sleep_duration": sleep_metrics.sleep_duration.std(),
            "mean_time_in_bed": sleep_metrics.time_in_bed.mean(),
            "sd_time_in_bed": sleep_metrics.time_in_bed.std(),
            "mean_sleep_efficiency": sleep_metrics.sleep_efficiency.mean(),
            "sd_sleep_efficiency": sleep_metrics.sleep_efficiency.std(),
            "mean_waso": sleep_metrics.waso.mean(),
            "sd_waso": sleep_metrics.waso.std(),
            "mean_num_awakenings": sleep_metrics.num_awakenings.mean(),
            "sd_num_awakenings": sleep_metrics.num_awakenings.std(),
            "mean_sleep_onset_time": utils.compute_circular_mean_time(
                sleep_metrics.sleep_onset
            ),
            "sd_sleep_onset_time": utils.compute_circular_sd_time(
                sleep_metrics.sleep_onset
            ),
            "mean_wakeup_time": utils.compute_circular_mean_time(
                sleep_metrics.sleep_wakeup
            ),
            "sd_wakeup_time": utils.compute_circular_sd_time(
                sleep_metrics.sleep_wakeup
            ),
            "mean_midpoint_sleep": utils.compute_circular_mean_time(
                sleep_metrics.sleep_midpoint
            ),
            "sd_midpoint_sleep": utils.compute_circular_sd_time(
                sleep_metrics.sleep_midpoint
            ),
            "mean_weekday_midpoint": utils.compute_circular_mean_time(
                sleep_metrics.weekday_midpoint
            ),
            "sd_weekday_midpoint": utils.compute_circular_sd_time(
                sleep_metrics.weekday_midpoint
            ),
            "mean_weekend_midpoint": utils.compute_circular_mean_time(
                sleep_metrics.weekend_midpoint
            ),
            "sd_weekend_midpoint": utils.compute_circular_sd_time(
                sleep_metrics.weekend_midpoint
            ),
        }
    )

    return df
