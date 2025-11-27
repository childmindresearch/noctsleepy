"""Python based runner for noctscleepy."""

import datetime
import itertools
import json
import pathlib
from typing import Iterable, Literal, Optional

from noctsleepy import timezones
from noctsleepy.io import readers
from noctsleepy.processing import sleep_variables

SLEEP_METRIC_CATEGORIES = Literal["sleep_duration", "sleep_continuity", "sleep_timing"]

METRIC_MAPPING: dict[SLEEP_METRIC_CATEGORIES, list[str]] = {
    "sleep_duration": ["sleep_duration", "time_in_bed"],
    "sleep_continuity": ["waso", "sleep_efficiency", "num_awakenings", "waso_30"],
    "sleep_timing": [
        "sleep_onset",
        "sleep_wakeup",
        "sleep_midpoint",
        "weekday_midpoint",
        "weekend_midpoint",
        "social_jetlag",
    ],
}


def compute_sleep_metrics(
    input_data: pathlib.Path | str,
    timezone: str,
    night_start: Optional[datetime.time] = None,
    night_end: Optional[datetime.time] = None,
    nw_threshold: float = 0.2,
    selected_metrics: Optional[Iterable[SLEEP_METRIC_CATEGORIES]] = None,
) -> sleep_variables.SleepMetrics:
    """Compute sleep metrics from the provided data file.

    The input data file contains time-series data from processed actigraphy devices.
    Ideally, the raw actigraphy data is processed with `wristpy`, or at the minimum
    must have a compatible output format.

    Users can specify the start and end times of the night to filter the data,
    a non-wear threshold, and the metrics they want to compute.

    **Handling Timezones and Daylight Savings Time (DST)**
    Users must provide a location-aware timezone that conforms to the IANA timezone
    database. During/after DST transitions, the nocturnal interval window will be
    defined based on the wall-clock time.

    Certain metrics, such as sleep onset, sleep wakeup, and sleep midpoint,
    are calculated based solely on the wall clock time within the nocturnal interval.
    However, sleep duration, time in bed, sleep efficiency, WASO are computed based
    on the anatomical clock, accounting for any DST changes.
    For example, if a night includes a "fall back" transition, the sleep duration
    will reflect the additional hour gained during the transition.


    The output is saved to csv format.

    Args:
        input_data: Path to the input data file (CSV or Parquet).
        timezone: Timezone aware location of the input data. Used for Daylight
            Savings Time processing, this must be an IANA timezone string.
        night_start: Start time of the nocturnal interval. If None, defaults to 20:00.
        night_end: End time of the nocturnal interval.  If None, defaults to 08:00.
        nw_threshold: Non-wear threshold, below which a night is considered valid.
            If None, defaults to 0.2.
        selected_metrics: Specific metrics to compute.
             If None, all metrics are computed.

    Returns:
        An instance of SleepMetrics containing the computed metrics.

    Raises:
        ValueError: If the provided timezone is not valid.
    """
    if timezone not in list(timezones.CommonTimezones.__args__):  # type: ignore[attr-defined] #Valid attribute for Literal
        raise ValueError(f"Invalid timezone: {timezone}")
    if night_start is None:
        night_start = datetime.time(hour=20, minute=0)
    if night_end is None:
        night_end = datetime.time(hour=8, minute=0)
    output_file = pathlib.Path(input_data).with_name(
        pathlib.Path(input_data).stem + "_sleep_metrics.json"
    )

    data = readers.read_wristpy_data(pathlib.Path(input_data))
    sleep_data = sleep_variables.SleepMetrics(
        data=data,
        night_start=night_start,
        night_end=night_end,
        nw_threshold=nw_threshold,
        timezone=timezone,
    )

    if selected_metrics is None:
        selected_metrics = ["sleep_duration", "sleep_continuity", "sleep_timing"]

    metrics_to_compute = itertools.chain.from_iterable(
        METRIC_MAPPING[metric] for metric in selected_metrics
    )

    sleep_metrics_dict = sleep_data.save_to_json(metrics_to_compute)

    summary_stats = sleep_variables.extract_simple_statistics(sleep_data)

    output_file.write_text(
        json.dumps(
            {
                "sleep_metrics": sleep_metrics_dict,
                "summary_statistics": summary_stats,
            },
            indent=4,
            default=str,
        )
    )

    return sleep_data
