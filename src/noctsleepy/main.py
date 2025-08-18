"""Python based runner for noctscleepy."""

import datetime
import pathlib
import typing

from noctsleepy.io import readers
from noctsleepy.processing import sleep_variables

SLEEP_METRIC_CATEGORIES = typing.Literal[
    "sleep_duration", "sleep_continuity", "sleep_timing"
]


def compute_sleep_metrics(
    input_data: pathlib.Path | str,
    night_start: typing.Optional[datetime.time] = None,
    night_end: typing.Optional[datetime.time] = None,
    nw_threshold: float = 0.2,
    selected_metrics: typing.Optional[list[SLEEP_METRIC_CATEGORIES]] = None,
) -> sleep_variables.SleepMetrics:
    """Compute sleep metrics from the provided data file.

    Users can specify the start and end times of the night to filter the data,
    a non-wear threshold, and the metrics they want to compute.

    The output is saved to csv format.

    Args:
        input_data: Path to the input data file (CSV or Parquet).
        night_start: Start time of the nocturnal interval. If None, defaults to 20:00.
        night_end: End time of the nocturnal interval.  If None, defaults to 08:00.
        nw_threshold: Non-wear threshold, below which a night is considered valid.
            If None, defaults to 0.2.
        selected_metrics: Specific metrics to compute.
             If None, all metrics are computed.

    Returns:
        An instance of SleepMetrics containing the computed metrics.
    """
    if night_start is None:
        night_start = datetime.time(hour=20, minute=0)
    if night_end is None:
        night_end = datetime.time(hour=8, minute=0)

    data = readers.read_wristpy_data(pathlib.Path(input_data))
    sleep_data = sleep_variables.SleepMetrics(
        data, night_start, night_end, nw_threshold
    )

    metric_mapping = {
        "sleep_duration": ["sleep_duration", "time_in_bed"],
        "sleep_continuity": ["waso", "sleep_efficiency", "num_awakenings", "waso_30"],
        "sleep_timing": ["weekday_midpoint", "weekend_midpoint"],
    }

    if selected_metrics is None:
        selected_metrics = ["sleep_duration", "sleep_continuity", "sleep_timing"]

    metrics_to_compute = []
    for category in selected_metrics:
        if category in metric_mapping:
            metrics_to_compute.extend(metric_mapping[category])

    for prop in metrics_to_compute:
        if hasattr(sleep_data, prop):
            getattr(sleep_data, prop)

    output_file = pathlib.Path(input_data).with_name(
        pathlib.Path(input_data).stem + "_sleep_metrics.csv"
    )
    sleep_data.save_to_csv(output_file)

    return sleep_data
