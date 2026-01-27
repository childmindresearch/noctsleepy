"""CLI for noctsleepy."""

import datetime
import pathlib
from enum import Enum
from typing import Annotated, List

import typer

from noctsleepy import main, timezones

app = typer.Typer(
    name="noctsleepy",
    rich_markup_mode="rich",
    help="A Python toolbox for computing nocturnal sleep metrics.",
    epilog="Please report issues at "
    "https://github.com/childmindresearch/noctsleepy/issues.",
)


def parse_time(value: str) -> datetime.time:
    """Parse time string in HH:MM format to datetime.time object.

    Args:
        value: Time string in HH:MM format.

    Returns:
        A datetime.time object.

    Raises:
        typer.BadParameter: If the input string is not in the correct format.
    """
    try:
        return datetime.datetime.strptime(value, "%H:%M").time()
    except ValueError:
        raise typer.BadParameter(
            f"Invalid time format: {value}. Expected HH:MM format (e.g., 20:00, 08:30)."
        )


class SleepMetricCategory(str, Enum):
    """Sleep metric categories."""

    sleep_duration = "sleep_duration"
    sleep_continuity = "sleep_continuity"
    sleep_timing = "sleep_timing"


@app.command(
    name="compute-metrics",
    help="Compute sleep metrics from actigraphy data and save results as JSON.",
    epilog=(
        "Results are automatically saved as a JSON file "
        "in the same directory as the input file."
    ),
)
def compute_metrics(
    input_data: Annotated[
        pathlib.Path,
        typer.Argument(
            exists=True,
            resolve_path=True,
            help="Path to the input data file (CSV or Parquet). "
            "This file should contain data from processed actigraphy devices. "
            "Ideally, the raw actigraphy data was processed with `wristpy`, "
            "or at the minimum, must have a compatible output format.",
        ),
    ],
    timezone: Annotated[
        timezones.CommonTimezones,
        typer.Argument(
            help="Geographic timezone location for the data collection site. "
            "Used for DST-aware processing. "
            "Must match the IANA timezone list provided in `timezones.py`.",
            case_sensitive=False,
            show_choices=True,
        ),
    ],
    night_start: Annotated[
        str,
        typer.Option(
            "--night-start",
            "-s",
            help="Start time of the nocturnal interval in HH:MM format. "
            "If not provided, defaults to 20:00.",
            callback=lambda value: parse_time(value) if value else None,
        ),
    ] = "20:00",
    night_end: Annotated[
        str,
        typer.Option(
            "--night-end",
            "-e",
            help="End time of the nocturnal interval in HH:MM format. "
            "If not provided, defaults to 08:00.",
            callback=lambda value: parse_time(value) if value else None,
        ),
    ] = "08:00",
    nw_threshold: Annotated[
        float,
        typer.Option(
            "--nw-threshold",
            "-t",
            help="Non-wear threshold fraction (0.0-1.0), "
            "below which a night is considered valid.",
            min=0.0,
            max=1.0,
        ),
    ] = 0.2,
    selected_metrics: Annotated[
        List[SleepMetricCategory] | None,
        typer.Option(
            "--metrics",
            "-m",
            help="Specific metric categories to compute. "
            "If not specified, all metrics are computed. "
            "Multiple categories can be specified by repeating the option. "
            "E.g., --metrics sleep_duration --metrics sleep_timing. ",
            case_sensitive=False,
            show_choices=True,
        ),
    ] = None,
    longest_sleep: Annotated[
        bool,
        typer.Option(
            "--longest-sleep",
            "-l",
            help="This parameter can be used to analyze sleep metrics from "
            "only the longest continuous sleep window per night.",
        ),
    ] = False,
) -> None:
    """Compute sleep metrics from actigraphy data.

    This command processes actigraphy data and computes various sleep metrics
    including sleep duration, continuity, and timing measures. Results are
    saved as a JSON file in the same directory as the input file.
    """
    main.compute_sleep_metrics(
        input_data=input_data,
        timezone=timezone,
        night_start=night_start,  # type: ignore[arg-type] #Covered by parse_time callback
        night_end=night_end,  # type: ignore[arg-type] #Covered by parse_time callback
        nw_threshold=nw_threshold,
        selected_metrics=[metric.value for metric in selected_metrics]
        if selected_metrics is not None
        else None,
        only_longest_sleep=longest_sleep,
    )
