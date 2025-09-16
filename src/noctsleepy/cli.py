"""CLI for noctsleepy."""

import datetime
import pathlib
from enum import Enum
from typing import List, Optional

import typer

from noctsleepy import main

app = typer.Typer(
    name="noctsleepy",
    rich_markup_mode="rich",
    help="A Python toolbox for computing nocturnal sleep metrics.",
    epilog="Please report issues at "
    "https://github.com/childmindresearch/noctsleepy/issues.",
)


class SleepMetricCategory(str, Enum):
    """Sleep metric categories."""

    sleep_duration = "sleep_duration"
    sleep_continuity = "sleep_continuity"
    sleep_timing = "sleep_timing"


@app.command(
    name="compute-metrics",
    help="Compute sleep metrics from actigraphy data and save results as JSON.",
    epilog="Results are automatically saved as a JSON file in the same directory as the input file.",
)
def compute_metrics(
    input_data: pathlib.Path = typer.Argument(
        ...,
        exists=True,
        file_okay=True,
        dir_okay=False,
        readable=True,
        resolve_path=True,
        help="Path to the input data file (CSV or Parquet).",
    ),
    night_start: Optional[str] = typer.Option(
        None,
        "--night-start",
        help="Start time of the nocturnal interval in HH:MM format. Defaults to 20:00.",
    ),
    night_end: Optional[str] = typer.Option(
        None,
        "--night-end",
        help="End time of the nocturnal interval in HH:MM format. Defaults to 08:00.",
    ),
    nw_threshold: float = typer.Option(
        0.2,
        "--nw-threshold",
        help="Non-wear threshold (0.0-1.0), below which a night is considered valid.",
        min=0.0,
        max=1.0,
    ),
    selected_metrics: Optional[List[SleepMetricCategory]] = typer.Option(
        None,
        "--metrics",
        help="Specific metric categories to compute. If not specified, all metrics are computed.",
    ),
) -> None:
    """Compute sleep metrics from actigraphy data.

    This command processes actigraphy data and computes various sleep metrics
    including sleep duration, continuity, and timing measures. Results are
    saved as a JSON file in the same directory as the input file.
    """
    # Parse time strings if provided
    night_start_time = None
    if night_start:
        try:
            hour, minute = map(int, night_start.split(":"))
            night_start_time = datetime.time(hour=hour, minute=minute)
        except ValueError:
            typer.echo(
                f"Error: Invalid time format for night_start: {night_start}. Use HH:MM format."
            )
            raise typer.Exit(1)

    night_end_time = None
    if night_end:
        try:
            hour, minute = map(int, night_end.split(":"))
            night_end_time = datetime.time(hour=hour, minute=minute)
        except ValueError:
            typer.echo(
                f"Error: Invalid time format for night_end: {night_end}. Use HH:MM format."
            )
            raise typer.Exit(1)

    # Convert enum values to strings if provided
    metrics_list = None
    if selected_metrics:
        metrics_list = [metric.value for metric in selected_metrics]

    try:
        # Call the main computation function
        sleep_metrics = main.compute_sleep_metrics(
            input_data=input_data,
            night_start=night_start_time,
            night_end=night_end_time,
            nw_threshold=nw_threshold,
            selected_metrics=metrics_list,
        )

        output_file = input_data.with_name(input_data.stem + "_sleep_metrics.json")
        typer.echo(f"✅ Sleep metrics computed successfully!")
        typer.echo(f"📄 Results saved to: {output_file}")

    except Exception as e:
        typer.echo(f"❌ Error computing sleep metrics: {str(e)}")
        raise typer.Exit(1)
