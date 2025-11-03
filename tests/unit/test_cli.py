"""Unit tests for the CLI interface of noctsleepy."""

import datetime
import pathlib

import pytest
import pytest_mock
import typer
from typer import testing

from noctsleepy import cli, main


@pytest.fixture
def create_typer_cli_runner() -> testing.CliRunner:
    """Create a Typer CLI runner."""
    return testing.CliRunner()


def test_main_default(
    mocker: pytest_mock.MockerFixture,
    sample_csv_data: pathlib.Path,
    create_typer_cli_runner: testing.CliRunner,
) -> None:
    """Test the CLI with default parameters."""
    mock_compute = mocker.patch.object(main, "compute_sleep_metrics")

    result = create_typer_cli_runner.invoke(
        cli.app,
        [str(sample_csv_data)],
    )

    assert result.exit_code == 0, f"CLI exited with error: {result.output}"
    mock_compute.assert_called_once_with(
        input_data=sample_csv_data,
        timezone="UTC",
        night_start=datetime.time(hour=20, minute=0),
        night_end=datetime.time(hour=8, minute=0),
        nw_threshold=0.2,
        selected_metrics=None,
    )


def test_main_custom_params(
    mocker: pytest_mock.MockerFixture,
    sample_csv_data: pathlib.Path,
    create_typer_cli_runner: testing.CliRunner,
) -> None:
    """Test the CLI with custom parameters."""
    mock_compute = mocker.patch.object(main, "compute_sleep_metrics")

    result = create_typer_cli_runner.invoke(
        cli.app,
        [
            str(sample_csv_data),
            "--night-start",
            "21:00",
            "--night-end",
            "07:00",
            "--nw-threshold",
            "0.3",
            "--metrics",
            "sleep_duration",
            "--metrics",
            "sleep_timing",
        ],
    )

    assert result.exit_code == 0, f"CLI exited with error: {result.output}"
    mock_compute.assert_called_once_with(
        input_data=sample_csv_data,
        timezone="UTC",
        night_start=datetime.time(hour=21, minute=0),
        night_end=datetime.time(hour=7, minute=0),
        nw_threshold=0.3,
        selected_metrics=["sleep_duration", "sleep_timing"],
    )


def test_parse_time_bad_input() -> None:
    """Test the parse_time function with bad input."""
    with pytest.raises(
        typer.BadParameter,
    ):
        cli.parse_time("20-00")
