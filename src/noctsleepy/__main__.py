"""Main function for wristpy."""

from noctsleepy import cli


def run_main() -> None:
    """Main entry point to wristpy."""
    cli.app()


if __name__ == "__main__":
    cli.app()
