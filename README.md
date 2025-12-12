# `noctsleepy`  

A Python package for computing nocturnal sleep metrics from processed actigraphy data.

[![Build](https://github.com/childmindresearch/noctsleepy/actions/workflows/test.yaml/badge.svg?branch=main)](https://github.com/childmindresearch/noctsleepy/actions/workflows/test.yaml?query=branch%3Amain)
[![codecov](https://codecov.io/gh/childmindresearch/noctsleepy/branch/main/graph/badge.svg?token=22HWWFWPW5)](https://codecov.io/gh/childmindresearch/noctsleepy)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
![stability-stable](https://img.shields.io/badge/stability-experimental-orange.svg)
[![MIT License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/childmindresearch/noctlseepy/blob/main/LICENSE)
[![pages](https://img.shields.io/badge/api-docs-blue)](https://childmindresearch.github.io/noctsleepy/noctsleepy.html)

## Overview

`noctsleepy` is a Python-based toolbox for computing comprehensive sleep metrics from processed actigraphy data. Sleep metrics are computed from a user-defined nocturnal window, thus the impact of naps/secondary sleep can be removed if needed. The package handles timezone-aware processing, including proper daylight saving time (DST) transitions, and computes both individual night metrics and summary statistics across multiple nights.

### Key Features

- **Comprehensive Sleep Metrics**: Computes sleep duration, time in bed, sleep efficiency, wake after sleep onset (WASO), number of awakenings, sleep onset/wakeup times, and sleep midpoints.
- **DST-Aware Processing**: Properly handles daylight saving time transitions using IANA timezone database, ensuring accurate sleep timing metrics during time changes.
- **Summary Statistics**: Automatically computes mean and standard deviation for all requested metrics.
- **Flexible Nocturnal Windows**: Users can define custom start and end times for the nocturnal interval.
- **Non-wear Filtering**: Filters out nights with excessive non-wear time based on user-defined thresholds.

## Installation

Install the `noctsleepy` package from PyPI via:

```sh
pip install noctsleepy
```

## Quick Start

`noctsleepy` provides two flexible interfaces: a command-line tool for direct execution and an importable Python library.

### Using noctsleepy through the command-line:

#### Basic usage:
```sh
noctsleepy /path/to/data.csv america/new_york
```

#### Custom parameters:
```sh
noctsleepy /path/to/data.csv europe/london \
  --night-start 22:00 \
  --night-end 07:00 \
  --nw-threshold 0.3 \
  --metrics sleep_duration \
  --metrics sleep_timing
```

#### For a full list of command line arguments:
```sh
noctsleepy --help
```

### Using noctsleepy through a Python script or notebook:

```python
import noctsleepy
import pathlib
import datetime

# Define input file path
input_path = pathlib.Path('/path/to/your/data.csv')

# Compute all sleep metrics
sleep_metrics = noctsleepy.compute_sleep_metrics(
    input_data=input_path,
    timezone='America/New_York'
)

# Compute custom pipeline
custom_metrics = noctsleepy.compute_sleep_metrics(    
    input_data=input_path,
    timezone='America/Toronto',
    night_start=datetime.time(22, 0),
    night_end=datetime.time(6, 0),
    nw_threshold=0.15,
    selected_metrics=['sleep_continuity', 'sleep_timing']
)

# Access computed metrics
sleep_duration = sleep_metrics.sleep_duration
sleep_onset = sleep_metrics.sleep_onset
sleep_efficiency = sleep_metrics.sleep_efficiency
```

## Input Data Requirements

`noctsleepy` requires processed actigraphy data with the following columns:
- `time`: Timestamps for each data point, it should be a singular sampling rate.
- `sib_periods`: Boolean indicating sustained inactivity bouts (sleep detection).
- `spt_periods`: Boolean indicating sleep period time windows.
- `nonwear_status`: Boolean indicating non-wear periods.

The input data should ideally be processed with [`wristpy`](https://github.com/childmindresearch/wristpy) or have a compatible output format. Supported input formats include CSV and Parquet files.

## Computed Sleep Metrics
All metrics are computed only during the defined nocturnal window, any sleep that occurs outside of this window is ignored.

### Sleep Duration Metrics
- **sleep_duration**: Total sleep time in minutes (sum of sustained inactivity bouts within sleep period time).
- **time_in_bed**: Total time in bed in minutes (duration of sleep period time windows).

### Sleep Continuity Metrics
- **sleep_efficiency**: Ratio of total sleep time to time in bed, as a percentage.
- **waso**: Wake After Sleep Onset, in minutes.
- **num_awakenings**: Number of awakening episodes during the sleep period.
- **waso_30**: Number of nights with WASO exceeding 30 minutes (normalized to 30-day protocol).

### Sleep Timing Metrics
- **sleep_onset**: Time when the first sleep period starts (HH:MM format).
- **sleep_wakeup**: Time when the last sleep period ends (HH:MM format).
- **sleep_midpoint**: Midpoint of the sleep period (HH:MM format).
- **weekday_midpoint**: Average sleep midpoint on weekdays (Monday-Friday).
- **weekend_midpoint**: Average sleep midpoint on weekends (Saturday-Sunday).
- **social_jetlag**: Absolute difference between weekend and weekday midpoints, in hours.

### Summary Statistics

For each metric, `noctsleepy` automatically computes:
- **Mean**: Average value across all valid nights
- **Standard Deviation**: Measure of variability across nights
  - Duration metrics use standard statistics
  - Time-based metrics use circular statistics to properly handle times crossing midnight

## Handling Timezones and Daylight Saving Time

`noctsleepy` requires users to specify a timezone-aware location using IANA timezone database names (e.g., `America/New_York`, `Europe/London`). The package properly handles DST transitions:

### Wall-Clock vs. Anatomical Clock

- **Wall-clock metrics** (sleep onset, wakeup, midpoint): Reported based on the local time displayed on a clock
- **Anatomical clock metrics** (sleep duration, time in bed, WASO, sleep efficiency): Account for actual elapsed time, including DST changes.

**Example**: During a "fall back" DST transition, if someone sleeps from 10:00 PM to 6:00 AM (wall-clock time), their sleep duration will reflect the additional hour gained during the transition (9 hours of actual sleep), while the onset/wakeup times show the wall-clock times (10:00 PM and 6:00 AM).


### Supported Timezones

`noctsleepy` supports timezones across many regions, including:
- **North America**: `us_eastern`, `us_central`, `us_mountain`, `us_pacific`, `us_alaska`, `us_hawaii`, `canada_eastern`, `mexico_central`, etc.
- **Europe**: `europe_london`, `europe_paris`, `europe_berlin`, `europe_rome`, `europe_moscow`, etc.
- **Asia**: `asia_tokyo`, `asia_shanghai`, `asia_kolkata`, `asia_singapore`, `asia_dubai`, etc.
- **South America**: `brazil_eastern`, `argentina`, `chile`, `colombia`, etc.
- **Africa**: `africa_cairo`, `africa_johannesburg`, `africa_nairobi`, `africa_lagos`, etc.
- **Oceania**: `australia_sydney`, `australia_melbourne`, `new_zealand`, `pacific_fiji`, etc.

For a complete list of supported timezones, run:
```sh
noctsleepy compute-metrics --help
```

## Output Format

Results are automatically saved as a JSON file in the same directory as the input file, containing both per night statistics and summary statistics. 


## Support

For questions, bug reports, or feature requests, please [open an issue](https://github.com/childmindresearch/noctsleepy/issues) on GitHub.