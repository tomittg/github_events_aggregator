# github_events_aggregator
Python package that extracts GitHub events from https://www.gharchive.org/, 
performs some aggregation operations over them and loads the result in csv and parquet formats.
Uses the pyspark package to fulfil its job.

## Table of Contents

- [Installation](#installation)
- [Usage](#usage)
- [Features](#features)
- [Project Structure](#project-structure)

## Installation

### Prerequisites

- Python 3.x (Tested in 3.12)
- Apache Spark

### Adding the package

An easy way to install it is provided, by just running the following in the python environment you want to use the package from

```shell
pip install <path-to-package>/dist/github_events_aggregator-0.1.1-py3-none-any.whl
```

This will automatically install all needed Python dependencies.


## Usage

There's 2 functions in total, one for running the job and one to clear the cached data


```python
import github_events_aggregator

github_events_aggregator.run_pipeline()
github_events_aggregator.clear_cache()
```
This will get the configuration set up in ~/config/config.yaml, which corresponds to the https://www.gharchive.org/ URL 
to download the data, and January 2024 for the month it will download.
Then with clear_cache() the downloaded data will be removed. As long as the data is not removed, subsequent calls to 
run the pipeline will reuse the downloaded data if the same year and month is chosen.


## Features

 - Data Aggregation: Aggregates GitHub event data for repositories and users.
 - Output Formats: Supports writing the aggregated data in both CSV and Parquet formats.

## Project structure

```
github_events_aggregator
├── README.md
├── dist
│   └── github_events_aggregator-0.1.1-py3-none-any.whl
├── github_events_aggregator
│   ├── __init__.py
│   ├── config
│   │   ├── __init__.py
│   │   ├── config.py
│   │   └── config.yaml
│   ├── extract.py
│   ├── load.py
│   ├── output
│   ├── pipeline.py
│   └── transform.py
└── setup.py
```
