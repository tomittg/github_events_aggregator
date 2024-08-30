from github_events_aggregator.extract import download_data
from github_events_aggregator.transform import aggregate_data
from github_events_aggregator.load import load_data


def run_pipeline():
    source_data = download_data()
    aggregated_data = aggregate_data(source_data)
    load_data(aggregated_data)


if __name__ == '__main__':
    run_pipeline()
