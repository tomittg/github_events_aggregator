from github_events_aggregator.config.config import get_config


def load_data(data: dict):
    config = get_config()

    for name, df in data.items():
        df.write.csv(f'{config['output_dir']}/{name}.csv', header=True, mode='overwrite')
        df.write.parquet(f'{config['output_dir']}/{name}.parquet', mode='overwrite')
