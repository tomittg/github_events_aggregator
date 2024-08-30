import logging

from github_events_aggregator.config.config import get_config

logger = logging.getLogger('pipeline.load')


def load_data(data):
    """
    Loads each table of aggregated data to the output directory set in the config file

    :param dict[str, pyspark.sql.DataFrame] data: Dictionary with the name of the file and data to be loaded
    """
    config = get_config()

    logger.info(f'Starting load process...')
    for name, df in data.items():
        df.write.csv(f'{config['output_dir']}/{name}.csv', header=True, mode='overwrite')
        df.write.parquet(f'{config['output_dir']}/{name}.parquet', mode='overwrite')
    logger.info('Loading successfully completed')
