import logging
import time

from github_events_aggregator.extract import download_data
from github_events_aggregator.transform import aggregate_data
from github_events_aggregator.load import load_data

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger('pipeline')


def run_pipeline():
    start_time = time.time()
    try:
        logger.info('Starting pipeline...')
        download_data()
        aggregated_data = aggregate_data()
        load_data(aggregated_data)
        logger.info('Pipeline successfully completed')
    except Exception as e:
        logger.exception(e)
    finally:
        total_time = time.time() - start_time
        logger.info(f'Total pipeline execution time: {total_time:.2f} seconds')


if __name__ == '__main__':
    run_pipeline()
