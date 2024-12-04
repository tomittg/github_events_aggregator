import logging
import gzip
import requests
from requests.adapters import HTTPAdapter
from pathlib import Path

from github_events_aggregator.config.config import get_config

logger = logging.getLogger('pipeline.extract')


def download_data():
    """
    Downloads the GitHub events for the parameters specified in the config file, which are:
        - base_url: URL from the website we are downloading the data from
        - year: year from which we are downloading the data
        - month: month from which we are downloading the data
    The function iterates through each hour and day of the month to collect all the data.
    It stores the downloaded data into a temporary folder called 'cached_data'

    """
    sess = requests.Session()
    sess.mount('https://', HTTPAdapter(max_retries=20))

    config = get_config()
    base_url = config['data_source']['base_url']
    year = config['data_source']['year']
    month = config['data_source']['month']

    logger.info(f'Starting the data download from {base_url}')

    base_url = f'{base_url}/{year}-{month}-{{day:02d}}-{{hour}}.json.gz'

    for day in range(1, 32):
        for hour in range(24):
            try:
                logger.info(f'Downloading data for {year}-{month}-{day}:{hour}h...')
                file_path = Path(f'cached_data/{year}-{month}-{day}-{hour}.json')
                if file_path.exists():
                    logger.info(f'Data already downloaded')
                else:
                    url = base_url.format(day=day, hour=hour)
                    response = sess.get(url)
                    response.raise_for_status()
                    compressed = response.content
                    decompressed = gzip.decompress(compressed)
                    decoded = decompressed.decode('utf-8').replace("\u2028", "")
                    _add_to_cache(file_path, decoded)
                logger.info(f'Data successfully extracted')
            except requests.exceptions.HTTPError as e:
                logger.exception(e)


def _preprocess_data(raw_data: bytes):
    decompressed = gzip.decompress(raw_data)
    decoded = decompressed.decode('utf-8').replace("\u2028", "")
    events_hourly = decoded.splitlines()
    return events_hourly


def _add_to_cache(file_path: Path, events_hourly: str):
    logger.info(f'Adding file to cache at {file_path}...')
    file_path.parent.mkdir(parents=True, exist_ok=True)
    file_path.touch()
    with file_path.open('w', encoding='utf-8') as file:
        file.write(events_hourly)
    logger.info('Cache successfully updated')