import logging
import gzip
import requests
from requests.adapters import HTTPAdapter

from github_events_aggregator.config.config import get_config

logger = logging.getLogger('pipeline.extract')


def download_data():
    """
    Downloads the GitHub events for the parameters specified in the config file, which are:
        - base_url: URL from the website we are downloading the data from
        - year: year from which we are downloading the data
        - month: month from which we are downloading the data
    The function iterates through each hour and day of the month to collect all the data

    :return: GitHub events for a given year and month
    :rtype: list[str]
    """
    sess = requests.Session()
    sess.mount('https://', HTTPAdapter(max_retries=20))

    config = get_config()
    base_url = config['data_source']['base_url']
    year = config['data_source']['year']
    month = config['data_source']['month']

    base_url = f'{base_url}/{year}-{month}-{{day:02d}}-{{hour}}.json.gz'
    json_list = []

    logger.info(f'Starting the data download from {base_url}')
    for day in range(1, 32):
        for hour in range(24):
            url = base_url.format(day=day, hour=hour)
            try:
                logger.info(f'Downloading data for {year}-{month}-{day}:{hour}h...')  # pending to use logger instead of prints
                response = sess.get(url)
                response.raise_for_status()
                unzipped = gzip.decompress(response.content)
                decoded = unzipped.decode('utf-8').replace("\u2028", "")
                events_hourly = decoded.splitlines()
                json_list.extend(events_hourly)
                logger.info(f'Data successfully downloaded and added to json list')
            except requests.exceptions.HTTPError as e:
                logger.exception(e)
    return json_list


