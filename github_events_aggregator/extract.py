import gzip
import requests
from requests.adapters import HTTPAdapter

from github_events_aggregator.config.config import get_config


def download_data():
    sess = requests.Session()
    sess.mount('https://', HTTPAdapter(max_retries=20))

    config = get_config()
    base_url = config['data_source']['base_url']
    year = config['data_source']['year']
    month = config['data_source']['month']

    base_url = f'{base_url}/{year}-{month}-{{day:02d}}-{{hour}}.json.gz'
    json_list = []
    for day in range(1, 32):
        for hour in range(24):
            url = base_url.format(day=day, hour=hour)
            try:
                print(f'Extracting day {day}, hour {hour}')  # pending to use logger instead of prints
                response = sess.get(url)
                response.raise_for_status()
                unzipped = gzip.decompress(response.content)
                decoded = unzipped.decode('utf-8').replace("\u2028", "")
                events_hourly = decoded.splitlines()
                json_list.extend(events_hourly)
            except requests.exceptions.HTTPError as e:
                print(e)
    return json_list


