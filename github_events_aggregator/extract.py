import requests
import gzip
from pyspark.sql import SparkSession


from github_events_aggregator.config.config import get_config


def download_data():
    spark = SparkSession.builder \
        .appName("JSON to DataFrame") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.instances", "4") \
        .getOrCreate()

    config = get_config()
    base_url = config['data_source']['base_url']
    year = config['data_source']['year']
    month = config['data_source']['month']

    base_url = f'{base_url}/{year}-{month}-{{day:02d}}-{{hour}}.json.gz'

    df = None

    for day in range(1, 32):
        for hour in range(24):
            url = base_url.format(day=day, hour=hour)
            try:
                print(f'Extracting day {day}, hour {hour}')  # pending to use logger instead of prints
                response = requests.get(url)
                response.raise_for_status()
                unzipped = gzip.decompress(response.content)
                decoded = unzipped.decode('utf-8')
                jlist = decoded.splitlines()
                print(jlist[2])
                rdd = spark.sparkContext.parallelize(jlist[:2])
                df_hourly = spark.read.json(rdd)
                df = df.unionByName(df_hourly) if df is not None else df_hourly
                break  # for the moment trying to get a single day to get parsed right
            except requests.exceptions.HTTPError as e:
                print(e)
        break
    df.show(2)


if __name__ == "__main__":
    download_data()
