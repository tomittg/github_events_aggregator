import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, countDistinct, when

from github_events_aggregator.config.config import get_config

logger = logging.getLogger('pipeline.transform')


def aggregate_data():
    """
    Aggregate data for both repositories and users, obtaining the following info:
    - Repo aggregation: date, repo_id, repo_name, starred_count, forked_count, created_issues_count, pr_count
    - User aggregation: date, user_id, user_name, starred_count, created_issues_count

    :return: Dataframes containing the aggregated data and their name as keys
    :rtype: dict[str, pyspark.sql.DataFrame]
    """
    config = get_config()
    source_format = f'{config['data_source']['year']}-{config['data_source']['month']}-*.json'

    logger.info('Initializing spark session...')
    spark = SparkSession.builder \
        .appName('JSON to DataFrame') \
        .config('spark.driver.memory', '8g') \
        .config('spark.executor.memory', '8g') \
        .config('spark.executor.instances', '4') \
        .getOrCreate()
    logger.info('Spark session successfully initialized')

    logger.info(f'Converting list of json extracted into pypark dataframe...')
    df = spark.read.json(f"cached_data/{source_format}")
    logger.info('Dataframe successfully created')

    df = df.withColumn('date', to_date(col('created_at')))

    repo_agg = _aggregate_repositories(df)
    user_agg = _aggregate_users(df)
    return {'repository_aggregation': repo_agg,
            'user_aggregation': user_agg}


def _aggregate_repositories(df):
    logger.info('Aggregating repositories...')
    repo_agg = df.groupBy('date', 'repo.id', 'repo.name') \
        .agg(
        countDistinct(when(col('type') == 'WatchEvent', col('actor.id'))).alias('starred_count'),
        countDistinct(when(col('type') == 'ForkEvent', col('actor.id'))).alias('forked_count'),
        countDistinct(
            when((col('type') == 'IssuesEvent') & (col('payload.action') == 'opened'), col('actor.id'))).alias(
            'created_issues_count'),
        countDistinct(
            when((col('type') == 'PullRequestEvent') & (col('payload.action') == 'closed'), col('actor.id'))).alias(
            'pr_count')
    )
    logger.info('Repositories successfully aggregated.')
    return repo_agg


def _aggregate_users(df):
    logger.info('Aggregating users...')
    user_agg = df.groupBy('date', 'actor.id', 'actor.login') \
        .agg(
        countDistinct(when(col('type') == 'WatchEvent', col('repo.id'))).alias('starred_count'),
        countDistinct(when((col('type') == 'IssuesEvent') & (col('payload.action') == 'opened'), col('repo.id'))).alias(
            'created_issues_count')
    )
    logger.info('Users successfully aggregated.')
    return user_agg
