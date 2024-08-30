from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, countDistinct, when


def aggregate_data(data: list[str]):
    spark = SparkSession.builder \
        .appName('JSON to DataFrame') \
        .config('spark.driver.memory', '8g') \
        .config('spark.executor.memory', '8g') \
        .config('spark.executor.instances', '4') \
        .getOrCreate()

    rdd = spark.sparkContext.parallelize(data, numSlices=10000)
    df = spark.read.json(rdd)

    df = df.withColumn('date', to_date(col('created_at')))

    repo_agg = _aggregate_repositories(df)
    user_agg = _aggregate_users(df)
    return {'repository_aggregation': repo_agg,
            'user_aggregation': user_agg}


def _aggregate_repositories(df):
    return df.groupBy('date', 'repo.id', 'repo.name') \
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


def _aggregate_users(df):
    return df.groupBy('date', 'actor.id', 'actor.login') \
        .agg(
        countDistinct(when(col('type') == 'WatchEvent', col('repo.id'))).alias('starred_count'),
        countDistinct(when((col('type') == 'IssuesEvent') & (col('payload.action') == 'opened'), col('repo.id'))).alias(
            'created_issues_count')
    )