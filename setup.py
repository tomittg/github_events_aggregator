from setuptools import setup, find_packages

setup(
    name='github_events_aggregator',
    version='0.1.0',
    packages=find_packages(),
    install_requires=[
        'pyspark',
        'requests',
        'pyyaml',
        'pytest'
    ],
    description='Package that extracts github events from https://www.gharchive.org/ , performs some aggregation '
                'operations over them and loads the result in csv and parquet formats',
    include_package_data=True,
    package_data={'github_events_aggregator': ['config/config.yaml']}
)
