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
)
