import yaml
import logging

from pathlib import Path

logger = logging.getLogger('pipeline.config')


def get_config():
    logger.info('Loading config file...')
    config_dir = Path(__file__).parent.resolve()
    with open(f'{config_dir}/config.yaml', 'r') as config_file:
        config = yaml.safe_load(config_file)
    logger.info(f'Loaded')
    return config
