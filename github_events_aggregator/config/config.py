import yaml
from pathlib import Path


def get_config():
    config_dir = Path(__file__).parent.resolve()
    with open(f'{config_dir}/config.yaml', 'r') as config_file:
        config = yaml.safe_load(config_file)
    return config
