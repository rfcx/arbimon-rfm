from .logs import get_logger
from .read_config_from_env import read_config_from_env
from .read_config_from_args import read_config_from_args
from .validate_config import validate_config

log = get_logger()

default_config = {
    'soundscape_normalize': 1
}

def read_config() -> dict:
    env_config = read_config_from_env()
    args_config = read_config_from_args()

    config = {**default_config, **env_config, **args_config}
    for k, v in config.items():
        log.info(f'{k}: {v}')

    validate_config(config)

    return config
