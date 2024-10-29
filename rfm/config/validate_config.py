
from .logs import get_logger

log = get_logger()

def validate_config(config: dict) -> None:
    not_set_project = 'project' not in config
    not_set_playlist_id = 'playlist_id' not in config

    if not_set_project and not_set_playlist_id:
        log.critical('Invalid configuration: set either project or playlist_id')
        exit(1)
