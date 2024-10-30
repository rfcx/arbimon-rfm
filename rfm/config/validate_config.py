
from .logs import get_logger

log = get_logger()

def validate_config(config: dict) -> None:
    not_set_job_id = 'job_id' not in config

    if not_set_job_id:
        log.critical('Invalid configuration: job_id not set')
        exit(1)
