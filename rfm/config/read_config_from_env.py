import os


def read_config_from_env() -> dict:
    config = {}

    if "JOB_ID" in os.environ:
        config['job_id'] = int(os.getenv("JOB_ID")) if os.getenv("JOB_ID") is not None else None
        
    return config
