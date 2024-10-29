import os


def read_config_from_env() -> dict:
    config = {}

    if "PLAYLIST_ID" in os.environ:
        config['playlist_id'] = int(os.getenv("PLAYLIST_ID")) if os.getenv("PLAYLIST_ID") is not None else None

    if "JOB_NAME" in os.environ:
        config['job_name'] = os.getenv("JOB_NAME")

    return config
