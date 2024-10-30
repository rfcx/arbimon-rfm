from .config.logs import get_logger
from .config.read_config import read_config
from .legacy.db import connect, get_automated_user, create_job
from .legacy.a2audio.classification_lib import run_classification

log = get_logger()

def main(config):
    conn = connect()
    
    playlist_id = config['playlist_id']
    job_name = config['job_name']

    user_id = get_automated_user(conn)

    job_id = create_job(conn, playlist_id, user_id, job_name)
    print('- Created and initialized job', job_id)
    if job_id is None:
        print('Something went wrong creating the job')
        exit(1)

    run_classification(job_id)
    print('- Completed job', job_id)


if __name__ == "__main__":
    log.info('PROCESS: Initialization')
    config = read_config()
    log.info('PROCESS: Job started')
    main(config)
    log.info('PROCESS: Job completed')
