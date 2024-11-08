from .config.logs import get_logger
from .config.read_config import read_config
from .legacy.classify import run_classification

log = get_logger()

def main(config):
    job_id = config['job_id']
    run_classification(job_id)

if __name__ == "__main__":
    log.info('PROCESS: Initialization')
    config = read_config()
    log.info('PROCESS: Job started')
    main(config)
    log.info('PROCESS: Job completed')
