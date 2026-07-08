import sys

from .config.logs import get_logger
from .config.read_config import read_config
from .legacy.retrain import retrain

log = get_logger()

def main(config):
    job_id = config['job_id']
    # retrain returns True on real success (all failure paths inside it
    # sys.exit(-1) via exit_error). Propagate the result so the process exit
    # code is HONEST: previously main() ignored it and always exited 0, so a
    # run that failed through any non-sys.exit path was reported to
    # Kubernetes as SUCCESS -- no retry, no alert (the same silent-completion
    # class as classify_legacy; see rfcx-local OPEN-ITEMS #28).
    return retrain(job_id)

if __name__ == "__main__":
    log.info('PROCESS: Initialization')
    config = read_config()
    log.info('PROCESS: Job started')
    ok = main(config)
    if ok is not True:
        log.info('PROCESS: Job FAILED')
        sys.exit(1)
    log.info('PROCESS: Job completed')
