import sys

from .config.logs import get_logger
from .config.read_config import read_config
from .legacy.classify import run_classification

log = get_logger()

def main(config):
    job_id = config['job_id']
    # run_classification returns True on real success, False on failure.
    # Propagate that so the process exit code is HONEST: previously main()
    # ignored the result and always exited 0, so a job that died mid-run
    # (e.g. a load-induced DB drop) was reported to Kubernetes as a SUCCESS
    # -- no retry, no alert, and jobs.state left mislabeled 'completed' with
    # completed=0 and zero classification_results. A non-zero exit marks the
    # k8s Job Failed so its backoff_limit retries it, and surfaces the failure
    # to monitoring instead of masquerading as success.
    return run_classification(job_id)

if __name__ == "__main__":
    log.info('PROCESS: Initialization')
    config = read_config()
    log.info('PROCESS: Job started')
    ok = main(config)
    if ok is False:
        log.info('PROCESS: Job FAILED')
        sys.exit(1)
    log.info('PROCESS: Job completed')
