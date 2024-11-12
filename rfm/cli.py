from .config.logs import get_logger

log = get_logger()

if __name__ == "__main__":
    log.info('CLI not implemented: use `train_legacy` or `classify_legacy`')
