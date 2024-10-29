import logging
import sys

logging.basicConfig(
    level=logging.ERROR,
    format="%(asctime)s.%(msecs)03d %(levelname)s: %(name)s: %(message)s",
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

def get_logger(logger_name=None):
    if logger_name == None:
        logger_name = 'app'
    else:
        logger_name = 'app.' + logger_name
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)  # TODO: replace with env var
    return logger
