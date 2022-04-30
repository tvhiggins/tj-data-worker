import logging
from time import gmtime


def setup_custom_logger(name, log_level=logging.INFO) -> logging.Logger:
    logger = logging.getLogger(name)

    # do not want to add same logger twice
    if not len(logger.handlers):
        logging.Formatter.converter = gmtime
        formatter = logging.Formatter(
            fmt="%(asctime)s.%(msecs)03d - %(levelname)s - %(module)s - %(funcName)s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        handler = logging.StreamHandler()
        handler.setFormatter(formatter)

        logger = logging.getLogger(name)
        logger.setLevel(log_level)
        logger.addHandler(handler)

    return logger
