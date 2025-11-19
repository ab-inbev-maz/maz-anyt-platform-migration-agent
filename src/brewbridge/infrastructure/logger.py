import logging
import sys


def get_logger(name: str) -> logging.Logger:
    """
    Returns a configured logger instance for the given module.

    Ensures:
    - Unified formatting across project
    - No duplicate handlers
    - Safe for use in infra, domain, and tools layers
    """

    logger = logging.getLogger(name)

    # Prevent duplicate handlers when re-importing modules
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)

    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "[%(asctime)s] [%(levelname)s] [%(name)s] %(message)s",
        "%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)

    logger.addHandler(handler)
    logger.propagate = False

    return logger
