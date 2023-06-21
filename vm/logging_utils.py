import logging
from logging.handlers import RotatingFileHandler


def setup_logger(level: str, log_file: str = 'logfile.log', jsonlogger=None):
    """
    Sets up the logger with the specified level.

    Args:
        level (str): The desired logging level.
        log_file (str): The file to write the logs to.
    """
    level = level.lower()

    if level not in ["debug", "info", "warning", "error", "critical"]:
        raise ValueError(f'Unknown logging level "{level}".')

    logger = logging.getLogger()
    logger.setLevel(level)

    # Create a file handler for writing logs to a file with rotation
    # 2KB per file, keeping the last 10 files
    file_handler = RotatingFileHandler(log_file, maxBytes=2000, backupCount=10)

    # Use a JSON formatter for structured logging
    formatter = jsonlogger.JsonFormatter()
    file_handler.setFormatter(formatter)

    # Add the file handler to the logger
    logger.addHandler(file_handler)

    for library in ("urllib3", "websockets", "pyppeteer", "asyncio", "selenium"):
        logging.getLogger(library).setLevel(logging.CRITICAL)

    return logger