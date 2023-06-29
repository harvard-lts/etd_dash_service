import logging
from logging.handlers import TimedRotatingFileHandler
import os

LOG_FILE_BACKUP_COUNT = os.getenv('LOG_FILE_BACKUP_COUNT')
LOG_ROTATION = "midnight"


def configure_logger():
    log_level = os.getenv("APP_LOG_LEVEL", "WARNING")
    log_file_path = os.getenv("LOGFILE_PATH",
                              "/home/etdadm/logs/etd_dash/etd_dash.log")
    formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger = logging.getLogger('etd_dash')
    logger.addHandler(console_handler)
    if not os.getenv("CONSOLE_LOGGING_ONLY"):
        file_handler = TimedRotatingFileHandler(
            filename=log_file_path,
            when=LOG_ROTATION,
            backupCount=LOG_FILE_BACKUP_COUNT
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    logger.setLevel(log_level)
