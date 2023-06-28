import logging
from logging.handlers import TimedRotatingFileHandler
import os

LOG_FILE_BACKUP_COUNT = os.getenv('LOG_FILE_BACKUP_COUNT')
LOG_ROTATION = "midnight"

def configure_logger():
    log_level = os.getenv("APP_LOG_LEVEL", "WARNING")
    log_file_path = os.getenv("LOGFILE_PATH", "/home/etdadm/logs/etd/etd_dash.log")
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    file_handler = TimedRotatingFileHandler(
        filename=log_file_path,
        when=LOG_ROTATION,
        backupCount=LOG_FILE_BACKUP_COUNT
    )
    logger = logging.getLogger('etd_dash')
        
    logger.addHandler(file_handler)
    file_handler.setFormatter(formatter)
    logger.setLevel(log_level)   