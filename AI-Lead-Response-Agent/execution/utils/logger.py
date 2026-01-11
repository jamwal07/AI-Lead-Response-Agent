import logging
import os
from logging.handlers import RotatingFileHandler

# Define Logs Dir
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LOG_DIR = os.path.join(BASE_DIR, 'logs')
os.makedirs(LOG_DIR, exist_ok=True)

APP_LOG_FILE = os.path.join(LOG_DIR, 'plumber_ai.log')

def setup_logger(name):
    """
    Sets up a structured logger with rotation.
    Format: [Time] [Level] [Module]: Message
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    # Avoid duplicate handlers if setup moved multiple times
    if not logger.handlers:
        from pythonjsonlogger import jsonlogger
        
        # 1. File Handler (Rotating: 5 files of 5MB each)
        file_handler = RotatingFileHandler(APP_LOG_FILE, maxBytes=5*1024*1024, backupCount=5)
        
        # JSON Formatter
        # We include standard fields + allow extras
        formatter = jsonlogger.JsonFormatter(
            '%(asctime)s %(levelname)s %(name)s %(message)s %(filename)s %(funcName)s %(lineno)d',
            rename_fields={"asctime": "timestamp", "levelname": "level", "name": "logger"},
            datefmt='%Y-%m-%dT%H:%M:%S%z'
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        
        # 2. Console Handler (Keep it simple for human readability, or JSON too?)
        # Let's keep Console simple for dev, JSON for machine/file.
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(jsonlogger.JsonFormatter('%(asctime)s %(levelname)s %(message)s'))
        logger.addHandler(console_handler)
        
    return logger
