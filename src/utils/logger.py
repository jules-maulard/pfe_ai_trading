import logging
import sys
from pathlib import Path
from concurrent_log_handler import ConcurrentRotatingFileHandler

def get_logger(name: str):
    logger = logging.getLogger(name)
    
    if not logger.handlers:
        logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%H:%M:%S" # %Y-%m-%d for date
        )

        # Handler Console
        console_handler = logging.StreamHandler(sys.stderr) # stderr for MCP
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # Handler File
        log_file = Path("database/app.log")
        log_file.parent.mkdir(parents=True, exist_ok=True)
        try:
            if log_file.exists():
                log_file.unlink()
        except Exception:
            pass
        file_handler = ConcurrentRotatingFileHandler(str(log_file), mode='a', maxBytes=10 * 1024 * 1024, backupCount=5)
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger