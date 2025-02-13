from pathlib import Path
import logging

LOG_DIR = Path(__file__).parent.parent.parent / "logs"

def get_logger(file_name: str) -> logging.Logger:
    """
    Configures and returns a logger instance.

    Args:
        file_name (str): The name of the log file (without extension).

    Returns:
        logging.Logger: Configured logger instance.
    """
    logger = logging.getLogger(file_name)
    if not logger.handlers:
        handler = logging.FileHandler(LOG_DIR / f"{file_name}.log")
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    return logger