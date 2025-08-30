import logging
import os


def get_logger(name: str) -> logging.Logger:
    """
    Get a configured logger by name, creating a logs directory and attaching file and stream handlers on first use.
    
    If the logger has no handlers, this function:
    - Ensures the "data/logs" directory exists.
    - Sets logger level to INFO.
    - Adds a FileHandler writing to "data/logs/pipeline.log" and a StreamHandler, both using the format "%(asctime)s %(levelname)s %(name)s: %(message)s".
    
    Parameters:
        name (str): Logger name passed to logging.getLogger().
    
    Returns:
        logging.Logger: The named logger, configured if it had no handlers.
    """
    os.makedirs("data/logs", exist_ok=True)
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        fh = logging.FileHandler("data/logs/pipeline.log")
        fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
        fh.setFormatter(fmt)
        logger.addHandler(fh)
        sh = logging.StreamHandler()
        sh.setFormatter(fmt)
        logger.addHandler(sh)
    return logger
