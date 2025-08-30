import logging
import os


def get_logger(name: str) -> logging.Logger:
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
