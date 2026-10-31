# SHARED LOGGING AND URL IDENTITY HELPERS
# Logs are relative to the chosen run directory. get_logger adds file/console
# handlers once per named logger, even when workers initialize concurrently.
import os
import logging
from hashlib import sha256
from urllib.parse import urldefrag
from threading import RLock

_LOGGER_LOCK = RLock()

def get_logger(name, filename=None):
    # Logger creation also occurs in tests and worker initialization.
    with _LOGGER_LOCK:
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
        if logger.handlers:
            # Reusing an existing logger must not duplicate every output line.
            return logger
        os.makedirs("Logs", exist_ok=True)
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        for handler in (
            logging.FileHandler(f"Logs/{filename if filename else name}.log"),
            logging.StreamHandler(),
        ):
            handler.setFormatter(formatter)
            handler.setLevel(logging.INFO)
            logger.addHandler(handler)
        logger.propagate = False
        return logger


def get_urlhash(url):
    # This framework URL identifier is separate from handwritten page-content
    # FNV/SimHash in similarity.py. It does not define textual similarity.
    # Scheme/trailing slash differences are significant under the assignment.
    return sha256(urldefrag(url)[0].encode("utf-8")).hexdigest()


def normalize(url):
    # Do not merge schemes, trailing slashes, or query parameters: the assigned
    # report identity rule removes only the fragment component.
    return urldefrag(url)[0]
