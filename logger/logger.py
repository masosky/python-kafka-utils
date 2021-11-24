import logging
import logging.config
import os
import time

logger = None

conf = {
    "version": 1,
    "disable_existing_loggers": "True",
    "formatters": {
        "standard": {
            "format": f"%(asctime)s, %(levelname)s, {os.getpid()}, %(name)s, %(message)s",
            "datefmt": "%Y-%m-%dT%H:%M:%SZ"
        }
    },
    "handlers": {
        "default": {
            "level": "DEBUG",
            "formatter": "standard",
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout"
        }
    },
    "loggers": {
        "python-kafka-utils": {
            "handlers": [
                "default"
            ],
            "level": "DEBUG",
            "propagate": "False"
        }
    }
}


def get_logger() -> logging.Logger:
    global logger
    if logger is None:
        logging.Formatter.converter = time.gmtime
        logging.config.dictConfig(conf)
        logger = logging.getLogger("python-kafka-utils")
    return logger
