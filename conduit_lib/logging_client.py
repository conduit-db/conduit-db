import logging.handlers
import logging
import os
import sys

from .constants import LOGGING_LEVEL_VARNAME

# Log MTreeLevel
PROFILING = 9
MODULE_DIR = os.path.dirname(os.path.abspath(__file__))


def set_logging_level(logging_level: int) -> None:
    if LOGGING_LEVEL_VARNAME in os.environ:
        logging_level_name = os.environ[LOGGING_LEVEL_VARNAME].lower()
        logging_levels = {
            "info": logging.INFO,
            "warning": logging.WARNING,
            "critical": logging.CRITICAL,
            "debug": logging.DEBUG,
            "error": logging.ERROR,
        }
        if logging_level_name not in logging_levels:
            print(
                f"Environment variable '{LOGGING_LEVEL_VARNAME}' invalid. "
                f"Must be one of {logging_levels.keys()}"
            )
            sys.exit(1)
        logging_level = logging_levels[logging_level_name]
    logging.root.setLevel(logging_level)


def setup_tcp_logging(port: int = 54545) -> None:
    rootLogger = logging.getLogger('')
    logging.addLevelName(PROFILING, 'PROFILING')

    # Remote Socket Handler (formatting is done server side after unpickling records)
    socketHandler = logging.handlers.SocketHandler('127.0.0.1', port)
    rootLogger.addHandler(socketHandler)

