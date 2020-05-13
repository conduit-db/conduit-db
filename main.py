import asyncio
import selectors
import logging
import os
import sys
import time
from typing import Dict

import database
from logs import logs
from session_manager import SessionManager
from constants import (
    DATABASE_NAME_VARNAME,
    BITCOIN_NETWORK_VARNAME,
    DATABASE_USER_VARNAME,
    DATABASE_HOST_VARNAME,
    DATABASE_PORT_VARNAME,
    DATABASE_PASSWORD_VARNAME,
    LOGGING_LEVEL_VARNAME,
)

selector = selectors.SelectSelector()
loop = asyncio.SelectorEventLoop(selector)
asyncio.set_event_loop(loop)

logger = logs.get_logger("main")


DEFAULT_ENV_VARS = [
    (DATABASE_NAME_VARNAME, "conduitdb", DATABASE_NAME_VARNAME),
    (BITCOIN_NETWORK_VARNAME, "regtest", BITCOIN_NETWORK_VARNAME),
    (DATABASE_USER_VARNAME, "conduitadmin", DATABASE_USER_VARNAME),
    (DATABASE_HOST_VARNAME, "127.0.0.1", DATABASE_HOST_VARNAME),
    (DATABASE_PORT_VARNAME, 5432, DATABASE_PORT_VARNAME),
    (DATABASE_PASSWORD_VARNAME, "conduitpass", DATABASE_PASSWORD_VARNAME),
]


def setup_logging() -> None:
    log_path = os.path.join(os.getcwd(), "logs")
    os.makedirs(log_path, exist_ok=True)
    logfile_path = os.path.join(log_path, time.strftime("%Y%m%d-%H%M%S") + ".log")
    file_handler = logging.FileHandler(logfile_path)
    formatter = logging.Formatter("%(asctime)s:" + logging.BASIC_FORMAT)
    file_handler.setFormatter(formatter)
    logging.root.addHandler(file_handler)


def set_logging_level() -> None:
    logging_level = logging.DEBUG
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


def get_env_vars() -> Dict:
    env_vars = {}
    setup_logging()
    for varname, vardefault, configname in DEFAULT_ENV_VARS:
        varvalue = vardefault
        if varname in os.environ:
            varvalue = type(vardefault)(os.environ[varname])
        env_vars[configname] = varvalue

    set_logging_level()
    return env_vars


def setup():
    env_vars = {}
    env_vars.update(get_env_vars())
    db = database.load(env_vars)
    return env_vars, db


def loop_exception_handler(loop, context) -> None:
    logger.debug("Exception handler called")
    logger.debug(context)


async def main():
    try:
        loop = asyncio.get_running_loop()
        loop.set_exception_handler(loop_exception_handler)
        env_vars, db = setup()
        session_manager = SessionManager(network="main", host="127.0.0.1", port=8000,
            env_vars=env_vars, db=db)
        await session_manager.run()
    except KeyboardInterrupt:
        pass
    finally:
        database.db.close()


if __name__ == "__main__":
    asyncio.run(main(), debug=True)
