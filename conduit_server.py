import argparse
import asyncio
import logging
import os
import sys
import time
from typing import Dict

from conduit.logs import logs
from conduit.session_manager import SessionManager
from conduit.constants import (
    DATABASE_NAME_VARNAME,
    BITCOIN_NETWORK_VARNAME,
    DATABASE_USER_VARNAME,
    DATABASE_HOST_VARNAME,
    DATABASE_PORT_VARNAME,
    DATABASE_PASSWORD_VARNAME,
    LOGGING_LEVEL_VARNAME,
    TESTNET,
    SCALINGTESTNET,
    REGTEST,
    MAINNET,
)

# selector = selectors.SelectSelector()
# loop = asyncio.SelectorEventLoop(selector)


loop = asyncio.ProactorEventLoop()
asyncio.set_event_loop(loop)

logger = logs.get_logger("main")


def get_parser():
    parser = argparse.ArgumentParser(description="run conduit chain-indexer")
    parser.add_argument(
        "--host",
        dest="host",
        nargs=argparse.OPTIONAL,
        help="specify a host bitcoin node to connect to (to override default)",
    )
    parser.add_argument(
        "--port",
        dest="port",
        action="store",
        type=int,
        help="port for remote daemon; defaults=[mainnet=8333, testnet=18333, "
        "scaling-testnet=9333, regtest=18444]",
    )

    parser.add_argument(
        "--mainnet", action="store_true", dest=MAINNET, help="use mainnet"
    )
    parser.add_argument(
        "--testnet", action="store_true", dest=TESTNET, help="use testnet (default)"
    )
    parser.add_argument(
        "--scaling-testnet",
        action="store_true",
        dest=SCALINGTESTNET,
        help="Use scaling-testnet",
    )
    parser.add_argument(
        "--regtest",
        action="store_true",
        dest=REGTEST,
        help="Use regression testnet",
    )

    parser.add_argument(
        "-v",
        "--verbosity",
        action="store",
        dest="verbosity",
        const="info",
        nargs=argparse.OPTIONAL,
        choices=("debug", "info", "warning", "error", "critical"),
        help="Set logging verbosity",
    )
    parser.add_argument(
        "--file-logging", action="store_true", dest="filelogging", help="log to file"
    )
    parser.add_argument(
        "--client-mode",
        action="store_true",
        dest="client_mode",
        help="will not sync any blocks and does not require a "
        "database or redis - for use as a broadcasting "
        "service only (Not implemented)",
    )
    return parser


DEFAULT_ENV_VARS = [
    (DATABASE_NAME_VARNAME, "conduitdb", DATABASE_NAME_VARNAME),
    (BITCOIN_NETWORK_VARNAME, MAINNET, BITCOIN_NETWORK_VARNAME),
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


def get_and_set_network_type(env_vars):
    nets = [TESTNET, SCALINGTESTNET, REGTEST, MAINNET]
    for arg in env_vars.keys():
        if (arg in nets and env_vars.get(arg) is True):
            env_vars['network'] = arg
    return env_vars.get('network')


def parse_args() -> Dict:
    parser = get_parser()
    args = parser.parse_args()

    # config is an object passed to various constructors
    config_options = args.__dict__
    config_options = {
        key: value for key, value in config_options.items() if value is not None
    }
    return config_options


def setup():
    env_vars = {}
    env_vars.update(get_env_vars())
    env_vars.update(parse_args())  # overrides
    get_and_set_network_type(env_vars)
    return env_vars


def loop_exception_handler(loop, context) -> None:
    logger.debug("Exception handler called")
    logger.debug(context)


async def main():
    try:
        loop = asyncio.get_running_loop()
        loop.set_exception_handler(loop_exception_handler)
        env_vars = setup()
        session_manager = SessionManager(
            network=env_vars.get("network"),
            host="127.0.0.1",
            port=8000,
            env_vars=env_vars
        )
        await session_manager.run()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    asyncio.run(main(), debug=True)
