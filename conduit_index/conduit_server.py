"""
Copyright (c) 2020-2021 AustEcon i.e. Hayden J. Donnelly <austecon0922@gmail.com>
"""
import asyncio
import logging.handlers
import logging
import os
import sys
from typing import Dict

from conduit_lib.logging_server import TCPLoggingServer
from conduit_lib.constants import (DATABASE_NAME_VARNAME, BITCOIN_NETWORK_VARNAME,
    DATABASE_USER_VARNAME, DATABASE_HOST_VARNAME, DATABASE_PORT_VARNAME, DATABASE_PASSWORD_VARNAME,
    TESTNET, SCALINGTESTNET, REGTEST, MAINNET, RESET_VARNAME, PROFILING,
    CONDUIT_INDEX_SERVICE_NAME, )
from conduit_lib.logging_client import setup_tcp_logging, set_logging_level
from conduit_lib.networks import NetworkConfig

from conduit_index.controller import Controller
from conduit_lib.argparsing import get_parser
# from conduit_index.conduit_index.workers.logging_server import TCPLoggingServer
from conduit_lib.utils import cast_to_valid_ipv4, get_log_level

if sys.platform == 'win32':
    loop = asyncio.ProactorEventLoop()
    asyncio.set_event_loop(loop)


# If uvloop is installed - make use of it
elif sys.platform == 'linux':
    try:
        import uvloop
        uvloop.install()
    except ImportError:
        pass


DEFAULT_ENV_VARS = [
    (DATABASE_NAME_VARNAME, "conduitdb", DATABASE_NAME_VARNAME),
    (BITCOIN_NETWORK_VARNAME, REGTEST, BITCOIN_NETWORK_VARNAME),
    (RESET_VARNAME, False, RESET_VARNAME),
    (DATABASE_USER_VARNAME, "conduitadmin", DATABASE_USER_VARNAME),
    (DATABASE_HOST_VARNAME, "127.0.0.1", DATABASE_HOST_VARNAME),
    (DATABASE_PORT_VARNAME, 5432, DATABASE_PORT_VARNAME),
    (DATABASE_PASSWORD_VARNAME, "conduitpass", DATABASE_PASSWORD_VARNAME),
]


def get_env_vars() -> Dict:
    env_vars = {}
    for varname, vardefault, configname in DEFAULT_ENV_VARS:
        varvalue = vardefault
        if varname in os.environ:
            varvalue = type(vardefault)(os.environ[varname])
        env_vars[configname] = varvalue
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

    # net_config is an object passed to various constructors
    config_options = args.__dict__
    config_options = {
        key: value for key, value in config_options.items() if value is not None
    }
    return config_options


def set_env_vars(config: Dict):
    logger = logging.getLogger("set_env_vars")
    os.environ['GRPC_ENABLE_FORK_SUPPORT'] = '1'
    if sys.platform == 'linux':
        os.environ['GRPC_POLL_STRATEGY'] = 'epoll1'
    host = cast_to_valid_ipv4(config['mysql_host'].split(":")[0])
    port = config['mysql_host'].split(":")[1]
    os.environ['MYSQL_HOST'] = host
    os.environ['MYSQL_PORT'] = port
    logger.debug(f"MYSQL_HOST: {host}")
    logger.debug(f"MYSQL_PORT: {port}")


def setup():
    env_vars = {}
    env_vars.update(get_env_vars())
    env_vars.update(parse_args())  # overrides
    get_and_set_network_type(env_vars)
    set_logging_level(get_log_level(CONDUIT_INDEX_SERVICE_NAME))
    setup_tcp_logging(port=65421)
    return env_vars


def loop_exception_handler(loop, context) -> None:
    logger = logging.getLogger("loop-exception-handler")
    logger.debug("Exception handler called")
    logger.debug(context)


async def main():
    loop = asyncio.get_running_loop()
    try:
        logging_server_proc = TCPLoggingServer(port=65421, service_name=CONDUIT_INDEX_SERVICE_NAME,
            kill_port=63241)
        logging_server_proc.start()
        config = setup()
        set_env_vars(config)
        config['server_type'] = "ConduitIndex"
        loop.set_exception_handler(loop_exception_handler)
        net_config = NetworkConfig(config.get("network"), node_host=config['node_host'])
        controller = Controller(
            config=config,
            net_config=net_config, host="127.0.0.1", port=8000,
            logging_server_proc=logging_server_proc
        )
        await controller.run()
    except KeyboardInterrupt:
        loop.stop()
        loop.run_forever()
        print("ConduitDB Stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main(), debug=False)
    except KeyboardInterrupt:
        print("ConduitDB Stopped")

