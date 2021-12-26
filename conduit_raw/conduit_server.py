"""
Copyright (c) 2020-2021 AustEcon i.e. Hayden J. Donnelly <austecon0922@gmail.com>
"""
import asyncio
import logging.handlers
import logging
import os
import sys
from pathlib import Path
from typing import Any, NamedTuple

from conduit_lib.logging_client import set_logging_level, setup_tcp_logging
from conduit_lib.constants import CONDUIT_RAW_SERVICE_NAME
from conduit_lib.networks import NetworkConfig
from conduit_lib.logging_server import TCPLoggingServer

from conduit_raw.controller import Controller
from conduit_lib.utils import get_log_level, get_network_type, \
    resolve_hosts_and_update_env_vars, load_dotenv


MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))

loop_type = None
if sys.platform == 'win32':
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    # selector = selectors.SelectSelector()
    # loop = asyncio.SelectorEventLoop(selector)

# If uvloop is installed - make use of it
elif sys.platform == 'linux':
    try:
        import uvloop
        uvloop.install()
        loop_type = 'uvloop'
    except ImportError:
        pass


def configure():
    dotenv_path = MODULE_DIR.parent / '.env'
    load_dotenv(dotenv_path)
    resolve_hosts_and_update_env_vars()
    set_logging_level(get_log_level(CONDUIT_RAW_SERVICE_NAME))
    setup_tcp_logging(port=54545)


def loop_exception_handler(loop, context) -> None:
    logger = logging.getLogger("loop-exception-handler")
    logger.debug("Exception handler called")
    logger.debug(context)


def print_config():
    logger = logging.getLogger("print-config")  # Not logged to file or remote TCPLoggingServer
    logger.debug(f"MYSQL_HOST: {os.environ['MYSQL_HOST']}")
    logger.debug(f"MYSQL_PORT: {os.environ['MYSQL_PORT']}")


async def main():
    os.environ['SERVER_TYPE'] = "ConduitRaw"
    loop = asyncio.get_running_loop()
    try:
        logging_server_proc = TCPLoggingServer(port=54545, service_name=CONDUIT_RAW_SERVICE_NAME,
            kill_port=46464)
        logging_server_proc.start()
        configure()  # All configuration is via environment variables

        loop.set_exception_handler(loop_exception_handler)
        net_config = NetworkConfig(os.environ["NETWORK"], node_host=os.environ['NODE_HOST'],
            node_port=int(os.environ['NODE_PORT']))
        controller = Controller(
            net_config=net_config, host="127.0.0.1", port=8000,
            logging_server_proc=logging_server_proc,
            loop_type=loop_type,
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
