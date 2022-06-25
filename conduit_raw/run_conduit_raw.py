"""
Copyright (c) 2020-2021 AustEcon i.e. Hayden J. Donnelly <austecon0922@gmail.com>
"""
import asyncio
import logging.handlers
import logging
import os
import sys
from asyncio import AbstractEventLoop
from pathlib import Path

import typing
from typing import Dict, Any

# The loading of environment variables must occur before importing any other
# conduit_lib modules so the `.env` file environment variables are loaded before `constants.py`.
MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
from conduit_lib.startup_utils import load_dotenv, is_docker, resolve_hosts_and_update_env_vars

dotenv_path = MODULE_DIR.parent / '.env'
if is_docker():
    dotenv_path = MODULE_DIR.parent / '.env.docker'
load_dotenv(dotenv_path)
resolve_hosts_and_update_env_vars()

if typing.TYPE_CHECKING:
    from conduit_raw.conduit_raw.controller import Controller
else:
    from conduit_raw.controller import Controller

CONDUIT_ROOT_PATH = MODULE_DIR.parent
sys.path.insert(0, str(CONDUIT_ROOT_PATH))
from conduit_lib.logging_client import set_logging_level, setup_tcp_logging
from conduit_lib.constants import CONDUIT_RAW_SERVICE_NAME
from conduit_lib.networks import NetworkConfig
from conduit_lib.logging_server import TCPLoggingServer
from conduit_lib.utils import get_log_level

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


def configure() -> None:
    set_logging_level(get_log_level(CONDUIT_RAW_SERVICE_NAME))
    setup_tcp_logging(port=54545)


def loop_exception_handler(_loop: AbstractEventLoop, context: Dict[str, Any]) -> None:
    logger = logging.getLogger("loop-exception-handler")
    logger.debug("Exception handler called")
    logger.debug(context)


async def main() -> None:
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
        os.environ['GENESIS_BLOCK_HASH'] = net_config.GENESIS_BLOCK_HASH
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
