"""
Copyright (c) 2020-2021 AustEcon i.e. Hayden J. Donnelly <austecon0922@gmail.com>
"""
import asyncio
import logging.handlers
import logging
import os
import sys
from pathlib import Path

import typing

if typing.TYPE_CHECKING:
    from conduit_index.conduit_index.controller import Controller
else:
    from conduit_index.controller import Controller

MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
CONDUIT_ROOT_PATH = MODULE_DIR.parent
sys.path.insert(1, str(CONDUIT_ROOT_PATH))
from conduit_lib.logging_server import TCPLoggingServer
from conduit_lib.constants import CONDUIT_INDEX_SERVICE_NAME
from conduit_lib.logging_client import setup_tcp_logging, set_logging_level
from conduit_lib.networks import NetworkConfig
from conduit_lib.utils import get_log_level, resolve_hosts_and_update_env_vars, load_dotenv, \
    is_docker

MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))


loop_type = None
if sys.platform == 'win32':
    # selector = selectors.SelectSelector()
    # loop = asyncio.SelectorEventLoop(selector)
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# If uvloop is installed - make use of it
elif sys.platform == 'linux':
    try:
        import uvloop
        uvloop.install()
        loop_type = 'uvloop'
    except ImportError:
        pass


def configure() -> None:
    dotenv_path = MODULE_DIR.parent / '.env'
    if is_docker():
        dotenv_path = MODULE_DIR.parent / '.env.docker'
    load_dotenv(dotenv_path)
    resolve_hosts_and_update_env_vars()
    set_logging_level(get_log_level(CONDUIT_INDEX_SERVICE_NAME))
    setup_tcp_logging(port=65421)


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
        configure()  # All configuration is via environment variables

        os.environ['SERVER_TYPE'] = "ConduitIndex"
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

