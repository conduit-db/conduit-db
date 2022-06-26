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
from typing import Any

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
    from conduit_index.conduit_index.controller import Controller
else:
    from conduit_index.controller import Controller

CONDUIT_ROOT_PATH = MODULE_DIR.parent
sys.path.insert(1, str(CONDUIT_ROOT_PATH))
from conduit_lib.logging_server import TCPLoggingServer
from conduit_lib.constants import CONDUIT_INDEX_SERVICE_NAME
from conduit_lib.logging_client import setup_tcp_logging, set_logging_level, teardown_tcp_logging
from conduit_lib.networks import NetworkConfig
from conduit_lib.utils import get_log_level


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


def loop_exception_handler(_loop: AbstractEventLoop, context: dict[str, Any]) -> None:
    logger = logging.getLogger("loop-exception-handler")
    exception = context.get("exception")
    if exception is not None:
        logger.exception("Exception raised in asyncio loop", exc_info=exception)
    else:
        logger.error("Error in asyncio loop without exception, message: %s", context["message"])


async def main() -> None:
    os.environ['SERVER_TYPE'] = "ConduitIndex"
    loop = asyncio.get_running_loop()
    loop.set_exception_handler(loop_exception_handler)

    logging_server_proc = TCPLoggingServer(port=65421, service_name=CONDUIT_INDEX_SERVICE_NAME,
        kill_port=63241)
    logging_server_proc.start()

    # Logging configuration is via environment variables
    set_logging_level(get_log_level(CONDUIT_INDEX_SERVICE_NAME))
    tcp_log_handler = setup_tcp_logging(port=65421)

    try:
        logger = logging.getLogger("main-task")

        net_config = NetworkConfig(os.environ["NETWORK"], node_host=os.environ['NODE_HOST'],
            node_port=int(os.environ['NODE_PORT']))
        os.environ['GENESIS_ACTIVATION_HEIGHT'] = str(net_config.GENESIS_ACTIVATION_HEIGHT)
        controller = Controller(
            net_config=net_config, host="127.0.0.1", port=8000, loop_type=loop_type,
        )
        try:
            await controller.run()
        finally:
            logger.debug("Stopping controller")
            try:
                await controller.stop()
            except Exception:
                # Exceptions raised in finally clauses are suppressed by the runtime.
                import traceback
                traceback.print_exc()
                raise
    finally:
        teardown_tcp_logging(tcp_log_handler)

        # This definitely would not make it to the log server.
        print("Shutting down logging server")
        logging_server_proc.terminate()
        logging_server_proc.join()


if __name__ == "__main__":
    try:
        asyncio.run(main(), debug=False)
    except KeyboardInterrupt:
        print("ConduitDB Stopped")
