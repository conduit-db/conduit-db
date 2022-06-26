from __future__ import annotations
from aiohttp import web
import asyncio
from conduit_lib import LMDB_Database
import logging
import os
from pathlib import Path
import sys
import typing

if typing.TYPE_CHECKING:
    from .server import ApplicationState

try:
    from .constants import SERVER_HOST, SERVER_PORT
    from .server import get_aiohttp_app
except ImportError:
    from conduit_lib.constants import SERVER_HOST, SERVER_PORT  # type: ignore
    from conduit_raw.conduit_raw.aiohttp_api.server import get_aiohttp_app  # type: ignore


MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
logger = logging.getLogger("server")


class AiohttpServer:

    def __init__(self, app: web.Application, host: str = SERVER_HOST,
            port: int = SERVER_PORT) -> None:
        self._runner: web.AppRunner | None = None
        self._app = app
        self._app_state: ApplicationState = app['app_state']
        self._app.on_startup.append(self.on_startup)
        self._app.on_shutdown.append(self.on_shutdown)
        self._app.freeze()  # No further callback modification allowed
        self._host = host
        self._port = port
        self._logger = logging.getLogger("aiohttp-rest-api")

    async def on_startup(self, app: web.Application) -> None:
        self._logger.debug("Started reference server API")

    async def on_shutdown(self, app: web.Application) -> None:
        self._logger.debug("Stopped reference server API")

    async def start(self) -> None:
        self._logger.debug("Starting reference server API on http://%s:%s", self._host,
            self._port)
        self._runner = web.AppRunner(self._app, access_log=None)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self._host, self._port, reuse_address=True)
        await site.start()
        self._app_state.start_threads()
        self._app_state.start_tasks()

    async def stop(self) -> None:
        self._logger.debug("Stopping reference server API")
        assert self._runner is not None
        await self._runner.cleanup()


async def main(lmdb: LMDB_Database) -> None:
    app, app_state = get_aiohttp_app(lmdb)
    server = AiohttpServer(app)
    await server.start()
    try:
        await app_state.wait_for_exit_async()
    finally:
        await server.stop()


if __name__ == "__main__":
    try:
        lmdb = LMDB_Database()
        asyncio.run(main(lmdb))
        sys.exit(0)
    except KeyboardInterrupt:
        pass
    except Exception:
        logger.exception("Unexpected exception in __main__")
    finally:
        logger.info("ConduitRaw REST API stopped")
