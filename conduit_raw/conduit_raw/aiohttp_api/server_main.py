from aiohttp import web
from conduit_lib import LMDB_Database
import logging
import os
from pathlib import Path

from conduit_lib.headers_api_threadsafe import HeadersAPIThreadsafe

from .constants import SERVER_HOST, SERVER_PORT
from .server import get_aiohttp_app, ApplicationState


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
        await self._app_state.setup_async()

    async def on_shutdown(self, app: web.Application) -> None:
        self._logger.debug("Stopped reference server API")

    async def start(self) -> None:
        self._logger.debug("Starting reference server API on http://%s:%s", self._host,
            self._port)
        self._runner = web.AppRunner(self._app, access_log=None)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self._host, self._port, reuse_address=True)
        await site.start()

    async def stop(self) -> None:
        self._logger.debug("Stopping reference server API")
        assert self._runner is not None
        await self._runner.cleanup()


async def main(lmdb: LMDB_Database, headers_threadsafe: HeadersAPIThreadsafe,
        network: str='mainnet') -> None:
    app, app_state = get_aiohttp_app(lmdb, headers_threadsafe, network)
    server = AiohttpServer(app)
    await server.start()
    try:
        await app_state.wait_for_exit_async()
    finally:
        await server.stop()
