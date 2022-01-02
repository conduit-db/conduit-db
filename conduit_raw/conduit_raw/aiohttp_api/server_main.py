from aiohttp import web
from conduit_lib import LMDB_Database
import os
import sys
from pathlib import Path
import asyncio
import logging
import typing
from typing import Optional

if typing.TYPE_CHECKING:
    from .server import ApplicationState

try:
    from .constants import SERVER_HOST, SERVER_PORT
    from .server import get_aiohttp_app
except ImportError:
    from conduit_lib.constants import SERVER_HOST, SERVER_PORT  # type: ignore[no-redef, attr-defined]
    from conduit_raw.conduit_raw.aiohttp_api.server import get_aiohttp_app


MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
logger = logging.getLogger("server")


class AiohttpServer:

    def __init__(self, app: web.Application, host: str = SERVER_HOST,
            port: int = SERVER_PORT) -> None:
        self.runner: Optional[web.AppRunner] = None
        self.app = app
        self.app_state: 'ApplicationState' = app['app_state']
        self.app.on_startup.append(self.on_startup)
        self.app.on_shutdown.append(self.on_shutdown)
        self.app.freeze()  # No further callback modification allowed
        self.host = host
        self.port = port
        self.logger = logging.getLogger("aiohttp-rest-api")

    async def on_startup(self, app: web.Application) -> None:
        # self.logger.debug("Starting...")
        pass

    async def on_shutdown(self, app: web.Application) -> None:
        self.logger.debug("Cleaning up...")
        self.app.is_alive = False
        self.logger.debug("Stopped.")

    async def start(self) -> None:
        self.app.is_alive = True
        self.logger.debug("Started on http://%s:%s", self.host, self.port)
        self.runner = web.AppRunner(self.app, access_log=None)
        await self.runner.setup()
        site = web.TCPSite(self.runner, self.host, self.port, reuse_address=True)
        await site.start()
        self.app_state.start_threads()
        self.app_state.start_tasks()
        while self.app.is_alive:  # type: ignore
            await asyncio.sleep(0.5)

    async def stop(self) -> None:
        assert self.runner is not None
        await self.runner.cleanup()


async def main(lmdb: LMDB_Database) -> None:
    app = get_aiohttp_app(lmdb)
    server = AiohttpServer(app)
    try:
        await server.start()
    except Exception:
        logger.exception("Unexpected exception in aiohttp server")
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
