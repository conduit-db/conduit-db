import time
from pathlib import Path

import aiohttp
import bitcoinx
import requests
from aiohttp import web
import asyncio
import os
import logging
import queue
import threading
from typing import Dict, NoReturn

from conduit_lib.database.lmdb.lmdb_database import LMDB_Database
from conduit_lib.database.mysql.mysql_database import load_mysql_database
from .constants import SERVER_HOST, SERVER_PORT
from . import handlers


MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))

# Silence verbose logging
logger = logging.getLogger("server")

aiohttp_logger = logging.getLogger("aiohttp")
aiohttp_logger.setLevel(logging.WARNING)
requests_logger = logging.getLogger("urllib3")
requests_logger.setLevel(logging.WARNING)


class ApplicationState(object):

    def __init__(self, app: web.Application, loop: asyncio.AbstractEventLoop, lmdb: LMDB_Database) -> None:
        self.logger = logging.getLogger('app_state')
        self.app = app
        self.loop = loop
        self.mysql_db = load_mysql_database()
        self.lmdb = lmdb

    def start_threads(self):
        pass


async def client_session_ctx(app: web.Application) -> NoReturn:
    """
    Cleanup context async generator to create and properly close aiohttp ClientSession
    Ref.:
        > https://docs.aiohttp.org/en/stable/web_advanced.html#cleanup-context
        > https://docs.aiohttp.org/en/stable/web_advanced.html#aiohttp-web-signals
        > https://docs.aiohttp.org/en/stable/web_advanced.html#data-sharing-aka-no-singletons-please
    """
    # logger.debug('Creating ClientSession')
    app['client_session'] = aiohttp.ClientSession()

    yield

    logger.debug('Closing ClientSession')
    await app['client_session'].close()


def get_aiohttp_app(lmdb) -> web.Application:
    loop = asyncio.get_event_loop()
    app = web.Application()
    app.cleanup_ctx.append(client_session_ctx)
    app_state = ApplicationState(app, loop, lmdb)

    # This is the standard aiohttp way of managing state within the handlers
    app['app_state'] = app_state

    # Non-optional APIs
    app.add_routes([
        web.get("/", handlers.ping),
        web.get("/error", handlers.error),
        web.get("/api/v1/restoration/search", handlers.get_pushdata_filter_matches),
        web.get("/api/v1/transaction/{txid}", handlers.get_transaction),
        web.get("/api/v1/merkle-proof/{txid}", handlers.get_tsc_merkle_proof),
    ])
    return app


if __name__ == "__main__":
    app = get_aiohttp_app()
    web.run_app(app, host=SERVER_HOST, port=SERVER_PORT)
