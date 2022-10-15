import aiohttp
from aiohttp import web
import asyncio
import bitcoinx
import cbor2
from concurrent.futures import ThreadPoolExecutor
import logging
from pathlib import Path
import os
import threading
from typing import AsyncIterator
import zmq
import zmq.asyncio

from conduit_lib.database.lmdb.lmdb_database import LMDB_Database
from conduit_lib.database.mysql.mysql_database import load_mysql_database
from conduit_lib.utils import create_task

from .constants import SERVER_HOST, SERVER_PORT
from . import handlers
from .server_websocket import ReferenceServerConnection, ReferenceServerWebSocket


MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))

# Silence verbose logging
logger = logging.getLogger("server")

aiohttp_logger = logging.getLogger("aiohttp")
aiohttp_logger.setLevel(logging.WARNING)
requests_logger = logging.getLogger("urllib3")
requests_logger.setLevel(logging.WARNING)


class ApplicationState(object):

    def __init__(self, app: web.Application, loop: asyncio.AbstractEventLoop, lmdb: LMDB_Database) \
            -> None:
        self._logger = logging.getLogger('app_state')
        self._app = app
        self._loop = loop
        self._exit_event = asyncio.Event()

        self.mysql_db = load_mysql_database()
        self.lmdb = lmdb
        self.executor = ThreadPoolExecutor(max_workers=1)

        context6 = zmq.asyncio.Context.instance()
        self._reorg_event_socket = context6.socket(zmq.PULL)
        self._reorg_event_socket.bind("tcp://127.0.0.1:51495")

        self._reference_server_connections = dict[str, ReferenceServerConnection]()
        self._reference_server_connections_lock: threading.RLock = threading.RLock()
        self._reference_server_connection_established_event = asyncio.Event()

    async def refresh_mysql_connection_task(self) -> None:
        REFRESH_TIMEOUT = 300
        while True:
            await asyncio.sleep(REFRESH_TIMEOUT)
            await self.mysql_db.mysql_conn.ping(reconnect=True)

    def start_threads(self) -> None:
        pass

    def start_tasks(self) -> None:
        create_task(self.listen_for_reorg_event_job())
        create_task(self.refresh_mysql_connection_task())

    async def wait_for_exit_async(self) -> None:
        await self._exit_event.wait()

    async def listen_for_reorg_event_job(self) -> None:
        """This may not actually be needed for the most part because db entries are immutable.
        In theory this should only be relevant for queries that touch the mempool because
        there will be an atomic step where txs must be both invalidated and put back.

        For confirmed tx metadata / pushdata queries, results should be filtered on the basis
        of the current longest chain (by block_hash). And so the reorg processing can work in
        parallel without corrupting the APIs responses.
        """

        while not self._exit_event.is_set():
            cbor_msg = await self._reorg_event_socket.recv()
            reorg_handling_complete, start_hash, stop_hash = cbor2.loads(cbor_msg)
            self._logger.debug(f"Reorg event received. "
                              f"reorg_handling_complete: {reorg_handling_complete} "
                              f"start_hash: {bitcoinx.hash_to_hex_str(start_hash)}, "
                              f"stop_hash: {bitcoinx.hash_to_hex_str(stop_hash)}")

    def register_reference_server_connection(self, connection: ReferenceServerConnection) -> None:
        with self._reference_server_connections_lock:
            self._reference_server_connections[connection.websocket_id] = connection

        # The reference server is reconnected pre-emptively allow immediate redelivery.
        self._reference_server_connection_established_event.set()
        self._reference_server_connection_established_event.clear()

    def unregister_reference_server_connection(self, websocket_id: str) -> None:
        with self._reference_server_connections_lock:
            del self._reference_server_connections[websocket_id]



async def client_session_ctx(app: web.Application) -> AsyncIterator[None]:
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


def get_aiohttp_app(lmdb: LMDB_Database) -> tuple[web.Application, ApplicationState]:
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

        web.view("/ws", ReferenceServerWebSocket),
        web.post("/api/v1/restoration/search", handlers.get_pushdata_filter_matches),
        web.get("/api/v1/transaction/{txid}", handlers.get_transaction),
        web.get("/api/v1/merkle-proof/{txid}", handlers.get_tsc_merkle_proof),
    ])
    return app, app_state


if __name__ == "__main__":
    lmdb = LMDB_Database()
    app, app_state = get_aiohttp_app(lmdb)
    web.run_app(app, host=SERVER_HOST, port=SERVER_PORT)
