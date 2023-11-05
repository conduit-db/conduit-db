import aiohttp
from aiohttp import web
import asyncio
import bitcoinx
from bitcoinx import hash_to_hex_str
import cbor2
from concurrent.futures import ThreadPoolExecutor
from http import HTTPStatus
import json
import logging
from pathlib import Path
import queue
import os
import time
import threading
from typing import AsyncIterator, Optional, Any
import zmq
import zmq.asyncio

from conduit_lib import NetworkConfig, DBInterface
from conduit_lib.database.db_interface.tip_filter import TipFilterQueryAPI
from conduit_lib.database.db_interface.tip_filter_types import OutboundDataFlag, OutboundDataRow
from conduit_lib.database.db_interface.types import PushdataRowParsed
from conduit_lib.database.lmdb.lmdb_database import LMDB_Database
from conduit_lib.headers_api_threadsafe import HeadersAPIThreadsafe
from conduit_lib.utils import (
    create_task,
    network_str_to_bitcoinx_network,
    future_callback,
)
from conduit_lib.zmq_sockets import bind_async_zmq_socket

from .constants import (
    REFERENCE_SERVER_SCHEME,
    REFERENCE_SERVER_HOST,
    REFERENCE_SERVER_PORT,
)
from . import handlers_restoration, handlers_tip_filter, handlers_headers
from .server_websocket import WSClient, ReferenceServerWebSocket
from .types import (
    TipFilterNotificationEntry,
    TipFilterNotificationBatch,
)
from .worker_state import WorkerStateManager

MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))

# Silence verbose logging
logger = logging.getLogger("server")

aiohttp_logger = logging.getLogger("aiohttp")
aiohttp_logger.setLevel(logging.WARNING)
requests_logger = logging.getLogger("urllib3")
requests_logger.setLevel(logging.WARNING)


class ApplicationState(object):
    def __init__(
        self,
        app: web.Application,
        loop: asyncio.AbstractEventLoop,
        lmdb: LMDB_Database,
        headers_threadsafe: HeadersAPIThreadsafe,
        net_config: NetworkConfig,
    ) -> None:
        self.pushdata_notification_can_send_event: dict[bytes, asyncio.Event] = {}

        self.logger = logging.getLogger("app_state")
        self._app = app
        self._loop = loop
        self._exit_event = asyncio.Event()
        self.BITCOINX_COIN = network_str_to_bitcoinx_network(net_config.NET)
        self.net_config = net_config

        self.db = DBInterface.load_db()
        self.db_tip_filter_queries = TipFilterQueryAPI.from_db(self.db)
        self.db_tip_filter_queries.setup()
        self.lmdb = lmdb
        self.headers_threadsafe = headers_threadsafe
        self.executor = ThreadPoolExecutor(max_workers=1)

        self.zmq_context = zmq.asyncio.Context.instance()
        ZMQ_BIND_HOST = os.getenv("ZMQ_BIND_HOST", "127.0.0.1")
        self._reorg_event_socket = bind_async_zmq_socket(
            self.zmq_context,
            f"tcp://{ZMQ_BIND_HOST}:51495",
            zmq.SocketType.PULL,
        )

        scheme = os.getenv("REFERENCE_SERVER_SCHEME", REFERENCE_SERVER_SCHEME)
        host = os.getenv("REFERENCE_SERVER_HOST", REFERENCE_SERVER_HOST)
        port = os.getenv("REFERENCE_SERVER_PORT", REFERENCE_SERVER_PORT)
        self.reference_server_url = f"{scheme}://{host}:{port}"

        self.ws_clients = dict[str, WSClient]()
        self.ws_clients_lock: threading.RLock = threading.RLock()
        self.ws_queue: queue.Queue[bytes] = queue.Queue()

        self._outbound_data_delivery_event = asyncio.Event()
        self.tasks: list[asyncio.Task[Any]] = []
        self.worker_state_manager = WorkerStateManager(self, self.db_tip_filter_queries)

    async def refresh_connection_task(self) -> None:
        REFRESH_TIMEOUT = 600
        while True:
            await asyncio.sleep(REFRESH_TIMEOUT)
            self.db.ping()

    def start_threads(self) -> None:
        threading.Thread(target=self.push_notifications_thread, daemon=True).start()

    async def setup_async(self) -> None:
        self.is_alive = True
        self.start_threads()
        self.aiohttp_session = aiohttp.ClientSession()
        self.tasks = [
            create_task(self._attempt_outbound_data_delivery_task()),
            create_task(self.listen_for_reorg_event_job()),
            create_task(self.refresh_connection_task()),
            create_task(self.wait_for_new_tip()),
        ]
        self.worker_state_manager.spawn_tasks()

    async def teardown_async(self) -> None:
        self.is_alive = False
        await self.aiohttp_session.close()
        self.worker_state_manager.close()
        for task in self.tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

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
            self.logger.debug(
                f"Reorg event received. "
                f"reorg_handling_complete: {reorg_handling_complete} "
                f"start_hash: {bitcoinx.hash_to_hex_str(start_hash)}, "
                f"stop_hash: {bitcoinx.hash_to_hex_str(stop_hash)}"
            )

    # ---------- SIMPLE INDEXER CODE ---------- #

    def get_ws_clients(self) -> dict[str, WSClient]:
        with self.ws_clients_lock:
            return self.ws_clients

    def add_ws_client(self, ws_client: WSClient) -> None:
        with self.ws_clients_lock:
            self.ws_clients[ws_client.websocket_id] = ws_client

        # The reference server is reconnected pre-emptively allow immediate redelivery.
        self._outbound_data_delivery_event.set()
        self._outbound_data_delivery_event.clear()

    def remove_ws_client_by_id(self, websocket_id: str) -> None:
        with self.ws_clients_lock:
            del self.ws_clients[websocket_id]

    def push_notifications_thread(self) -> None:
        """Emits any notifications from the queue to all connected websockets"""
        try:
            while self.is_alive:
                try:
                    message_bytes = self.ws_queue.get(timeout=0.5)
                except queue.Empty:
                    continue

                self.logger.debug(
                    "Dispatching outgoing websocket message with length=%d",
                    len(message_bytes),
                )
                for ws_client in self.get_ws_clients().values():
                    self.logger.debug(
                        "Sending message to websocket, websocket_id=%s",
                        ws_client.websocket_id,
                    )
                    future = asyncio.run_coroutine_threadsafe(
                        ws_client.websocket.send_bytes(message_bytes),
                        self._loop,
                    )
                    # Futures swallow exceptions so we must install a callback that raises them.
                    future.add_done_callback(future_callback)
        except Exception:
            self.logger.exception("unexpected exception in push_notifications_thread")
        finally:
            self.logger.info("Exited push notifications thread")

    async def _attempt_outbound_data_delivery_task(self) -> None:
        """
        Non-blocking delivery of new tip filter notifications.
        """
        self.logger.debug("Starting outbound data delivery task")
        MAXIMUM_DELAY = 120.0
        while self.is_alive:
            # No point in trying if there is no reference server connected.
            next_check_delay = MAXIMUM_DELAY
            if len(self.get_ws_clients()) > 0:
                rows = self.db_tip_filter_queries.read_pending_outbound_datas(
                    OutboundDataFlag.NONE,
                    OutboundDataFlag.DISPATCHED_SUCCESSFULLY,
                )
                current_rows = list[OutboundDataRow]()
                if len(rows) > 0:
                    current_time = time.time()
                    for row in rows:
                        if row.date_last_tried + MAXIMUM_DELAY > current_time:
                            next_check_delay = (row.date_last_tried + MAXIMUM_DELAY) - current_time + 0.5
                            break
                        current_rows.append(row)

                if len(current_rows) > 0:
                    self.logger.debug(
                        "Outbound data delivery of %d entries, next delay will " "be %0.2f",
                        len(current_rows),
                        next_check_delay,
                    )
                delivery_updates = list[tuple[OutboundDataFlag, int, int]]()
                for row in current_rows:
                    assert row.outbound_data_id is not None
                    url = self.reference_server_url + "/api/v1/tip-filter/matches"
                    headers = {
                        "Content-Type": "application/json",
                    }
                    batch_text = row.outbound_data.decode("utf-8")
                    updated_flags = row.outbound_data_flags
                    try:
                        async with self.aiohttp_session.post(
                            url, headers=headers, data=batch_text
                        ) as response:
                            if response.status == HTTPStatus.OK:
                                self.logger.debug(
                                    "Posted outbound data to reference server " + "status=%s, reason=%s",
                                    response.status,
                                    response.reason,
                                )
                                updated_flags |= OutboundDataFlag.DISPATCHED_SUCCESSFULLY
                            else:
                                self.logger.error(
                                    "Failed to post outbound data to reference "
                                    + "server status=%s, reason=%s",
                                    response.status,
                                    response.reason,
                                )
                    except aiohttp.ClientError:
                        self.logger.exception("Failed to post outbound data to reference server")

                    delivery_updates.append((updated_flags, int(time.time()), row.outbound_data_id))

                if len(delivery_updates) > 0:
                    await asyncio.get_running_loop().run_in_executor(
                        self.executor,
                        self.db_tip_filter_queries.update_outbound_data_last_tried_write,
                        delivery_updates,
                    )
            else:
                self.logger.debug(
                    "Outbound data delivery deferred due to lack of reference " "server connection"
                )

            try:
                await asyncio.wait_for(self._outbound_data_delivery_event.wait(), next_check_delay)
            except asyncio.TimeoutError:
                pass

    def dispatch_tip_filter_notifications(
        self,
        matches: list[PushdataRowParsed],
        block_hash: Optional[bytes],
        request_id: str,
    ) -> None:
        """
        Non-blocking delivery of new tip filter notifications.
        """
        self.logger.debug(
            "Starting task for dispatching tip filter notifications (%d)",
            len(matches),
        )
        future = asyncio.run_coroutine_threadsafe(
            self.dispatch_tip_filter_notifications_async(matches, block_hash, request_id),
            self._loop,
        )
        # Futures swallow exceptions so we must install a callback that raises any errors.
        future.add_done_callback(future_callback)

    async def dispatch_tip_filter_notifications_async(
        self,
        matches: list[PushdataRowParsed],
        block_hash: Optional[bytes],
        request_id: str,
    ) -> None:
        """
        Worker task for delivery of new tip filter notifications.
        """
        self.logger.debug("Entered task for dispatching tip filter notifications")
        matches_by_hash = dict[bytes, list[PushdataRowParsed]]()
        for match in matches:
            if match.pushdata_hash in matches_by_hash:
                matches_by_hash[match.pushdata_hash].append(match)
            else:
                matches_by_hash[match.pushdata_hash] = [match]

        # Get all the accounts and which pushdata they have registered.
        rows = self.db_tip_filter_queries.read_indexer_filtering_registrations_for_notifications(
            list(matches_by_hash)
        )
        self.logger.debug("Found %d registrations for tip filter notifications", len(rows))

        if len(rows) == 0:
            return

        # This also allows us to identify the false positive matches. This is not critical and
        # just for debugging/interest.
        invalid_pushdata_hashes = set(matches_by_hash) - set(row.pushdata_hash for row in rows)
        self.logger.debug(
            "Ignored %d false positive filter matches",
            len(invalid_pushdata_hashes),
        )

        # Gather the true matches for each account so that we can notify them of those matches.
        matches_by_account_id = dict[int, list[PushdataRowParsed]]()
        for row in rows:
            matched_rows = matches_by_hash[row.pushdata_hash]
            if row.account_id in matches_by_account_id:
                matches_by_account_id[row.account_id].extend(matched_rows)
            else:
                matches_by_account_id[row.account_id] = list(matched_rows)

        # TODO(1.4.0) Servers. Consider moving the callback metadata and the notifications made to
        #     it to the reference server. It can queue the results if they were not able to be
        #     delivered.
        metadata_by_account_id = {
            row.account_id: row
            for row in self.db_tip_filter_queries.read_account_metadata(list(matches_by_account_id))
        }
        block_id = hash_to_hex_str(block_hash) if block_hash is not None else None

        entries = list[TipFilterNotificationEntry]()
        for account_id, matched_rows in matches_by_account_id.items():
            if account_id not in metadata_by_account_id:
                self.logger.error(
                    "Account does not have peer channel callback set, " "account_id: %d for hashes: %s",
                    account_id,
                    [pdh.pushdata_hash for pdh in matched_rows],
                )
                continue

            account_metadata = metadata_by_account_id[account_id]
            self.logger.debug("Posting matches for peer channel account %s", account_metadata)
            request_data: TipFilterNotificationEntry = {
                "accountId": account_metadata.external_account_id,
                "matches": [
                    {
                        "pushDataHashHex": matched_row.pushdata_hash.hex(),
                        "transactionId": hash_to_hex_str(matched_row.tx_hash),
                        "transactionIndex": matched_row.idx,
                        "flags": matched_row.ref_type,
                    }
                    for matched_row in matched_rows
                ],
            }
            entries.append(request_data)

        batch: TipFilterNotificationBatch = {
            "blockId": block_id,
            "entries": entries,
        }

        url = self.reference_server_url + "/api/v1/tip-filter/matches"
        headers = {
            "Content-Type": "application/json",
        }
        if block_id:
            assert block_hash is not None
            new_header_event = self.pushdata_notification_can_send_event[block_hash]
            await new_header_event.wait()
        # else, it's a mempool notification
        try:
            async with self.aiohttp_session.post(url, headers=headers, json=batch) as response:
                if response.status == HTTPStatus.OK:
                    self.logger.debug(
                        "Posted outbound data to reference server " + "status=%s, reason=%s",
                        response.status,
                        response.reason,
                    )
                    return

                self.logger.error(
                    "Failed to post outbound data to reference server " + "status=%s, reason=%s",
                    response.status,
                    response.reason,
                )
        except aiohttp.ClientError:
            self.logger.exception("Failed to post outbound data to reference server")

        batch_json = json.dumps(batch)
        date_created = int(time.time())
        creation_row = OutboundDataRow(
            None,
            batch_json.encode(),
            OutboundDataFlag.TIP_FILTER_NOTIFICATIONS,
            date_created,
            date_created,
        )

        await asyncio.get_running_loop().run_in_executor(
            self.executor,
            self.db_tip_filter_queries.create_outbound_data_write,
            creation_row,
        )

    async def wait_for_new_tip(self) -> None:
        while self.is_alive:
            current_tip_height = self.headers_threadsafe.tip().height

            for_deletion = []
            for (
                block_hash,
                new_tip_event,
            ) in self.pushdata_notification_can_send_event.items():
                height = self.headers_threadsafe.get_header_for_hash(block_hash).height
                if height <= current_tip_height:
                    new_tip_event.set()
                for_deletion.append(block_hash)
            await asyncio.sleep(0.2)
            for block_hash in for_deletion:
                del self.pushdata_notification_can_send_event[block_hash]


async def client_session_ctx(app: web.Application) -> AsyncIterator[None]:
    """
    Cleanup context async generator to create and properly close aiohttp ClientSession
    Ref.:
        > https://docs.aiohttp.org/en/stable/web_advanced.html#cleanup-context
        > https://docs.aiohttp.org/en/stable/web_advanced.html#aiohttp-web-signals
        > https://docs.aiohttp.org/en/stable/web_advanced.html#data-sharing-aka-no-singletons-please
    """
    # logger.debug('Creating ClientSession')
    app["client_session"] = aiohttp.ClientSession()

    yield

    logger.debug("Closing ClientSession")
    await app["client_session"].close()


def get_aiohttp_app(
    lmdb: LMDB_Database,
    headers_threadsafe: HeadersAPIThreadsafe,
    net_config: NetworkConfig,
) -> tuple[web.Application, ApplicationState]:
    loop = asyncio.get_event_loop()
    app = web.Application()
    app.cleanup_ctx.append(client_session_ctx)
    app_state = ApplicationState(app, loop, lmdb, headers_threadsafe, net_config)

    # This is the standard aiohttp way of managing state within the handlers
    app["app_state"] = app_state

    # Non-optional APIs
    app.add_routes(
        [
            web.get("/", handlers_restoration.ping),
            web.get("/error", handlers_restoration.error),
            web.view("/ws", ReferenceServerWebSocket),
            web.get("/api/v1/chain/tips", handlers_headers.get_chain_tips),
            web.get(
                "/api/v1/chain/header/byHeight",
                handlers_headers.get_headers_by_height,
            ),
            web.get("/api/v1/chain/header/{hash}", handlers_headers.get_header),
            # These need to be registered before "/transaction/{txid}" to avoid clashes.
            web.post(
                "/api/v1/transaction/filter",
                handlers_tip_filter.indexer_post_transaction_filter,
            ),
            web.post(
                "/api/v1/transaction/filter:delete",
                handlers_tip_filter.indexer_post_transaction_filter_delete,
            ),
            # ----- RESTORATION API BEGIN ----- #
            web.get(
                "/api/v1/transaction/{txid}",
                handlers_restoration.get_transaction,
            ),
            web.get(
                "/api/v1/merkle-proof/{txid}",
                handlers_restoration.get_tsc_merkle_proof,
            ),
            web.post(
                "/api/v1/restoration/search",
                handlers_restoration.get_pushdata_filter_matches,
            ),
            web.post(
                "/api/v1/restoration/search_p2pkh",
                handlers_restoration.get_p2pkh_address_filter_matches,
            ),
            # ----- RESTORATION API END ----- #
            web.post("/api/v1/output-spend", handlers_tip_filter.get_output_spends),
            web.post(
                "/api/v1/output-spend/notifications",
                handlers_tip_filter.post_output_spend_notifications_register,
            ),
            web.post(
                "/api/v1/output-spend/notifications:unregister",
                handlers_tip_filter.post_output_spend_notifications_unregister,
            ),
        ]
    )
    return app, app_state
