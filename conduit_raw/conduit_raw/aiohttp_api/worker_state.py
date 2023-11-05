import asyncio
import logging
import os
import time
import typing
from pathlib import Path

import cbor2
import zmq
import zmq.asyncio
from bitcoinx import hash_to_hex_str

from conduit_lib.constants import ZERO_HASH
from conduit_lib.database.db_interface.tip_filter import TipFilterQueryAPI
from conduit_lib.database.db_interface.tip_filter_types import OutputSpendRow, \
    TipFilterRegistrationEntry
from conduit_lib.database.db_interface.types import PushdataRowParsed
from conduit_lib.types import outpoint_struct, OutpointType, output_spend_struct
from conduit_lib.utils import create_task, zmq_send_no_block_async
from conduit_lib.zmq_sockets import bind_async_zmq_socket
from .constants import UTXO_REGISTRATION_TOPIC, PUSHDATA_REGISTRATION_TOPIC

from .types import (
    OutpointStateUpdate,
    OutpointMessageType,
    RequestId,
    PushdataFilterStateUpdate,
    PushdataFilterMessageType,
    BackendWorkerOfflineError,
)

if typing.TYPE_CHECKING:
    from .server import ApplicationState

MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))

# Silence verbose logging
logger = logging.getLogger("server")


class WorkerStateManager:
    def __init__(
        self,
        app_state: "ApplicationState",
        db_tip_filter_queries: TipFilterQueryAPI,
    ) -> None:
        self.app_state = app_state
        self.db = db_tip_filter_queries
        # Tip filtering API
        self.zmq_async_context = self.app_state.zmq_context
        ZMQ_BIND_HOST = os.getenv("ZMQ_BIND_HOST", "127.0.0.1")
        logger.debug(f"Binding zmq utxo and pushdata sockets on host: {ZMQ_BIND_HOST}")
        self.socket_utxo_spend_registrations = bind_async_zmq_socket(
            self.zmq_async_context,
            f"tcp://{ZMQ_BIND_HOST}:60000",
            zmq.SocketType.PUB,
        )
        self.socket_utxo_spend_notifications = bind_async_zmq_socket(
            self.zmq_async_context,
            f"tcp://{ZMQ_BIND_HOST}:60001",
            zmq.SocketType.PULL,
        )
        self.socket_pushdata_registrations = bind_async_zmq_socket(
            self.zmq_async_context,
            f"tcp://{ZMQ_BIND_HOST}:60002",
            zmq.SocketType.PUB,
        )
        self.socket_pushdata_notifications = bind_async_zmq_socket(
            self.zmq_async_context,
            f"tcp://{ZMQ_BIND_HOST}:60003",
            zmq.SocketType.PULL,
        )

        self.WORKER_COUNT_TX_PARSERS = int(os.getenv("WORKER_COUNT_TX_PARSERS", "4"))
        self._utxo_spend_inbound_queue: asyncio.Queue[OutpointStateUpdate] = asyncio.Queue()
        self._pushdata_inbound_queue: asyncio.Queue[PushdataFilterStateUpdate] = asyncio.Queue()

        self.expected_ack_outpoint_count_map: dict[RequestId, int] = {}
        self.expected_ack_pushdata_count_map: dict[RequestId, int] = {}
        self.expected_ack_outpoint_map: dict[RequestId, set[OutpointType]] = {}
        self.expected_ack_pushdata_map: dict[RequestId, set[bytes]] = {}
        self.all_workers_acked_event_map: dict[RequestId, asyncio.Event] = {}

        self.worker_ready_event_map: dict[int, asyncio.Event] = {}  # worker_id: event
        for worker_id in range(1, self.WORKER_COUNT_TX_PARSERS + 1):
            self.worker_ready_event_map[worker_id] = asyncio.Event()
        self.all_workers_connected_event: asyncio.Event = asyncio.Event()

    def close(self) -> None:
        self.socket_utxo_spend_registrations.close()
        self.socket_utxo_spend_notifications.close()
        self.socket_pushdata_registrations.close()
        self.socket_pushdata_notifications.close()

    def spawn_tasks(self) -> None:
        self.app_state.tasks = [
            create_task(self.listen_for_utxo_spend_notifications_async()),
            create_task(self.listen_for_pushdata_notifications_async()),
        ]

    async def wait_for_output_spend_worker_acks(
        self, request_id: RequestId, outpoints: list[OutpointType]
    ) -> None:
        """Ensure that all workers ACK for receiving the new outpoint registrations"""
        expected_total_ack_count = self.WORKER_COUNT_TX_PARSERS * len(outpoints)
        self.expected_ack_outpoint_count_map[request_id] = expected_total_ack_count
        self.expected_ack_outpoint_map[request_id] = set(outpoints)
        timeout = 10.0
        start_time = time.time()
        while True:
            for i in range(expected_total_ack_count):
                try:
                    notification: OutpointStateUpdate = self._utxo_spend_inbound_queue.get_nowait()
                    # In theory there could be cross-talk between aiohttp async handlers
                    # so the PUB/SUB messages could contain notifications for different request_ids
                    self.expected_ack_outpoint_count_map[notification.request_id] -= 1
                    if notification.request_id == request_id:
                        assert notification.outpoint is not None
                        outpoint = OutpointType.from_outpoint_struct(notification.outpoint)
                        assert outpoint in self.expected_ack_outpoint_map[request_id]

                    if self.expected_ack_outpoint_count_map[request_id] == 0:
                        return
                except asyncio.QueueEmpty:
                    if time.time() - start_time > timeout:
                        logger.error(
                            "Waited over %s seconds for workers "
                            "to acknowkedge the state update, but got no response" % timeout
                        )
                        raise BackendWorkerOfflineError(
                            "Waited over %s seconds for workers "
                            "to acknowkedge the state update, but got no response" % timeout
                        )
                    await asyncio.sleep(0.1)

    async def register_output_spend_notifications(
        self, outpoints: list[OutpointType]
    ) -> list[OutputSpendRow]:
        """raises `BackendWorkerOfflineError"""
        # TODO Persist to database in case of server restart
        # TODO This should all be relative to a client account_id
        timeout = 10.0
        try:
            request_id = os.urandom(16).hex()
            for outpoint in outpoints:
                outpoint_struct.pack(outpoint.tx_hash, outpoint.out_idx)
                msg = OutpointStateUpdate(
                    request_id,
                    OutpointMessageType.REGISTER,
                    outpoint_struct.pack(outpoint.tx_hash, outpoint.out_idx),
                    None,
                    None,
                )
                await zmq_send_no_block_async(
                    self.socket_utxo_spend_registrations,
                    UTXO_REGISTRATION_TOPIC + cbor2.dumps(msg),
                    timeout=timeout,
                )
        except TimeoutError:
            logger.error(
                "Waited over %s seconds for workers "
                "to acknowledge the output spend state update, but got no response" % timeout
            )
            raise BackendWorkerOfflineError(
                "Waited over %s seconds for workers to acknowledge"
                "the state update, but got no response" % timeout
            )
        await self.wait_for_output_spend_worker_acks(request_id, outpoints)
        # Subsequent notifications are send via the web socket.
        logger.debug(f"Successfully sent utxo registrations for {len(outpoints)} outpoints")
        return self.db.get_spent_outpoints(outpoints, self.app_state.lmdb)

    async def unregister_output_spend_notifications(self, outpoints: list[OutpointType]) -> None:
        """raises `BackendWorkerOfflineError"""
        # TODO Persist to database in case of server restart
        # TODO This should all be relative to a client account_id
        timeout = 10.0
        try:
            request_id = os.urandom(16).hex()
            for outpoint in outpoints:
                outpoint_struct.pack(outpoint.tx_hash, outpoint.out_idx)
                msg = OutpointStateUpdate(
                    request_id,
                    OutpointMessageType.UNREGISTER,
                    outpoint_struct.pack(outpoint.tx_hash, outpoint.out_idx),
                    None,
                    None,
                )
                await zmq_send_no_block_async(
                    self.socket_utxo_spend_registrations,
                    UTXO_REGISTRATION_TOPIC + cbor2.dumps(msg),
                    timeout=timeout,
                )
        except TimeoutError:
            logger.error(
                "Waited over %s seconds for workers "
                "to acknowledge the output spend state update, but got no response" % timeout
            )
            raise BackendWorkerOfflineError(
                "Waited over %s seconds for workers to acknowkedge"
                "the state update, but got no response" % timeout
            )
        await self.wait_for_output_spend_worker_acks(request_id, outpoints)
        logger.debug(f"Successfully sent utxo unregistrations for {len(outpoints)} outpoints")

    async def clear_output_spend_notifications(self) -> None:
        """raises `BackendWorkerOfflineError"""
        # TODO Persist to database in case of server restart
        # TODO This should all be relative to a client account_id
        timeout = 10.0
        try:
            request_id = os.urandom(16).hex()
            msg = OutpointStateUpdate(request_id, OutpointMessageType.CLEAR_ALL, None, None, None)
            await zmq_send_no_block_async(
                self.socket_utxo_spend_registrations,
                UTXO_REGISTRATION_TOPIC + cbor2.dumps(msg),
                timeout=timeout,
            )
        except TimeoutError:
            logger.error(
                "Waited over %s seconds for workers "
                "to acknowledge the output spend state update, but got no response" % timeout
            )
            raise BackendWorkerOfflineError(
                "Waited over %s seconds for workers to acknowledge                    "
                "the state update, but got no response" % timeout
            )

        # Fake outpoints are to make the ack accounting work in `wait_for_output_spend_worker_acks`
        fake_outpoints = []
        for i in range(self.WORKER_COUNT_TX_PARSERS):
            fake_outpoints.append(OutpointType(tx_hash=b"", out_idx=0))
        await self.wait_for_output_spend_worker_acks(request_id, fake_outpoints)

    async def wait_for_pushdata_worker_acks(
        self,
        request_id: RequestId,
        pushdata_registrations: list[TipFilterRegistrationEntry],
    ) -> None:
        """Ensure that all workers ACK for receiving the new outpoint registrations"""
        expected_total_ack_count = self.WORKER_COUNT_TX_PARSERS * len(pushdata_registrations)
        self.expected_ack_pushdata_count_map[request_id] = expected_total_ack_count
        self.expected_ack_pushdata_map[request_id] = set(
            [registration.pushdata_hash for registration in pushdata_registrations]
        )

        timeout = 10.0
        start_time = time.time()
        while True:
            try:
                notification = self._pushdata_inbound_queue.get_nowait()

                # In theory there could be cross-talk between aiohttp async handlers
                # so the PUB/SUB messages could contain notifications for different request_ids
                self.expected_ack_pushdata_count_map[notification.request_id] -= len(notification.entries)
                if notification.request_id == request_id:
                    for entry in notification.entries:
                        entry_obj = TipFilterRegistrationEntry(*entry)
                        assert entry_obj.pushdata_hash in self.expected_ack_pushdata_map[request_id]

                if self.expected_ack_pushdata_count_map[request_id] == 0:
                    return
            except asyncio.QueueEmpty:
                if time.time() - start_time > timeout:
                    logger.error(
                        "Waited over %s seconds for workers "
                        "to acknowledge the pushdata state update, but got no response" % timeout
                    )
                    raise BackendWorkerOfflineError(
                        "Waited over %s seconds for workers to "
                        "acknowledge the pushdata state update, but got no response" % timeout
                    )
                await asyncio.sleep(0.1)
                logger.debug(f"Waiting for worker ACKs for new pushdata registrations")

    async def register_pushdata_notifications(
        self, pushdata_registrations: list[TipFilterRegistrationEntry]
    ) -> None:
        """raises `BackendWorkerOfflineError"""
        # TODO Persist to database in case of server restart
        # TODO This should all be relative to a client account_id
        timeout = 10.0
        try:
            request_id = os.urandom(16).hex()
            msg = PushdataFilterStateUpdate(
                request_id,
                PushdataFilterMessageType.REGISTER,
                pushdata_registrations,
                [],
                ZERO_HASH,
            )
            await zmq_send_no_block_async(
                self.socket_pushdata_registrations,
                PUSHDATA_REGISTRATION_TOPIC + cbor2.dumps(msg),
                timeout=timeout,
            )
        except TimeoutError:
            logger.error(
                "Waited over %s seconds for workers "
                "to acknowledge the pushdata state update, but got no response" % timeout
            )
            raise BackendWorkerOfflineError(
                "Waited over %s seconds for workers to acknowkedge"
                "the state update, but got no response" % timeout
            )
        await self.wait_for_pushdata_worker_acks(request_id, pushdata_registrations)

    async def unregister_pushdata_notifications(
        self, pushdata_registrations: list[TipFilterRegistrationEntry]
    ) -> None:
        """raises `BackendWorkerOfflineError"""
        # TODO Persist to database in case of server restart
        # TODO This should all be relative to a client account_id
        timeout = 10.0
        try:
            request_id = os.urandom(16).hex()
            msg = PushdataFilterStateUpdate(
                request_id,
                PushdataFilterMessageType.UNREGISTER,
                pushdata_registrations,
                [],
                ZERO_HASH,
            )
            await zmq_send_no_block_async(
                self.socket_pushdata_registrations,
                PUSHDATA_REGISTRATION_TOPIC + cbor2.dumps(msg),
                timeout=timeout,
            )
        except TimeoutError:
            logger.error(
                "Waited over %s seconds for workers "
                "to acknowledge the pushdata state update, but got no response" % timeout
            )
            raise BackendWorkerOfflineError(
                "Waited over %s seconds for workers to acknowkedge"
                "the state update, but got no response" % timeout
            )
        await self.wait_for_pushdata_worker_acks(request_id, pushdata_registrations)

    def _broadcast_spent_output_event(self, output_spend: bytes) -> None:
        # For debugging only
        (
            out_tx_hash,
            out_idx,
            in_tx_hash,
            in_idx,
            block_hash,
        ) = output_spend_struct.unpack(output_spend)
        logger.debug(
            "Broadcasting spent output event for %s:%d",
            hash_to_hex_str(out_tx_hash),
            out_idx,
        )
        # We do not provide any kind of message envelope at this time as this is the only
        # kind of message we send.
        self.app_state.ws_queue.put_nowait(output_spend)

    async def listen_for_utxo_spend_notifications_async(self) -> None:
        logger.debug(f"Waiting for all 'TxParser' workers to give 'READY' signal")
        while True:
            msg = await self.socket_utxo_spend_notifications.recv()
            if not msg:
                raise ValueError("Workers should never send empty zmq messages")
            notification: OutpointStateUpdate = OutpointStateUpdate(*cbor2.loads(msg))
            logger.debug(f"Got notification: {notification}")
            if notification.command & OutpointMessageType.READY:
                assert notification.worker_id is not None
                self.worker_ready_event_map[notification.worker_id].set()
                if all([event.is_set() for event in self.worker_ready_event_map.values()]):
                    self.all_workers_connected_event.set()
                    logger.debug(f"All backend 'TxParser' workers are ready")
            elif notification.command & OutpointMessageType.ACK:
                self._utxo_spend_inbound_queue.put_nowait(notification)
            elif notification.command & OutpointMessageType.SPEND:
                assert notification.output_spend is not None
                self._broadcast_spent_output_event(notification.output_spend)

    async def listen_for_pushdata_notifications_async(self) -> None:
        while True:
            msg = await self.socket_pushdata_notifications.recv()
            if not msg:
                raise ValueError("Workers should never send empty zmq messages")
            notification = PushdataFilterStateUpdate(*cbor2.loads(msg))
            logger.debug(f"Got notification: {notification}")
            if notification.command & PushdataFilterMessageType.ACK:
                self._pushdata_inbound_queue.put_nowait(notification)
            elif notification.command & PushdataFilterMessageType.NOTIFICATION:
                matches = []
                for match in notification.matches:
                    matches.append(PushdataRowParsed(*match))

                if notification.block_hash is not None:
                    local_new_tip_event = asyncio.Event()
                    if (
                        self.app_state.pushdata_notification_can_send_event.get(notification.block_hash)
                        is None
                    ):
                        self.app_state.pushdata_notification_can_send_event[
                            notification.block_hash
                        ] = local_new_tip_event
                # else, it's a mempool notification

                self.app_state.dispatch_tip_filter_notifications(
                    matches, notification.block_hash, notification.request_id
                )
