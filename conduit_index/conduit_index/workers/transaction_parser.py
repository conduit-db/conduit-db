import cbor2
import logging.handlers
import logging
import multiprocessing
import os
from pathlib import Path
import queue
import sys
import threading
import time
import refcuckoo
import zmq

from conduit_lib import MySQLDatabase
from conduit_lib.database.mysql.mysql_database import mysql_connect
from conduit_lib.database.mysql.types import InputRowParsed, MySQLFlushBatch, PushdataRowParsed
from conduit_lib.logging_client import setup_tcp_logging
from conduit_lib.types import OutpointType, output_spend_struct
from conduit_lib.zmq_sockets import connect_non_async_zmq_socket
from conduit_raw.conduit_raw.aiohttp_api.mysql_db_tip_filtering import MySQLTipFilterQueries
from conduit_raw.conduit_raw.aiohttp_api.types import CuckooResult, IndexerPushdataRegistrationFlag, \
    OutpointMessageType, OutpointStateUpdate, PushdataFilterMessageType, PushdataFilterStateUpdate
from .mempool_parsing_thread import MempoolParsingThread
from .mined_block_parsing_thread import MinedBlockParsingThread
from ..types import ProcessedBlockAcks, MempoolTxAck

MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))


class TxParser(multiprocessing.Process):
    """
    in: blk_hash, blk_height, blk_start_pos, blk_end_pos, tx_positions_div
    out: N/A - directly updates posgres database
    ack_confirmed: blk_hash, count_txs_done
    ack_mempool: tx_counts
    """

    def __init__(self, worker_id: int) -> None:
        super(TxParser, self).__init__()
        self.confirmed_tx_flush_queue: queue.Queue[tuple[MySQLFlushBatch, ProcessedBlockAcks]] | None = None
        self.mempool_tx_flush_queue: queue.Queue[tuple[MySQLFlushBatch, MempoolTxAck,
                list[InputRowParsed], list[PushdataRowParsed]]] | None = None

        self.last_mysql_activity: float = time.time()
        self.worker_id = worker_id

    def run(self) -> None:
        if sys.platform == 'win32':
            setup_tcp_logging(port=65421)
        self.logger = logging.getLogger(f"tx-parser-{self.worker_id}")
        self.logger.setLevel(logging.DEBUG)
        self.logger.info(f"Started {self.__class__.__name__}")

        # ZMQ Setup
        self.zmq_context = zmq.Context[zmq.Socket[bytes]]()
        self.kill_worker_socket = connect_non_async_zmq_socket(self.zmq_context,
            "tcp://127.0.0.1:63241", zmq.SocketType.SUB,
            options=[(zmq.SocketOption.SUBSCRIBE, b"stop_signal")])
        self.confirmed_tx_flush_queue = queue.Queue()
        self.mempool_tx_flush_queue = queue.Queue()

        self.setup_tip_filtering()
        try:
            main_thread = threading.Thread(target=self.main_thread, daemon=True)
            main_thread.start()

            assert self.confirmed_tx_flush_queue is not None
            assert self.mempool_tx_flush_queue is not None
            threads = [
                MinedBlockParsingThread(self, self.worker_id, self.confirmed_tx_flush_queue,
                    daemon=True),
                MempoolParsingThread(self, self.worker_id, self.mempool_tx_flush_queue,
                    daemon=True)
            ]
            for t in threads:
                t.start()

            for t in threads:
                t.join()
            self.logger.info(f"{self.__class__.__name__} exiting")
        except Exception:
            self.logger.exception("Caught exception")
            raise

    def main_thread(self) -> None:
        try:
            while True:
                # For some reason I am unable to catch a KeyboardInterrupt or SIGINT here so
                # need to rely on an overt "stop_signal" from the Controller for graceful shutdown
                message = self.kill_worker_socket.recv()
                if message == b"stop_signal":
                    break
                time.sleep(0.2)
        finally:
            self.logger.info(f"Process Stopped")
            sys.exit(0)

    def setup_tip_filtering(self) -> None:
        mysql_db: MySQLDatabase = mysql_connect(worker_id=self.worker_id)
        mysql_db_tip_filter_queries = MySQLTipFilterQueries(mysql_db)

        self.unspent_output_registrations: set[OutpointType] = set()

        # Populate the shared cuckoo filter with all existing registrations. Remember that the
        # filter handles duplicate registrations, and it is a lot easier to just register them
        # and unregister them for every account, than try and manage duplicates.
        # Note that at the time of writing the bits per item is 12 (compiled into `refcuckoo`).
        self.common_cuckoo = refcuckoo.CuckooFilter(500000)  # pylint: disable=I1101
        self._filter_expiry_next_time = int(time.time()) + 30
        registration_entries = mysql_db_tip_filter_queries.read_tip_filter_registrations(
            mask=IndexerPushdataRegistrationFlag.DELETING | IndexerPushdataRegistrationFlag.FINALISED,
            expected_flags=IndexerPushdataRegistrationFlag.FINALISED)

        # TODO: deduplicate pushdatas to avoid corrupting the filter
        for registration_entry in registration_entries:
            # TODO(1.4.0) Tip filter. Expiry dates should be tracked and entries removed.
            self.common_cuckoo.add(registration_entry.pushdata_hash)
        self.logger.debug("Populated the common cuckoo filter with %d entries",
            len(registration_entries))

        ZMQ_CONNECT_HOST = os.getenv('ZMQ_CONNECT_HOST', '127.0.0.1')
        self.logger.debug(f"Connecting zmq utxo and pushdata sockets on host: {ZMQ_CONNECT_HOST}")
        self.socket_utxo_spend_registrations = connect_non_async_zmq_socket(self.zmq_context,
            f'tcp://{ZMQ_CONNECT_HOST}:60000', zmq.SocketType.SUB,
            options=[(zmq.SocketOption.SUBSCRIBE, b'utxo_registration')])
        self.socket_utxo_spend_notifications = connect_non_async_zmq_socket(self.zmq_context,
            f'tcp://{ZMQ_CONNECT_HOST}:60001', zmq.SocketType.PUSH)
        self.socket_pushdata_registrations = connect_non_async_zmq_socket(self.zmq_context,
            f'tcp://{ZMQ_CONNECT_HOST}:60002', zmq.SocketType.SUB,
            options=[(zmq.SocketOption.SUBSCRIBE, b'pushdata_registration')])
        self.socket_pushdata_notifications = connect_non_async_zmq_socket(self.zmq_context,
            f'tcp://{ZMQ_CONNECT_HOST}:60003', zmq.SocketType.PUSH)

        request_id = os.urandom(16).hex()
        state_update_to_server = OutpointStateUpdate(request_id, OutpointMessageType.READY, None,
            None, self.worker_id)
        self.socket_utxo_spend_notifications.send(cbor2.dumps(
            state_update_to_server))
        # It would be redundant to send a "READY" signal for socket_pushdata_notifications

    def send_utxo_spend_notifications(self, in_rows_parsed: list[InputRowParsed],
            blk_hash: bytes | None) -> None:
        # Send UTXO spend notifications
        for input_row in in_rows_parsed:
            spent_output = OutpointType(input_row.out_tx_hash, input_row.out_idx)
            if spent_output in self.unspent_output_registrations:
                request_id = os.urandom(16).hex()
                notification = OutpointStateUpdate(request_id, OutpointMessageType.SPEND, None,
                    output_spend_struct.pack(*input_row, blk_hash), self.worker_id)
                self.socket_utxo_spend_notifications.send(cbor2.dumps(notification))

    def send_pushdata_match_notifications(self,
            pushdata_matches_tip_filter: list[PushdataRowParsed], blk_hash: bytes | None) -> None:
        # Send Cuckoo Filter match notifications
        filter_matches = []
        for pushdata_match in pushdata_matches_tip_filter:
            result = self.common_cuckoo.contains(pushdata_match.pushdata_hash)
            if result == CuckooResult.OK:
                filter_matches.append(pushdata_match)
            else:
                # TODO: DEBUGGING - MUST REMOVE - THIS IS VERY WASTEFUL
                assert self.common_cuckoo.contains(pushdata_match.pushdata_hash) == CuckooResult.NOT_FOUND

        if len(filter_matches):
            # NOTE: if there is message delivery failure, the acks
            #  below should not be allowed to occur which will result in a redo of the same
            #  blocks on startup when it performs a "repair"
            # ZMQ send over websocket fitler_matches
            request_id = os.urandom(16).hex()
            notification_pushdata = PushdataFilterStateUpdate(request_id,
                PushdataFilterMessageType.NOTIFICATION, [], filter_matches, blk_hash)
            self.logger.debug(f"Sending {len(filter_matches)} pushdata matches")
            self.socket_pushdata_notifications.send(cbor2.dumps(notification_pushdata))

