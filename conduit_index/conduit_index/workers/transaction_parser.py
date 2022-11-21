import logging.handlers
import logging
import multiprocessing
import os
from pathlib import Path
import queue
import sys
import threading
import time
import zmq

from conduit_lib.database.mysql.types import MySQLFlushBatch
from conduit_lib.logging_client import setup_tcp_logging
from conduit_lib.zmq_sockets import connect_non_async_zmq_socket
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
        self.mempool_tx_flush_queue: queue.Queue[tuple[MySQLFlushBatch, MempoolTxAck]] | None = None

        self.last_mysql_activity: float = time.time()
        self.worker_id = worker_id

    def run(self) -> None:
        if sys.platform == 'win32':
            setup_tcp_logging(port=65421)
        self.logger = logging.getLogger(f"tx-parser-{self.worker_id}")
        self.logger.setLevel(logging.DEBUG)
        self.logger.info(f"Started {self.__class__.__name__}")

        self.zmq_context = zmq.Context[zmq.Socket[bytes]]()
        self.kill_worker_socket = connect_non_async_zmq_socket(self.zmq_context,
            "tcp://127.0.0.1:63241", zmq.SocketType.SUB,
            options=[(zmq.SocketOption.SUBSCRIBE, b"stop_signal")])

        self.confirmed_tx_flush_queue = queue.Queue()
        self.mempool_tx_flush_queue = queue.Queue()
        try:
            main_thread = threading.Thread(target=self.main_thread, daemon=True)
            main_thread.start()

            assert self.confirmed_tx_flush_queue is not None
            assert self.mempool_tx_flush_queue is not None
            threads = [
                MinedBlockParsingThread(self.worker_id, self.confirmed_tx_flush_queue, daemon=True),
                MempoolParsingThread(self.worker_id, self.mempool_tx_flush_queue, daemon=True)
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
