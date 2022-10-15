from __future__ import annotations

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
from conduit_lib.stack_tracer import trace_start, trace_stop
from .flush_mempool_thread import FlushMempoolTransactionsThread
from .flush_blocks_thread import FlushConfirmedTransactionsThread
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
        # PUB-SUB from Controller to worker to kill the worker
        context3 = zmq.Context[zmq.Socket[bytes]]()
        self.kill_worker_socket = context3.socket(zmq.SUB)
        self.kill_worker_socket.connect("tcp://127.0.0.1:63241")
        self.kill_worker_socket.setsockopt(zmq.SUBSCRIBE, b"stop_signal")

        if sys.platform == 'win32':
            setup_tcp_logging(port=65421)
        self.logger = logging.getLogger(f"tx-parser-{self.worker_id}")
        self.logger.setLevel(logging.DEBUG)
        self.logger.info(f"Started {self.__class__.__name__}")

        self.confirmed_tx_flush_queue = queue.Queue()
        self.mempool_tx_flush_queue = queue.Queue()
        try:
            if int(os.environ.get('RUN_STACK_TRACER', '0')):
                trace_start(str(MODULE_DIR / f"stack_tracing_{os.urandom(4).hex()}.html"))
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

            # Database flush threads
            threads = [
                FlushConfirmedTransactionsThread(self.worker_id, self.confirmed_tx_flush_queue),
                FlushMempoolTransactionsThread(self.worker_id, self.mempool_tx_flush_queue),
            ]
            for t in threads:
                t.start()
            for t in threads:
                t.join()
            self.logger.info(f"{self.__class__.__name__} exiting")
        except Exception:
            self.logger.exception("Caught exception")
            raise
        finally:
            if int(os.environ.get('RUN_STACK_TRACER', '0')):
                trace_stop()

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
