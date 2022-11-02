from __future__ import annotations

import logging
import struct
import threading
import time
from datetime import datetime
from functools import partial
from queue import Queue

import zmq

from conduit_lib.zmq_sockets import connect_non_async_zmq_socket
from .flush_mempool_thread import FlushMempoolTransactionsThread
from ..types import MempoolTxAck
from conduit_lib.algorithms import parse_txs
from conduit_lib.database.mysql.types import MySQLFlushBatch
from conduit_lib.utils import zmq_recv_and_process_batchwise_no_block


class MempoolParsingThread(threading.Thread):

    def __init__(self, worker_id: int,
            mempool_tx_flush_queue: Queue[tuple[MySQLFlushBatch, MempoolTxAck]],
            daemon: bool=True) -> None:
        self.logger = logging.getLogger(f"mempool-parsing-thread-{worker_id}")
        self.logger.setLevel(logging.DEBUG)
        threading.Thread.__init__(self, daemon=daemon)

        self.worker_id = worker_id
        self.mempool_tx_flush_queue = mempool_tx_flush_queue

        self.zmq_context = zmq.Context[zmq.Socket[bytes]]()
        self.socket_mempool_tx = connect_non_async_zmq_socket(self.zmq_context,
            'tcp://127.0.0.1:55556', zmq.SocketType.PULL)
        self.socket_is_post_ibd = connect_non_async_zmq_socket(self.zmq_context,
            'tcp://127.0.0.1:52841', zmq.SocketType.SUB,
            options=[(zmq.SocketOption.SUBSCRIBE, b"is_ibd_signal")])

    def run(self) -> None:
        while True:
            # For some reason I am unable to catch a KeyboardInterrupt or SIGINT here so
            # need to rely on an overt "stop_signal" from the Controller for graceful shutdown
            message = self.socket_is_post_ibd.recv()
            if message == b"is_ibd_signal":
                self.logger.debug(f"Got initial block download signal. "
                    f"Starting mempool tx parsing thread.")
                break

        # Database flush threads
        t = FlushMempoolTransactionsThread(self.worker_id, self.mempool_tx_flush_queue)
        t.start()

        try:
            process_batch_func = partial(self.process_mempool_batch)

            zmq_recv_and_process_batchwise_no_block(
                sock=self.socket_mempool_tx,
                process_batch_func=process_batch_func,
                on_blocked_msg=None,
                batching_rate=0.3,
                poll_timeout_ms=100
            )
        except KeyboardInterrupt:
            return
        except Exception as e:
            self.logger.exception("Caught exception")
        finally:
            self.logger.info("Closing mined_blocks_thread")
            self.socket_mempool_tx.close()

    def process_mempool_batch(self, batch: list[bytes]) -> None:
        assert self.mempool_tx_flush_queue is not None
        tx_rows_batched, in_rows_batched, out_rows_batched, set_pd_rows_batched = [], [], [], []
        for msg in batch:
            msg_type, size_tx = struct.unpack_from(f"<II", msg)
            msg_type, size_tx, rawtx = struct.unpack(f"<II{size_tx}s", msg)
            # self.logger.debug(f"Got mempool tx: {hash_to_hex_str(double_sha256(rawtx))}")
            dt = datetime.utcnow()
            tx_offsets = [0]
            timestamp = int(time.time())
            tx_rows, tx_rows_mempool, in_rows, out_rows, set_pd_rows = parse_txs(rawtx,
                tx_offsets, timestamp, False, 0)
            tx_rows_batched.extend(tx_rows_mempool)
            in_rows_batched.extend(in_rows)
            out_rows_batched.extend(out_rows)
            set_pd_rows_batched.extend(set_pd_rows)

        num_mempool_txs_processed = len(tx_rows_batched)
        # self.logger.debug(f"Flushing {num_mempool_txs_processed} parsed mempool txs")
        self.mempool_tx_flush_queue.put(
            (MySQLFlushBatch([], tx_rows_batched, in_rows_batched, out_rows_batched,
                set_pd_rows_batched),
            num_mempool_txs_processed)
        )
