import array
from functools import partial
import logging
import struct
import threading
import time
from queue import Queue
import typing
import zmq

from conduit_lib.database.db_interface.types import (
    MySQLFlushBatch,
    MempoolTxAck,
    InputRowParsed,
    PushdataRowParsed,
)
from conduit_lib.zmq_sockets import connect_non_async_zmq_socket
from .common import (
    convert_pushdata_rows_for_flush,
    convert_input_rows_for_flush,
)
from .flush_mempool_thread import FlushMempoolTransactionsThread
from conduit_lib.algorithms import parse_txs
from conduit_lib.utils import zmq_recv_and_process_batchwise_no_block

if typing.TYPE_CHECKING:
    from .transaction_parser import TxParser


class MempoolParsingThread(threading.Thread):
    def __init__(
        self,
        parent: "TxParser",
        worker_id: int,
        mempool_tx_flush_queue: Queue[
            tuple[
                MySQLFlushBatch,
                MempoolTxAck,
                list[InputRowParsed],
                list[PushdataRowParsed],
            ]
        ],
        daemon: bool = True,
    ) -> None:
        self.logger = logging.getLogger(f"mempool-parsing-thread-{worker_id}")
        self.logger.setLevel(logging.DEBUG)
        threading.Thread.__init__(self, daemon=daemon)

        self.parent = parent
        self.worker_id = worker_id
        self.mempool_tx_flush_queue = mempool_tx_flush_queue

        self.zmq_context = zmq.Context[zmq.Socket[bytes]]()
        self.socket_mempool_tx = connect_non_async_zmq_socket(
            self.zmq_context, "tcp://127.0.0.1:55556", zmq.SocketType.PULL
        )
        self.socket_is_post_ibd = connect_non_async_zmq_socket(
            self.zmq_context,
            "tcp://127.0.0.1:52841",
            zmq.SocketType.SUB,
            options=[(zmq.SocketOption.SUBSCRIBE, b"is_ibd_signal")],
        )

    def run(self) -> None:
        while True:
            # For some reason I am unable to catch a KeyboardInterrupt or SIGINT here so
            # need to rely on an overt "stop_signal" from the Controller for graceful shutdown
            message = self.socket_is_post_ibd.recv()
            if message == b"is_ibd_signal":
                self.logger.debug(
                    f"Got initial block download signal. " f"Starting mempool tx parsing thread."
                )
                break

        # Database flush threads
        t = FlushMempoolTransactionsThread(self.parent, self.worker_id, self.mempool_tx_flush_queue)
        t.start()

        try:
            process_batch_func = partial(self.process_mempool_batch)

            zmq_recv_and_process_batchwise_no_block(
                sock=self.socket_mempool_tx,
                process_batch_func=process_batch_func,
                on_blocked_msg=None,
                batching_rate=0.3,
                poll_timeout_ms=100,
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
        utxo_spends: list[InputRowParsed] = []
        pushdata_matches_tip_filter: list[PushdataRowParsed] = []
        (
            tx_rows_batched,
            in_rows_batched,
            set_pd_rows_batched,
        ) = (
            [],
            [],
            [],
        )
        for msg in batch:
            msg_type, size_tx = struct.unpack_from(f"<II", msg)
            msg_type, size_tx, rawtx = struct.unpack(f"<II{size_tx}s", msg)
            # self.logger.debug(f"Got mempool tx: {hash_to_hex_str(double_sha256(rawtx))}")
            tx_offsets = array.array("Q", [0])
            timestamp = int(time.time())
            (
                tx_rows,
                tx_rows_mempool,
                in_rows,
                pd_rows,
                utxo_spends,
                pushdata_matches_tip_filter,
            ) = parse_txs(rawtx, tx_offsets, timestamp, False, 0)
            pushdata_rows_for_flushing = convert_pushdata_rows_for_flush(pd_rows)
            input_rows_for_flushing = convert_input_rows_for_flush(in_rows)
            tx_rows_batched.extend(tx_rows_mempool)
            in_rows_batched.extend(input_rows_for_flushing)
            set_pd_rows_batched.extend(pushdata_rows_for_flushing)

        num_mempool_txs_processed = len(tx_rows_batched)
        # self.logger.debug(f"Flushing {num_mempool_txs_processed} parsed mempool txs")
        self.mempool_tx_flush_queue.put(
            (
                MySQLFlushBatch(
                    [],
                    tx_rows_batched,
                    in_rows_batched,
                    set_pd_rows_batched,
                ),
                num_mempool_txs_processed,
                utxo_spends,
                pushdata_matches_tip_filter,
            )
        )
