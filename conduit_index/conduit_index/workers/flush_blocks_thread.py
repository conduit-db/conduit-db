import logging
import queue
import threading
import time
import typing
import zmq

from conduit_lib import DBInterface
from conduit_lib.database.db_interface.types import (
    MySQLFlushBatch,
    ProcessedBlockAcks,
    TipFilterNotifications,
    DBFlushBatchWithAcks,
)
from conduit_lib.zmq_sockets import connect_non_async_zmq_socket

from ..workers.common import (
    reset_rows,
    maybe_refresh_connection,
    flush_rows_confirmed,
    extend_batched_rows,
)

if typing.TYPE_CHECKING:
    from .transaction_parser import TxParser

BLOCKS_MAX_TX_BATCH_LIMIT = 200_000
BLOCK_BATCHING_RATE = 0.3


class FlushConfirmedTransactionsThread(threading.Thread):
    def __init__(
        self,
        parent: "TxParser",
        worker_id: int,
        confirmed_tx_flush_queue: queue.Queue[
            tuple[MySQLFlushBatch, ProcessedBlockAcks, TipFilterNotifications]
        ],
        daemon: bool = True,
    ) -> None:
        self.parent = parent
        self.logger = logging.getLogger(f"mined-blocks-thread-{worker_id}")
        self.logger.setLevel(logging.DEBUG)
        threading.Thread.__init__(self, daemon=daemon)

        self.worker_id = worker_id
        self.confirmed_tx_flush_queue: queue.Queue[
            tuple[MySQLFlushBatch, ProcessedBlockAcks, TipFilterNotifications]
        ] = confirmed_tx_flush_queue

        self.zmq_context = zmq.Context[zmq.Socket[bytes]]()
        self.socket_mined_tx_ack: zmq.Socket[bytes] | None = None
        self.socket_mined_tx_parsed_ack: zmq.Socket[bytes] | None = None

        # A dedicated in-memory only table exclusive to this worker
        # it is frequently dropped and recreated for each chip-away batch
        self.inbound_tx_table_name = f"inbound_tx_table_{worker_id}"
        self.last_activity: int = int(time.time())

    def run(self) -> None:
        assert self.confirmed_tx_flush_queue is not None
        db: DBInterface = DBInterface.load_db(worker_id=self.worker_id)
        self.socket_mined_tx_ack = connect_non_async_zmq_socket(
            self.zmq_context,
            "tcp://127.0.0.1:55889",
            zmq.SocketType.PUSH,
            [(zmq.SocketOption.SNDHWM, 10000)],
        )
        self.socket_mined_tx_parsed_ack = connect_non_async_zmq_socket(
            self.zmq_context,
            "tcp://127.0.0.1:54214",
            zmq.SocketType.PUSH,
            [(zmq.SocketOption.SNDHWM, 10000)],
        )
        txs, txs_mempool, ins, outs, pds, acks = reset_rows()
        all_tip_filter_notifications: list[TipFilterNotifications] = []
        try:
            while True:
                try:
                    # Pre-IBD do large batched flushes
                    (
                        confirmed_rows,
                        new_acks,
                        tip_filter_notifications,
                    ) = self.confirmed_tx_flush_queue.get(timeout=BLOCK_BATCHING_RATE)
                    if not confirmed_rows:  # poison pill
                        break

                    txs, txs_mempool, ins, outs, pds = extend_batched_rows(
                        confirmed_rows, txs, txs_mempool, ins, outs, pds
                    )
                    acks.extend(new_acks)
                    all_tip_filter_notifications.append(tip_filter_notifications)

                    if len(txs) > BLOCKS_MAX_TX_BATCH_LIMIT:
                        (
                            db,
                            self.last_activity,
                        ) = maybe_refresh_connection(db, self.last_activity, self.logger)
                        flush_rows_confirmed(
                            self,
                            DBFlushBatchWithAcks(txs, txs_mempool, ins, outs, pds, acks),
                            db=db,
                        )
                        for tfn in all_tip_filter_notifications:
                            self.parent.send_utxo_spend_notifications(tfn.utxo_spends, tfn.block_hash)
                            self.parent.send_pushdata_match_notifications(
                                tfn.pushdata_matches, tfn.block_hash
                            )
                        txs, txs_mempool, ins, outs, pds, acks = reset_rows()
                        all_tip_filter_notifications = []

                # Post-IBD
                except queue.Empty:
                    if len(txs) != 0:
                        (
                            db,
                            self.last_activity,
                        ) = maybe_refresh_connection(db, self.last_activity, self.logger)
                        flush_rows_confirmed(
                            self,
                            DBFlushBatchWithAcks(txs, txs_mempool, ins, outs, pds, acks),
                            db=db,
                        )
                        txs, txs_mempool, ins, outs, pds, acks = reset_rows()
                    for tfn in all_tip_filter_notifications:
                        self.parent.send_utxo_spend_notifications(tfn.utxo_spends, tfn.block_hash)
                        self.parent.send_pushdata_match_notifications(tfn.pushdata_matches, tfn.block_hash)
                    all_tip_filter_notifications = []
                    continue
        except KeyboardInterrupt:
            return
        except Exception as e:
            self.logger.exception("Caught exception")
            raise e
        finally:
            db.close()
            if self.socket_mined_tx_ack:
                self.socket_mined_tx_ack.close()
            if self.socket_mined_tx_parsed_ack:
                self.socket_mined_tx_parsed_ack.close()
