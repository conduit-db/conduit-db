import logging
import queue
import threading
import time
import zmq

from conduit_lib.database.mysql.types import MySQLFlushBatch
from conduit_lib import MySQLDatabase
from conduit_lib.database.mysql.mysql_database import mysql_connect
from conduit_lib.zmq_sockets import connect_non_async_zmq_socket

from ..types import ProcessedBlockAcks, MySQLFlushBatchWithAcks
from ..workers.common import reset_rows, maybe_refresh_mysql_connection, mysql_flush_rows_confirmed, \
    extend_batched_rows


BLOCKS_MAX_TX_BATCH_LIMIT = 200_000
BLOCK_BATCHING_RATE = 0.3


class FlushConfirmedTransactionsThread(threading.Thread):

    def __init__(self, worker_id: int,
            confirmed_tx_flush_queue: queue.Queue[tuple[MySQLFlushBatch, ProcessedBlockAcks]],
            daemon: bool = True) -> None:
        self.logger = logging.getLogger(f"mined-blocks-thread-{worker_id}")
        self.logger.setLevel(logging.DEBUG)
        threading.Thread.__init__(self, daemon=daemon)

        self.worker_id = worker_id
        self.confirmed_tx_flush_queue: queue.Queue[tuple[MySQLFlushBatch, ProcessedBlockAcks]] = \
            confirmed_tx_flush_queue

        self.zmq_context = zmq.Context[zmq.Socket[bytes]]()
        self.socket_mined_tx_ack: zmq.Socket[bytes] | None = None
        self.socket_mined_tx_parsed_ack: zmq.Socket[bytes] | None = None

        # A dedicated in-memory only table exclusive to this worker
        # it is frequently dropped and recreated for each chip-away batch
        self.inbound_tx_table_name = f'inbound_tx_table_{worker_id}'
        self.last_mysql_activity: int = int(time.time())

    def run(self) -> None:
        assert self.confirmed_tx_flush_queue is not None
        mysql_db: MySQLDatabase = mysql_connect(worker_id=self.worker_id)
        self.socket_mined_tx_ack = connect_non_async_zmq_socket(self.zmq_context,
            'tcp://127.0.0.1:55889', zmq.SocketType.PUSH, [(zmq.SocketOption.SNDHWM, 10000)])
        self.socket_mined_tx_parsed_ack = connect_non_async_zmq_socket(self.zmq_context,
            'tcp://127.0.0.1:54214', zmq.SocketType.PUSH, [(zmq.SocketOption.SNDHWM, 10000)])
        txs, txs_mempool, ins, outs, pds, acks = reset_rows()
        try:
            while True:
                try:
                    # Pre-IBD do large batched flushes
                    confirmed_rows, new_acks = self.confirmed_tx_flush_queue.get(
                        timeout=BLOCK_BATCHING_RATE)
                    if not confirmed_rows:  # poison pill
                        break

                    txs, txs_mempool, ins, outs, pds = extend_batched_rows(confirmed_rows, txs,
                        txs_mempool, ins, outs, pds)
                    acks.extend(new_acks)

                    if len(txs) > BLOCKS_MAX_TX_BATCH_LIMIT:
                        mysql_db, self.last_mysql_activity = maybe_refresh_mysql_connection(
                            mysql_db, self.last_mysql_activity, self.logger)
                        mysql_flush_rows_confirmed(self, MySQLFlushBatchWithAcks(txs, txs_mempool,
                            ins, outs, pds, acks),
                            mysql_db=mysql_db)
                        txs, txs_mempool, ins, outs, pds, acks = reset_rows()

                # Post-IBD
                except queue.Empty:
                    if len(txs) != 0:
                        mysql_db, self.last_mysql_activity = maybe_refresh_mysql_connection(
                            mysql_db, self.last_mysql_activity, self.logger)
                        mysql_flush_rows_confirmed(self, MySQLFlushBatchWithAcks(txs, txs_mempool,
                            ins, outs, pds, acks),
                            mysql_db=mysql_db)
                        txs, txs_mempool, ins, outs, pds, acks = reset_rows()
                    continue
        except KeyboardInterrupt:
            return
        except Exception as e:
            self.logger.exception("Caught exception")
            raise e
        finally:
            mysql_db.close()
            if self.socket_mined_tx_ack:
                self.socket_mined_tx_ack.close()
            if self.socket_mined_tx_parsed_ack:
                self.socket_mined_tx_parsed_ack.close()

