from __future__ import annotations

import logging
import queue
import threading
import time
import zmq

from ..types import ProcessedBlockAcks, MySQLFlushBatchWithAcks
from ..workers.common import reset_rows, maybe_refresh_mysql_connection, \
    mysql_flush_rows_confirmed, extend_batched_rows

from conduit_lib.database.mysql.types import MySQLFlushBatch
from conduit_lib import MySQLDatabase
from conduit_lib.database.mysql.mysql_database import mysql_connect


# accumulate x seconds worth of txs or MAX_TX_BATCH_SIZE (whichever comes first)
# TODO - during initial block download - should NOT rely on a timeout at all
#  Should just keep on pumping the entire batch of blocks as fast as possible to
#  Max out CPU.
#  This ideally requires reliable PUB/SUB to do properly
BLOCKS_MAX_TX_BATCH_LIMIT = 200_000
BLOCK_BATCHING_RATE = 0.3


class FlushConfirmedTransactionsThread(threading.Thread):

    def __init__(self, worker_id: int,
            confirmed_tx_flush_queue: queue.Queue[tuple[MySQLFlushBatch, ProcessedBlockAcks]],
            daemon: bool = True) -> None:
        self.logger = logging.getLogger("mined-blocks-thread")
        self.logger.setLevel(logging.DEBUG)
        threading.Thread.__init__(self, daemon=daemon)

        self.worker_id = worker_id
        self.confirmed_tx_flush_queue: queue.Queue[tuple[MySQLFlushBatch, ProcessedBlockAcks]] = \
            confirmed_tx_flush_queue

        self.ack_for_mined_tx_socket: zmq.Socket[bytes] | None = None

        context = zmq.Context[zmq.Socket[bytes]]()
        self.tx_parse_ack_socket = context.socket(zmq.PUSH)
        self.tx_parse_ack_socket.setsockopt(zmq.SNDHWM, 10000)
        self.tx_parse_ack_socket.connect("tcp://127.0.0.1:54214")

        context2 = zmq.Context[zmq.Socket[bytes]]()
        self.ack_for_mined_tx_socket = context2.socket(zmq.PUSH)
        self.ack_for_mined_tx_socket.setsockopt(zmq.SNDHWM, 10000)
        self.ack_for_mined_tx_socket.connect("tcp://127.0.0.1:55889")

        # A dedicated in-memory only table exclusive to this worker
        # it is frequently dropped and recreated for each chip-away batch
        self.inbound_tx_table_name = f'inbound_tx_table_{worker_id}'

        self.last_mysql_activity: int = int(time.time())

    def run(self) -> None:
        assert self.confirmed_tx_flush_queue is not None
        txs, ins, outs, pds, acks = reset_rows()
        mysql_db: MySQLDatabase = mysql_connect(worker_id=self.worker_id)
        try:
            while True:
                try:
                    # Pre-IBD do large batched flushes
                    confirmed_rows, acks = self.confirmed_tx_flush_queue.get(
                        timeout=BLOCK_BATCHING_RATE)
                    if not confirmed_rows:  # poison pill
                        break

                    txs, ins, outs, pds = extend_batched_rows(confirmed_rows, txs, ins, outs, pds)

                    if len(txs) > BLOCKS_MAX_TX_BATCH_LIMIT:
                        mysql_db, self.last_mysql_activity = maybe_refresh_mysql_connection(
                            mysql_db, self.last_mysql_activity, self.logger)
                        mysql_flush_rows_confirmed(self, MySQLFlushBatchWithAcks(txs, ins, outs, pds, acks),
                            mysql_db=mysql_db)
                        txs, ins, outs, pds, acks = reset_rows()

                # Post-IBD
                except queue.Empty:
                    if len(txs) != 0:
                        mysql_db, self.last_mysql_activity = maybe_refresh_mysql_connection(
                            mysql_db, self.last_mysql_activity, self.logger)
                        mysql_flush_rows_confirmed(self, MySQLFlushBatchWithAcks(txs, ins, outs, pds, acks),
                            mysql_db=mysql_db)
                        txs, ins, outs, pds, acks = reset_rows()
                    continue
        except KeyboardInterrupt:
            return
        except Exception as e:
            self.logger.exception("Caught exception")
            raise e
        finally:
            mysql_db.close()
            assert self.ack_for_mined_tx_socket is not None
            self.ack_for_mined_tx_socket.close()

