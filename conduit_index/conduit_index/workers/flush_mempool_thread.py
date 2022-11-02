from __future__ import annotations

import logging
import queue
from queue import Queue
import threading
import time

from ..types import MySQLFlushBatchWithAcksMempool, MempoolTxAck
from ..workers.common import maybe_refresh_mysql_connection, mysql_flush_rows_mempool, \
    extend_batched_rows, reset_rows_mempool

from conduit_lib.database.mysql.types import MySQLFlushBatch
from conduit_lib import MySQLDatabase
from conduit_lib.database.mysql.mysql_database import mysql_connect

MEMPOOL_MAX_TX_BATCH_LIMIT = 2000
MEMPOOL_BATCHING_RATE = 0.1


class FlushMempoolTransactionsThread(threading.Thread):

    def __init__(self, worker_id: int,
            mempool_tx_flush_queue: Queue[tuple[MySQLFlushBatch, MempoolTxAck]],
            daemon: bool = True) -> None:
        self.logger = logging.getLogger(f"mempool-transactions-thread-{worker_id}")
        self.logger.setLevel(logging.DEBUG)
        threading.Thread.__init__(self, daemon=daemon)

        self.last_mysql_activity = int(time.time())
        self.worker_id = worker_id
        self.mempool_tx_flush_queue = mempool_tx_flush_queue

    def run(self) -> None:
        assert self.mempool_tx_flush_queue is not None
        txs, txs_mempool, ins, outs, pds, acks = reset_rows_mempool()
        mysql_db: MySQLDatabase = mysql_connect(worker_id=self.worker_id)
        try:
            while True:
                try:
                    mempool_rows, new_acks = self.mempool_tx_flush_queue.get(
                        timeout=MEMPOOL_BATCHING_RATE)
                    if not mempool_rows:  # poison pill
                        break

                    txs, txs_mempool, ins, outs, pds = extend_batched_rows(mempool_rows, txs, txs_mempool, ins, outs, pds)
                    acks += new_acks

                    if len(txs_mempool) > MEMPOOL_MAX_TX_BATCH_LIMIT - 1:
                        self.logger.debug(f"hit max mempool batch size ({len(txs_mempool)})")
                        mysql_db, self.last_mysql_activity = \
                            maybe_refresh_mysql_connection(mysql_db, self.last_mysql_activity,
                                self.logger)
                        mysql_flush_rows_mempool(self,
                            MySQLFlushBatchWithAcksMempool(txs, txs_mempool, ins, outs, pds, acks), mysql_db=mysql_db)
                        txs, txs_mempool, ins, outs, pds, acks = reset_rows_mempool()

                except queue.Empty:
                    # self.logger.debug("mempool batch timer triggered")
                    if len(txs_mempool) != 0:
                        mysql_db, self.last_mysql_activity = maybe_refresh_mysql_connection(
                            mysql_db, self.last_mysql_activity, self.logger)
                        mysql_flush_rows_mempool(self,
                            MySQLFlushBatchWithAcksMempool(txs, txs_mempool, ins, outs, pds, acks), mysql_db=mysql_db)
                        txs, txs_mempool, ins, outs, pds, acks = reset_rows_mempool()
                        self.last_mysql_activity = int(time.time())
                    continue
        except KeyboardInterrupt:
            return
        except Exception as e:
            self.logger.exception("Caught exception")
            raise e
        finally:
            mysql_db.close()