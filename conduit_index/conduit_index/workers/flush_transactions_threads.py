from __future__ import annotations

import logging
import queue
import threading
import time

import cbor2
from MySQLdb import _mysql
from typing import cast
import zmq

from ..types import ProcessedBlockAcks, MySQLFlushBatchWithAcks, MempoolTxAck
from ..workers.common import reset_rows, maybe_refresh_mysql_connection

from conduit_lib.database.mysql.types import MempoolTransactionRow, InputRow, OutputRow, \
    PushdataRow, ConfirmedTransactionRow, MySQLFlushBatch
from conduit_lib import MySQLDatabase
from conduit_lib.database.mysql.mysql_database import mysql_connect
from conduit_lib.utils import zmq_send_no_block


def extend_batched_rows(blk_rows: MySQLFlushBatch,
        txs: list[MempoolTransactionRow | ConfirmedTransactionRow], ins: list[InputRow],
        outs: list[OutputRow], pds: list[PushdataRow]) -> MySQLFlushBatch:
    """The updates are grouped as a safety precaution to not accidentally forget one of them"""
    tx_rows, in_rows, out_rows, set_pd_rows = blk_rows
    txs.extend(tx_rows)
    ins.extend(in_rows)
    outs.extend(out_rows)
    pds.extend(set_pd_rows)
    return MySQLFlushBatch(txs, ins, outs, pds)


# accumulate x seconds worth of txs or MAX_TX_BATCH_SIZE (whichever comes first)
# TODO - during initial block download - should NOT rely on a timeout at all
#  Should just keep on pumping the entire batch of blocks as fast as possible to
#  Max out CPU.
#  This ideally requires reliable PUB/SUB to do properly
BLOCKS_MAX_TX_BATCH_LIMIT = 200_000
BLOCK_BATCHING_RATE = 0.3
MEMPOOL_MAX_TX_BATCH_LIMIT = 2000
MEMPOOL_BATCHING_RATE = 0.1


class FlushTransactionsBaseThread(threading.Thread):

    def __init__(self, worker_id: int, daemon: bool = True) -> None:
        self.logger = logging.getLogger("flush-thread-base-class")
        threading.Thread.__init__(self, daemon=daemon)

        self.worker_id = worker_id

        self.ack_for_mined_tx_socket: zmq.Socket[bytes] | None = None

        context = zmq.Context[zmq.Socket[bytes]]()
        self.tx_parse_ack_socket = context.socket(zmq.PUSH)
        self.tx_parse_ack_socket.setsockopt(zmq.SNDHWM, 10000)
        self.tx_parse_ack_socket.connect("tcp://127.0.0.1:54214")

    def mysql_flush_ins_outs_and_pushdata_rows(self, in_rows: list[InputRow],
            out_rows: list[OutputRow], pd_rows: list[PushdataRow], mysql_db: MySQLDatabase) -> None:
        mysql_db.mysql_bulk_load_output_rows(out_rows)
        mysql_db.mysql_bulk_load_input_rows(in_rows)
        mysql_db.mysql_bulk_load_pushdata_rows(pd_rows)

    def mysql_flush_rows(self, flush_batch_with_acks: MySQLFlushBatchWithAcks, confirmed: bool,
            mysql_db: MySQLDatabase) -> None:
        tx_rows, in_rows, out_rows, pd_rows, acks = flush_batch_with_acks
        try:
            if confirmed:
                # TODO `mysql_bulk_load_confirmed_tx_rows` should raise if the database
                #  is overloaded and the `acks` should be pushed to a different error
                #  handling queue for the controller to re-enqueue the work items.
                #  The controller should keep a hash map of workitem ids -> WorkUnits for
                #  re-scheduling the work
                mysql_db.mysql_bulk_load_confirmed_tx_rows(
                    cast(list[ConfirmedTransactionRow], tx_rows))
                self.mysql_flush_ins_outs_and_pushdata_rows(in_rows, out_rows, pd_rows, mysql_db)

                # Ack for all flushed blocks
                assert self.ack_for_mined_tx_socket is not None
                acks = cast(ProcessedBlockAcks, acks)
                for blk_num, work_item_id, blk_hash, part_tx_hashes in acks:
                    msg = cbor2.dumps({blk_num: part_tx_hashes})
                    zmq_send_no_block(self.ack_for_mined_tx_socket, msg,
                        on_blocked_msg="Mined Transaction ACK receiver is busy")

                    tx_count = len(part_tx_hashes)
                    msg2 = cbor2.dumps((self.worker_id, work_item_id, blk_hash, tx_count))
                    zmq_send_no_block(self.tx_parse_ack_socket, msg2,
                        on_blocked_msg="Tx parse ACK receiver is busy")
            else:
                mysql_db.mysql_bulk_load_mempool_tx_rows(cast(list[MempoolTransactionRow], tx_rows))
                self.mysql_flush_ins_outs_and_pushdata_rows(in_rows, out_rows, pd_rows, mysql_db)

        except _mysql.IntegrityError as e:
            self.logger.exception(f"IntegrityError: {e}")
            raise


class FlushConfirmedTransactionsThread(FlushTransactionsBaseThread):

    def __init__(self, worker_id: int,
            confirmed_tx_flush_queue: queue.Queue[tuple[MySQLFlushBatch, ProcessedBlockAcks]],
            daemon: bool = True) -> None:
        self.logger = logging.getLogger("mined-blocks-thread")
        FlushTransactionsBaseThread.__init__(self, worker_id, daemon)

        self.worker_id = worker_id
        self.confirmed_tx_flush_queue: queue.Queue[tuple[MySQLFlushBatch, ProcessedBlockAcks]] = \
            confirmed_tx_flush_queue

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
                        self.mysql_flush_rows(MySQLFlushBatchWithAcks(txs, ins, outs, pds, acks),
                            confirmed=True, mysql_db=mysql_db)
                        txs, ins, outs, pds, acks = reset_rows()

                # Post-IBD
                except queue.Empty:
                    if len(txs) != 0:
                        mysql_db, self.last_mysql_activity = maybe_refresh_mysql_connection(
                            mysql_db, self.last_mysql_activity, self.logger)
                        self.mysql_flush_rows(MySQLFlushBatchWithAcks(txs, ins, outs, pds, acks),
                            confirmed=True, mysql_db=mysql_db)
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

    def mysql_insert_confirmed_tx_rows_thread(self) -> None:
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
                        self.mysql_flush_rows(MySQLFlushBatchWithAcks(txs, ins, outs, pds, acks),
                            confirmed=True, mysql_db=mysql_db)
                        txs, ins, outs, pds, acks = reset_rows()

                # Post-IBD
                except queue.Empty:
                    if len(txs) != 0:
                        mysql_db, self.last_mysql_activity = maybe_refresh_mysql_connection(
                            mysql_db, self.last_mysql_activity, self.logger)
                        self.mysql_flush_rows(MySQLFlushBatchWithAcks(txs, ins, outs, pds, acks),
                            confirmed=True, mysql_db=mysql_db)
                        txs, ins, outs, pds, acks = reset_rows()
                    continue

        except Exception as e:
            self.logger.exception("Caught exception")
            raise e
        finally:
            mysql_db.close()

    def maybe_refresh_mysql_connection_mined(self, mysql_db: MySQLDatabase) -> MySQLDatabase:
        REFRESH_TIMEOUT = 600
        if time.time() - self.last_mysql_activity > REFRESH_TIMEOUT:
            self.logger.info(f"Refreshing MySQLDatabase connection due to {REFRESH_TIMEOUT} "
                             f"second refresh timeout")
            mysql_db.close()
            mysql_db = mysql_db.mysql_conn.ping(reconnect=True)
            self.last_mysql_activity = int(time.time())
            return mysql_db
        else:
            return mysql_db


class FlushMempoolTransactionsThread(FlushTransactionsBaseThread):

    def __init__(self, worker_id: int,
            mempool_tx_flush_queue: queue.Queue[tuple[MySQLFlushBatch, MempoolTxAck]],
            daemon: bool = True) -> None:
        self.logger = logging.getLogger("mempool-transactions-thread")
        FlushTransactionsBaseThread.__init__(self, worker_id, daemon)

        self.last_mysql_activity = int(time.time())
        self.worker_id = worker_id
        # self.worker_ack_queue_tx_parse_mempool = worker_ack_queue_tx_parse_mempool
        self.mempool_tx_flush_queue = mempool_tx_flush_queue

    def mysql_insert_mempool_tx_rows_thread(self) -> None:
        assert self.mempool_tx_flush_queue is not None
        txs, ins, outs, pds, acks = reset_rows()
        mysql_db: MySQLDatabase = mysql_connect(worker_id=self.worker_id)
        try:
            while True:
                try:
                    mempool_rows, acks = self.mempool_tx_flush_queue.get(
                        timeout=MEMPOOL_BATCHING_RATE)
                    if not mempool_rows:  # poison pill
                        break

                    txs, ins, outs, pds = extend_batched_rows(mempool_rows, txs, ins, outs, pds)

                    if len(txs) > MEMPOOL_MAX_TX_BATCH_LIMIT - 1:
                        self.logger.debug(f"hit max mempool batch size ({len(txs)})")
                        mysql_db, self.last_mysql_activity = \
                            maybe_refresh_mysql_connection(mysql_db, self.last_mysql_activity,
                                self.logger)
                        self.mysql_flush_rows(MySQLFlushBatchWithAcks(txs, ins, outs, pds, acks),
                            confirmed=False, mysql_db=mysql_db)
                        txs, ins, outs, pds, acks = reset_rows()

                except queue.Empty:
                    # self.logger.debug("mempool batch timer triggered")
                    if len(txs) != 0:
                        mysql_db, self.last_mysql_activity = maybe_refresh_mysql_connection(
                            mysql_db, self.last_mysql_activity, self.logger)
                        self.mysql_flush_rows(MySQLFlushBatchWithAcks(txs, ins, outs, pds, acks),
                            confirmed=False, mysql_db=mysql_db)
                        txs, ins, outs, pds, acks = reset_rows()
                        self.last_mysql_activity = int(time.time())
                    continue
        except KeyboardInterrupt:
            return
        except Exception as e:
            self.logger.exception("Caught exception")
            raise e
        finally:
            mysql_db.close()
