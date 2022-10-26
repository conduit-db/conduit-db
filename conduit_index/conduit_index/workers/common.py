import logging
import time
import typing
from typing import cast

import cbor2
from MySQLdb import _mysql

from conduit_lib.utils import zmq_send_no_block
from ..types import ProcessedBlockAcks, MySQLFlushBatchWithAcks, MySQLFlushBatchWithAcksMempool, \
    MempoolTxAck
from conduit_lib import MySQLDatabase
from conduit_lib.database.mysql.types import MempoolTransactionRow, ConfirmedTransactionRow, \
    InputRow, OutputRow, PushdataRow, MySQLFlushBatch

if typing.TYPE_CHECKING:
    from .flush_blocks_thread import FlushConfirmedTransactionsThread
    from .flush_mempool_thread import FlushMempoolTransactionsThread


def extend_batched_rows(blk_rows: MySQLFlushBatch,
        txs: list[ConfirmedTransactionRow], txs_mempool: list[MempoolTransactionRow],
        ins: list[InputRow], outs: list[OutputRow], pds: list[PushdataRow]) -> MySQLFlushBatch:
    """The updates are grouped as a safety precaution to not accidentally forget one of them"""
    tx_rows, tx_rows_mempool, in_rows, out_rows, set_pd_rows = blk_rows
    txs.extend(tx_rows)
    txs_mempool.extend(tx_rows_mempool)
    ins.extend(in_rows)
    outs.extend(out_rows)
    pds.extend(set_pd_rows)
    return MySQLFlushBatch(txs, txs_mempool, ins, outs, pds)


def mysql_flush_ins_outs_and_pushdata_rows(in_rows: list[InputRow], out_rows: list[OutputRow],
        pd_rows: list[PushdataRow], mysql_db: MySQLDatabase) -> None:
    mysql_db.mysql_bulk_load_output_rows(out_rows)
    mysql_db.mysql_bulk_load_input_rows(in_rows)
    mysql_db.mysql_bulk_load_pushdata_rows(pd_rows)


def mysql_flush_rows_confirmed(worker: 'FlushConfirmedTransactionsThread',
        flush_batch_with_acks: MySQLFlushBatchWithAcks, mysql_db: MySQLDatabase) -> None:
    tx_rows, tx_rows_mempool, in_rows, out_rows, pd_rows, acks = flush_batch_with_acks
    try:
        # TODO `mysql_bulk_load_confirmed_tx_rows` should raise if the database
        #  is overloaded and the `acks` should be pushed to a different error
        #  handling queue for the controller to re-enqueue the work items.
        #  There should be database tables for workitem ids -> WorkUnits for
        #  re-scheduling the work
        mysql_db.mysql_bulk_load_confirmed_tx_rows(tx_rows)
        mysql_flush_ins_outs_and_pushdata_rows(in_rows, out_rows, pd_rows, mysql_db)

        # Ack for all flushed blocks
        assert worker.ack_for_mined_tx_socket is not None
        for blk_num, work_item_id, blk_hash, part_tx_hashes in acks:
            msg = cbor2.dumps({blk_num: part_tx_hashes})
            zmq_send_no_block(worker.ack_for_mined_tx_socket, msg,
                on_blocked_msg="Mined Transaction ACK receiver is busy")

            tx_count = len(part_tx_hashes)

            msg2 = cbor2.dumps((worker.worker_id, work_item_id, blk_hash, tx_count))
            zmq_send_no_block(worker.tx_parse_ack_socket, msg2,
                on_blocked_msg="Tx parse ACK receiver is busy")
    except _mysql.IntegrityError as e:
        worker.logger.exception(f"IntegrityError")
        raise


def mysql_flush_rows_mempool(worker: 'FlushMempoolTransactionsThread',
        flush_batch_with_acks: MySQLFlushBatchWithAcksMempool, mysql_db: MySQLDatabase) -> None:
    tx_rows, tx_rows_mempool, in_rows, out_rows, pd_rows, acks = flush_batch_with_acks
    try:
        mysql_db.mysql_bulk_load_mempool_tx_rows(tx_rows_mempool)
        mysql_flush_ins_outs_and_pushdata_rows(in_rows, out_rows, pd_rows, mysql_db)
    except _mysql.IntegrityError as e:
        worker.logger.exception(f"IntegrityError")
        raise


def reset_rows() -> MySQLFlushBatchWithAcks:
    txs: list[ConfirmedTransactionRow] = []
    txs_mempool: list[MempoolTransactionRow] = []
    ins: list[InputRow] = []
    outs: list[OutputRow] = []
    pds: list[PushdataRow] = []
    acks: ProcessedBlockAcks = []
    return MySQLFlushBatchWithAcks(txs, txs_mempool, ins, outs, pds, acks)


def reset_rows_mempool() -> MySQLFlushBatchWithAcksMempool:
    txs: list[ConfirmedTransactionRow] = []
    txs_mempool: list[MempoolTransactionRow] = []
    ins: list[InputRow] = []
    outs: list[OutputRow] = []
    pds: list[PushdataRow] = []
    acks: MempoolTxAck = 0
    return MySQLFlushBatchWithAcksMempool(txs, txs_mempool, ins, outs, pds, acks)


def maybe_refresh_mysql_connection(mysql_db: MySQLDatabase,
        last_mysql_activity: int, logger: logging.Logger) -> tuple[MySQLDatabase, int]:
    REFRESH_TIMEOUT = 600
    if int(time.time()) - last_mysql_activity > REFRESH_TIMEOUT:
        logger.info(f"Refreshing MySQLDatabase connection due to {REFRESH_TIMEOUT} "
            f"second refresh timeout")
        mysql_db.mysql_conn.ping()
        last_mysql_activity = int(time.time())
        return mysql_db, last_mysql_activity
    else:
        return mysql_db, last_mysql_activity
