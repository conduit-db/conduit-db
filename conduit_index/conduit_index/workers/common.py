import MySQLdb
import cbor2
import logging
import typing

from conduit_lib.constants import HashXLength
from conduit_lib.database.db_interface.types import MySQLFlushBatch, ConfirmedTransactionRow, \
    MempoolTransactionRow, InputRow, OutputRow, PushdataRow, PushdataRowParsed, InputRowParsed, \
    DBFlushBatchWithAcks, DBFlushBatchWithAcksMempool, ProcessedBlockAcks, MempoolTxAck
from conduit_lib.utils import zmq_send_no_block
from conduit_lib import DBInterface

if typing.TYPE_CHECKING:
    from .flush_blocks_thread import FlushConfirmedTransactionsThread
    from .flush_mempool_thread import FlushMempoolTransactionsThread


def extend_batched_rows(
    blk_rows: MySQLFlushBatch,
    txs: list[ConfirmedTransactionRow],
    txs_mempool: list[MempoolTransactionRow],
    ins: list[InputRow],
    outs: list[OutputRow],
    pds: list[PushdataRow],
) -> MySQLFlushBatch:
    """The updates are grouped as a safety precaution to not accidentally forget one of them"""
    tx_rows, tx_rows_mempool, in_rows, out_rows, set_pd_rows = blk_rows
    txs.extend(tx_rows)
    txs_mempool.extend(tx_rows_mempool)
    ins.extend(in_rows)
    outs.extend(out_rows)
    pds.extend(set_pd_rows)
    return MySQLFlushBatch(txs, txs_mempool, ins, outs, pds)


def flush_ins_outs_and_pushdata_rows(
    in_rows: list[InputRow],
    out_rows: list[OutputRow],
    pd_rows: list[PushdataRow],
    db: DBInterface,
) -> None:
    db.bulk_load_output_rows(out_rows)
    db.bulk_load_input_rows(in_rows)
    db.bulk_load_pushdata_rows(pd_rows)


def flush_rows_confirmed(
    worker: "FlushConfirmedTransactionsThread",
    flush_batch_with_acks: DBFlushBatchWithAcks,
    db: DBInterface,
) -> None:
    (
        tx_rows,
        tx_rows_mempool,
        in_rows,
        out_rows,
        pd_rows,
        acks,
    ) = flush_batch_with_acks
    try:
        db.bulk_load_confirmed_tx_rows(tx_rows)
        flush_ins_outs_and_pushdata_rows(in_rows, out_rows, pd_rows, db)

        # Ack for all flushed blocks
        assert worker.socket_mined_tx_ack is not None
        assert worker.socket_mined_tx_parsed_ack is not None
        for blk_num, work_item_id, blk_hash, part_tx_hashes in acks:
            msg = cbor2.dumps({blk_num: part_tx_hashes})
            zmq_send_no_block(
                worker.socket_mined_tx_ack,
                msg,
                on_blocked_msg="Mined Transaction ACK receiver is busy",
            )

            tx_count = len(part_tx_hashes)

            msg2 = cbor2.dumps((worker.worker_id, work_item_id, blk_hash, tx_count))
            zmq_send_no_block(
                worker.socket_mined_tx_parsed_ack,
                msg2,
                on_blocked_msg="Tx parse ACK receiver is busy",
            )
    except MySQLdb.IntegrityError as e:
        worker.logger.exception(f"IntegrityError")
        raise


def flush_rows_mempool(
    worker: "FlushMempoolTransactionsThread",
    flush_batch_with_acks: DBFlushBatchWithAcksMempool,
    db: DBInterface,
) -> None:
    (
        tx_rows,
        tx_rows_mempool,
        in_rows,
        out_rows,
        pd_rows,
        acks,
    ) = flush_batch_with_acks
    try:
        db.bulk_load_mempool_tx_rows(tx_rows_mempool)
        flush_ins_outs_and_pushdata_rows(in_rows, out_rows, pd_rows, db)
    except MySQLdb.IntegrityError as e:
        worker.logger.exception(f"IntegrityError")
        raise


def reset_rows() -> (
    tuple[
        list[ConfirmedTransactionRow],
        list[MempoolTransactionRow],
        list[InputRow],
        list[OutputRow],
        list[PushdataRow],
        ProcessedBlockAcks,
    ]
):
    txs: list[ConfirmedTransactionRow] = []
    txs_mempool: list[MempoolTransactionRow] = []
    ins: list[InputRow] = []
    outs: list[OutputRow] = []
    pds: list[PushdataRow] = []
    acks: ProcessedBlockAcks = []
    return txs, txs_mempool, ins, outs, pds, acks


def reset_rows_mempool() -> (
    tuple[
        list[ConfirmedTransactionRow],
        list[MempoolTransactionRow],
        list[InputRow],
        list[OutputRow],
        list[PushdataRow],
        MempoolTxAck,
    ]
):
    txs: list[ConfirmedTransactionRow] = []
    txs_mempool: list[MempoolTransactionRow] = []
    ins: list[InputRow] = []
    outs: list[OutputRow] = []
    pds: list[PushdataRow] = []
    acks: MempoolTxAck = 0
    return txs, txs_mempool, ins, outs, pds, acks


def maybe_refresh_connection(
    db: DBInterface, last_activity: int, logger: logging.Logger
) -> tuple[DBInterface, int]:
    return db.maybe_refresh_connection(last_activity, logger)


def convert_pushdata_rows_for_flush(
    pd_rows: list[PushdataRowParsed],
) -> list[PushdataRow]:
    pushdata_rows_for_flushing = []
    for pd_row in pd_rows:
        pushdata_rows_for_flushing.append(
            PushdataRow(
                pd_row.pushdata_hash[0:HashXLength].hex(),
                pd_row.tx_hash[0:HashXLength].hex(),
                pd_row.idx,
                pd_row.ref_type,
            )
        )
    return pushdata_rows_for_flushing


def convert_input_rows_for_flush(
    in_rows: list[InputRowParsed],
) -> list[InputRow]:
    input_rows_for_flushing = []
    for input in in_rows:
        input_rows_for_flushing.append(
            InputRow(
                input.out_tx_hash[0:HashXLength].hex(),
                input.out_idx,
                input.in_tx_hash[0:HashXLength].hex(),
                input.in_idx,
            )
        )
    return input_rows_for_flushing
