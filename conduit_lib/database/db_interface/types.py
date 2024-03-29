# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.

from typing import NamedTuple


class PushdataRow(NamedTuple):
    pushdata_hash: str  # hashX as hex ready for db flush
    tx_hash: str  # hashX as hex ready for db flush
    idx: int
    ref_type: int


class PushdataRowParsed(NamedTuple):
    pushdata_hash: bytes  # full hash
    tx_hash: bytes  # full hash
    idx: int
    ref_type: int


class ConfirmedTransactionRow(NamedTuple):
    tx_hash: str
    tx_block_num: int
    tx_position: int


class CheckpointStateRow(NamedTuple):
    best_flushed_block_height: int
    best_flushed_block_hash: bytes
    reorg_was_allocated: bool
    first_allocated_block_hash: bytes
    last_allocated_block_hash: bytes
    old_hashes_array: bytes
    new_hashes_array: bytes


class InputRow(NamedTuple):
    out_tx_hash: str  # hashX as hex ready for db flush
    out_idx: int  # hashX as hex ready for db flush
    in_tx_hash: str
    in_idx: int


class InputRowParsed(NamedTuple):
    out_tx_hash: bytes  # full hash
    out_idx: int
    in_tx_hash: bytes  # full hash
    in_idx: int


class MempoolTransactionRow(NamedTuple):
    mp_tx_hashX: str


class OutputRow(NamedTuple):
    out_tx_hash: str
    out_idx: int
    out_value: int


class MinedTxHashXes(NamedTuple):
    txid: str
    block_number: int


class MySQLFlushBatch(NamedTuple):
    tx_rows: list[ConfirmedTransactionRow]
    tx_rows_mempool: list[MempoolTransactionRow]
    in_rows: list[InputRow]
    pd_rows: list[PushdataRow]


class ProcessedBlockAck(NamedTuple):
    block_num: int
    work_item_id: int
    block_hash: bytes
    partition_block_hashes: list[bytes]


ProcessedBlockAcks = dict[int, ProcessedBlockAck]  # work_item_id: ProcessedBlockAck


class DBFlushBatchWithAcks(NamedTuple):
    tx_rows: list[ConfirmedTransactionRow]
    tx_rows_mempool: list[MempoolTransactionRow]
    in_rows: list[InputRow]
    pd_rows: list[PushdataRow]
    acks: ProcessedBlockAcks


MempoolTxAck = int


class DBFlushBatchWithAcksMempool(NamedTuple):
    tx_rows: list[ConfirmedTransactionRow]
    tx_rows_mempool: list[MempoolTransactionRow]
    in_rows: list[InputRow]
    pd_rows: list[PushdataRow]
    acks: MempoolTxAck


WorkItemId = int
TxOffset = int
AlreadySeenMempoolTxOffsets = dict[WorkItemId, set[TxOffset]]
NewNotSeenBeforeTxOffsets = dict[WorkItemId, set[TxOffset]]


class TipFilterNotifications(NamedTuple):
    utxo_spends: list[InputRowParsed]
    pushdata_matches: list[PushdataRowParsed]
    block_hash: bytes
