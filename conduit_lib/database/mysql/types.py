from __future__ import annotations

from typing import NamedTuple


class PushdataRow(NamedTuple):
    pushdata_hash: str
    tx_hash: str
    idx: int
    ref_type: int


class ConfirmedTransactionRow(NamedTuple):
    tx_hash: str
    tx_block_num: int
    tx_position: int


class HeadersRow(NamedTuple):
    block_num: int
    block_hash: str
    block_height: int
    block_header: str
    block_tx_count: int
    block_size: int
    is_orphaned: bool


class CheckpointStateRow(NamedTuple):
    id: int
    best_flushed_block_height: int
    best_flushed_block_hash: str
    reorg_was_allocated: int
    first_allocated_block_hash: str
    last_allocated_block_hash: str
    old_hashes_array: str
    new_hashes_array: str


class InputRow(NamedTuple):
    out_tx_hash: str
    out_idx: int
    in_tx_hash: str
    in_idx: int


class MempoolTransactionRow(NamedTuple):
    mp_tx_hash: str
    mp_tx_timestamp: str


class OutputRow(NamedTuple):
    out_tx_hash: str
    out_idx: int
    out_value: int


class MinedTxHashes(NamedTuple):
    txid: str
    block_number: int


class MySQLFlushBatch(NamedTuple):
    tx_rows: list[MempoolTransactionRow | ConfirmedTransactionRow]
    in_rows: list[InputRow]
    out_rows: list[OutputRow]
    pd_rows: list[PushdataRow]
