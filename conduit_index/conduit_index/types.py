import typing

import bitcoinx
from typing import NamedTuple

# NOTE(typing) For some reason we can't embed `array.ArrayType[int]` here:
#   TypeError: 'type' object is not subscriptable
from conduit_lib.database.mysql.types import InputRowParsed, MempoolTransactionRow, \
    ConfirmedTransactionRow, InputRow, OutputRow, PushdataRow, PushdataRowParsed

if typing.TYPE_CHECKING:
    import array


class ProcessedBlockAck(NamedTuple):
    block_num: int
    work_item_id: int
    block_hash: bytes
    partition_block_hashes: list[bytes]


WorkUnit = tuple[bool, int, int, bytes, int, int, int, 'array.ArrayType[int]']
MainBatch = list[tuple[int, 'array.ArrayType[int]', bitcoinx.Header, int]]
WorkPart = tuple[int, bytes, int, int, int, 'array.ArrayType[int]']
BatchedRawBlockSlices = list[tuple[bytes, int, int, int, bytes, int]]
ProcessedBlockAcks = list[ProcessedBlockAck]
TxHashRows = list[tuple[str]]
TxHashes = list[bytes]
TxHashToWorkIdMap = dict[bytes, int]
TxHashToOffsetMap = dict[bytes, int]
BlockSliceOffsets = tuple[int, int]  # i.e. start and end byte offset for the slice


class MySQLFlushBatchWithAcks(NamedTuple):
    tx_rows: list[ConfirmedTransactionRow]
    tx_rows_mempool: list[MempoolTransactionRow]
    in_rows: list[InputRow]
    out_rows: list[OutputRow]
    pd_rows: list[PushdataRow]
    acks: ProcessedBlockAcks


MempoolTxAck = int
class MySQLFlushBatchWithAcksMempool(NamedTuple):
    tx_rows: list[ConfirmedTransactionRow]
    tx_rows_mempool: list[MempoolTransactionRow]
    in_rows: list[InputRow]
    out_rows: list[OutputRow]
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
