import array
from typing import NamedTuple

import bitcoinx

# NOTE(typing) For some reason we can't embed `array.ArrayType[int]` here:
#   TypeError: 'type' object is not subscriptable
from conduit_lib.database.mysql.types import MempoolTransactionRow, ConfirmedTransactionRow, \
    InputRow, OutputRow, PushdataRow


class ProcessedBlockAck(NamedTuple):
    block_num: int
    work_item_id: int
    block_hash: bytes
    partition_block_hashes: list[bytes]


class MempoolTxAck(NamedTuple):
    num_mempool_txs_processed: int


WorkUnit = tuple[bool, int, int, bytes, int, int, int, 'array.ArrayType[int]']
MainBatch = list[tuple[int, 'array.ArrayType[int]', bitcoinx.Header, int]]
WorkPart = tuple[int, bytes, int, int, int, 'array.ArrayType[int]']
BatchedRawBlockSlices = list[tuple['array.ArrayType[int]', int, int, int, int]]
ProcessedBlockAcks = list[ProcessedBlockAck]
TxHashRows = list[tuple[str]]
TxHashes = list[bytes]
TxHashToWorkIdMap = dict[bytes, int]
TxHashToOffsetMap = dict[bytes, int]
BlockSliceOffsets = tuple[int, int]  # i.e. start and end byte offset for the slice


class MySQLFlushBatchWithAcks(NamedTuple):
    tx_rows: list[MempoolTransactionRow | ConfirmedTransactionRow]
    in_rows: list[InputRow]
    out_rows: list[OutputRow]
    pd_rows: list[PushdataRow]
    acks: MempoolTxAck | ProcessedBlockAcks
