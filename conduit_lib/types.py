import enum
import struct
import typing
from queue import Queue
from typing import TypedDict, NamedTuple

import bitcoinx
from bitcoinx import hex_str_to_hash, Header

from conduit_lib.constants import MAX_UINT32

MultiprocessingQueue = Queue  # workaround for mypy: https://github.com/python/typeshed/issues/4266
Hash256 = bytes


class DataLocation(NamedTuple):
    """This metadata must be persisted elsewhere.
    For example, a key-value store such as LMDB"""
    file_path: str
    start_offset: int
    end_offset: int


class TSCMerkleProof(TypedDict):
    index: int
    txOrId: str
    target: str
    nodes: list[str]
    targetType: str | None


class BlockHeaderRow(NamedTuple):
    block_num: int
    block_hash: str
    block_height: int
    block_header: str
    block_tx_count: int
    block_size: int
    is_orphaned: int  # mysql TINYINT either 1 or zero


class MerkleTreeArrayLocation(NamedTuple):
    write_path: str
    start_offset: int
    end_offset: int
    base_node_count: int



class TxOffsetsArrayLocation(NamedTuple):
    write_path: str
    start_offset: int
    end_offset: int


class RawBlockLocation(NamedTuple):
    write_path: str
    start_offset: int
    end_offset: int


class Slice(NamedTuple):
    start_offset: int
    end_offset: int


class BlockSliceRequestType(NamedTuple):
    block_num: int
    slice: Slice


class BlockMetadata(NamedTuple):
    block_size: int
    tx_count: int


class TxLocation(NamedTuple):
    block_hash: bytes
    block_num: int
    tx_position: int  # 0-based index in block


class TxMetadata(NamedTuple):
    tx_hashX: bytes
    tx_block_num: int
    tx_position: int
    block_num: int
    block_hash: bytes
    block_height: int


class RestorationFilterQueryResult(NamedTuple):
    ref_type: int
    pushdata_hashX: bytes
    transaction_hash: bytes
    spend_transaction_hash: bytes | None
    transaction_output_index: int  # max(uint32) if no spend
    spend_input_index: int
    tx_location: TxLocation


class RestorationFilterRequest(typing.TypedDict):
    filterKeys: typing.List[str]


class RestorationFilterJSONResponse(TypedDict):
    flags: int
    pushDataHashHex: str
    lockingTransactionId: str
    lockingTransactionIndex: int
    unlockingTransactionId: str | None
    unlockingInputIndex: int


class RestorationFilterResult(NamedTuple):
    flags: int  # one byte integer
    push_data_hash: bytes
    locking_transaction_hash: bytes
    locking_output_index: int
    unlocking_transaction_hash: bytes  # null hash
    unlocking_input_index: int  # 0


RESULT_UNPACK_FORMAT = ">B32s32sI32sI"
FILTER_RESPONSE_SIZE = 1 + 32 + 32 + 4 + 32 + 4
assert struct.calcsize(RESULT_UNPACK_FORMAT) == FILTER_RESPONSE_SIZE

filter_response_struct = struct.Struct(RESULT_UNPACK_FORMAT)


def le_int_to_char(le_int: int) -> bytes:
    return struct.pack('<I', le_int)[0:1]


class TxOrId(enum.IntEnum):
    TRANSACTION_ID = 0
    FULL_TRANSACTION = 1


class TargetType(enum.IntEnum):
    HASH = 0
    HEADER = 1 << 1
    MERKLE_ROOT = 1 << 2


class ProofType(enum.IntEnum):
    MERKLE_BRANCH = 0
    MERKLE_TREE = 1 << 3


class CompositeProof(enum.IntEnum):
    SINGLE_PROOF = 0
    COMPOSITE_PROOF = 1 << 4


class PushdataMatchFlags(enum.IntFlag):
    OUTPUT: int = 1 << 0
    INPUT: int = 1 << 1
    DATA: int = 1 << 2


def _pack_pushdata_match_response_bin(row: RestorationFilterQueryResult, full_tx_hash: str,
        full_pushdata_hash: str, full_spend_transaction_hash: bytes) -> RestorationFilterResult:
    pushdata_hash = hex_str_to_hash(full_pushdata_hash)
    tx_hash = hex_str_to_hash(full_tx_hash)
    idx = row.transaction_output_index
    in_tx_hash = hex_str_to_hash("00"*32)
    if full_spend_transaction_hash is not None:
        in_tx_hash = hex_str_to_hash(full_spend_transaction_hash)
    in_idx = MAX_UINT32
    if row.spend_input_index is not None:
        in_idx = row.spend_input_index

    return RestorationFilterResult(
        flags=row.ref_type,
        push_data_hash=pushdata_hash,
        locking_transaction_hash=tx_hash,
        locking_output_index=idx,
        unlocking_transaction_hash=in_tx_hash,
        unlocking_input_index=in_idx
    )



def _pack_pushdata_match_response_json(row: RestorationFilterQueryResult, full_tx_hash: str,
        full_pushdata_hash: str, full_spend_transaction_hash: str | None) \
            -> RestorationFilterJSONResponse:
    pushdata_hash = full_pushdata_hash
    tx_hash = full_tx_hash
    idx = row.transaction_output_index
    flags = row.ref_type
    in_tx_hash = full_spend_transaction_hash  # can be null
    in_idx = MAX_UINT32
    if row.spend_input_index is not None:
        in_idx = row.spend_input_index
    return RestorationFilterJSONResponse(
        flags=flags,
        pushDataHashHex=pushdata_hash,
        lockingTransactionId=tx_hash,
        lockingTransactionIndex=idx,
        unlockingTransactionId=in_tx_hash,
        unlockingInputIndex=in_idx
    )


def tsc_merkle_proof_json_to_binary(tsc_json: TSCMerkleProof, include_full_tx: bool,
        target_type: str) -> bytearray:
    """{'index': 0, 'txOrId': txOrId, 'target': target, 'nodes': []}"""
    response = bytearray()

    flags = 0
    if include_full_tx:
        flags = flags | TxOrId.FULL_TRANSACTION

    if target_type == 'hash':
        flags = flags | TargetType.HASH
    elif target_type == 'header':
        flags = flags | TargetType.HEADER
    elif target_type == 'merkleroot':
        flags = flags | TargetType.MERKLE_ROOT
    else:
        raise NotImplementedError("Caller should have ensured `target_type` is valid.")

    flags = flags | ProofType.MERKLE_BRANCH  # ProofType.MERKLE_TREE not supported
    flags = flags | CompositeProof.SINGLE_PROOF  # CompositeProof.COMPOSITE_PROOF not supported

    response += le_int_to_char(flags)
    response += bitcoinx.pack_varint(tsc_json['index'])

    if include_full_tx:
        txLength = len(tsc_json['txOrId']) // 2
        response += bitcoinx.pack_varint(txLength)
        response += bytes.fromhex(tsc_json['txOrId'])
    else:
        response += bitcoinx.hex_str_to_hash(tsc_json['txOrId'])

    if target_type in {'hash', 'merkleroot'}:
        response += bitcoinx.hex_str_to_hash(tsc_json['target'])
    else:  # header
        response += bytes.fromhex(tsc_json['target'])

    nodeCount = bitcoinx.pack_varint(len(tsc_json['nodes']))
    response += nodeCount
    for node in tsc_json['nodes']:
        if node == "*":
            duplicate_type_node = b'\x01'
            response += duplicate_type_node
        else:
            hash_type_node = b"\x00"
            response += hash_type_node
            response += bitcoinx.hex_str_to_hash(node)
    return response


class HeaderSpan(NamedTuple):
    """Used to allocate which continguous set of raw blocks to pre-fetch and process"""
    is_reorg: bool
    start_header: Header
    stop_header: Header


ChainHashes = list[bytes]
