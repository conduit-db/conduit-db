import enum
import struct
import typing
from typing import TypedDict, Optional, NamedTuple, Dict

import bitcoinx
from bitcoinx import hash_to_hex_str, hex_str_to_hash

from conduit_lib.constants import MAX_UINT32


class BlockHeaderRow(NamedTuple):
    block_num: int
    block_hash: str
    block_height: int
    block_header: str
    block_tx_count: int
    block_size: int


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


class TransactionQueryResult(NamedTuple):
    tx_hash: bytes
    tx_block_num: int
    tx_position: int
    block_num: int
    block_hash: bytes
    block_height: int


class RestorationFilterQueryResult(NamedTuple):
    ref_type: int
    pushdata_hashX: bytes
    transaction_hash: bytes
    spend_transaction_hash: Optional[bytes]
    transaction_output_index: int  # max(uint32) if no spend
    spend_input_index: int
    block_height: int
    tx_location: TxLocation


class RestorationFilterRequest(typing.TypedDict):
    filterKeys: typing.List[str]


class RestorationFilterJSONResponse(TypedDict):
    flags: int
    pushDataHashHex: str
    transactionId: str
    index: int
    spendTransactionId: Optional[str]
    spendInputIndex: int
    blockHeight: int


class RestorationFilterResult(NamedTuple):
    flags: bytes
    push_data_hash: bytes
    transaction_hash: bytes
    spend_transaction_hash: bytes
    transaction_output_index: int
    spend_input_index: int
    block_height: int


RESULT_UNPACK_FORMAT = ">c32s32s32sIII"
FILTER_RESPONSE_SIZE = 1 + 32 + 32 + 32 + 4 + 4 + 4
assert struct.calcsize(RESULT_UNPACK_FORMAT) == FILTER_RESPONSE_SIZE

filter_response_struct = struct.Struct(RESULT_UNPACK_FORMAT)


def le_int_to_char(le_int):
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


class PushdataMatchFlags(enum.IntEnum):
    OUTPUT = 1 << 0
    INPUT = 1 << 1


def _get_pushdata_match_flag(ref_type: int) -> int:
    if ref_type == 0:
        return PushdataMatchFlags.OUTPUT
    if ref_type == 1:
        return PushdataMatchFlags.INPUT


def _pack_pushdata_match_response(row: RestorationFilterQueryResult, full_tx_hash,
        full_pushdata_hash, full_spend_transaction_hash, json):
    if json:
        pushdata_hash = full_pushdata_hash
        tx_hash = full_tx_hash
        idx = row.transaction_output_index
        flags = row.ref_type
        in_tx_hash = full_spend_transaction_hash  # can be null
        in_idx = MAX_UINT32
        if row.spend_input_index is not None:
            in_idx = row.spend_input_index
        block_height = row.block_height
        return {
            "PushDataHashHex": pushdata_hash,
            "TransactionId": tx_hash,
            "Index": idx,
            "Flags": flags,
            "SpendTransactionId": in_tx_hash,
            "SpendInputIndex": in_idx,
            "BlockHeight": block_height
        }
    else:
        pushdata_hash = hex_str_to_hash(full_pushdata_hash)
        tx_hash = hex_str_to_hash(full_tx_hash)
        idx = row.transaction_output_index
        flags = le_int_to_char(_get_pushdata_match_flag(row.ref_type))
        in_tx_hash = hex_str_to_hash("00"*32)
        if full_spend_transaction_hash is not None:
            in_tx_hash = hex_str_to_hash(full_spend_transaction_hash)
        in_idx = MAX_UINT32
        if row.spend_input_index is not None:
            in_idx = row.spend_input_index
        block_height = row.block_height

        return RestorationFilterResult(
            flags=flags,
            push_data_hash=pushdata_hash,
            transaction_hash=tx_hash,
            spend_transaction_hash=in_tx_hash,
            transaction_output_index=idx,
            spend_input_index=in_idx,
            block_height=block_height,
        )


def tsc_merkle_proof_json_to_binary(tsc_json: Dict, include_full_tx: bool, target_type: str) \
        -> bytearray:
    """{'index': 0, 'txOrId': txOrId, 'target': target, 'nodes': []}"""
    response = bytearray()

    flags = 0
    if include_full_tx:
        flags = flags & TxOrId.FULL_TRANSACTION

    if target_type == 'hash':
        flags = flags & TargetType.HASH
    elif target_type == 'header':
        flags = flags & TargetType.HEADER
    elif target_type == 'merkleroot':
        flags = flags & TargetType.MERKLE_ROOT
    else:
        flags = flags & TargetType.HASH

    flags = flags & ProofType.MERKLE_BRANCH  # ProofType.MERKLE_TREE not supported
    flags = flags & CompositeProof.SINGLE_PROOF  # CompositeProof.COMPOSITE_PROOF not supported

    response += le_int_to_char(flags)
    response += bitcoinx.pack_varint(tsc_json['index'])

    if include_full_tx:
        txLength = len(tsc_json['txOrId'])
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
