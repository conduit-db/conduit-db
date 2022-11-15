import bitcoinx
from dataclasses import dataclass
import enum
from enum import IntEnum
import struct
from typing import Any, Optional, NamedTuple, TypedDict

from conduit_lib.database.mysql.types import PushdataRowParsed
from conduit_lib.types import OutpointType, output_spend_struct

from .constants import AccountFlag, OutboundDataFlag

PushdataRegistrationJSONType = tuple[str, int]  # pushdata_hash, duration_seconds


class CuckooResult(enum.IntEnum):
    OK = 0
    NOT_FOUND = 1
    NOT_ENOUGH_SPACE = 2
    NOT_SUPPORTED = 3


class BackendWorkerOfflineError(Exception):
    pass


ZEROED_OUTPOINT: OutpointType = OutpointType(bytes(32), 0)


class OutpointMessageType(IntEnum):
    REGISTER = 1 << 0
    UNREGISTER = 1 << 1
    ACK = 1 << 2
    SPEND = 1 << 3
    CLEAR_ALL = 1 << 4
    READY = 1 << 5  # initial "READY" signal for each worker


RequestId = str  # hex


class OutpointStateUpdate(NamedTuple):
    request_id: RequestId
    command: OutpointMessageType
    outpoint: bytes | None  # outpoint_struct
    output_spend: bytes | None  # output_spend_struct
    worker_id: int | None

    def __str__(self) -> str:
        return f"""OutpointStateUpdate(request_id={self.request_id},
               command={repr(OutpointMessageType(self.command))},
               outpoint={OutpointType.from_outpoint_struct(self.outpoint)
                    if self.outpoint else None},
               output_spend={OutputSpendRow.from_output_spend_struct(self.output_spend) 
                    if self.output_spend else None},
               worker_id={self.worker_id})""".replace("\n", "").replace(" ", "").replace("\t", "")


class TipFilterRegistrationEntry(NamedTuple):
    pushdata_hash: bytes
    duration_seconds: int   # when unregistering duration_seconds=0xffffffff

    def __str__(self) -> str:
        return f"TipFilterRegistrationEntry(pushdata_hash={self.pushdata_hash.hex()}, " \
               f"duration_seconds={self.duration_seconds})"


class PushdataFilterMessageType(IntEnum):
    REGISTER = 1 << 0
    UNREGISTER = 1 << 1
    ACK = 1 << 2
    NOTIFICATION = 1 << 3


class PushdataFilterStateUpdate(NamedTuple):
    request_id: RequestId
    command: PushdataFilterMessageType
    entries: list[TipFilterRegistrationEntry]
    matches: list[PushdataRowParsed]
    block_hash: bytes    # ZERO_HASH if PushdataFilterMessageType is not NOTIFICATION

    def __str__(self) -> str:
        return f"PushdataFilterStateUpdate(request_id={self.request_id}, " \
               f"command={repr(OutpointMessageType(self.command))}, " \
               f"entries={[str(entry) for entry in self.entries]}, " \
               f"matches={self.matches}, " \
               f"block_hash={self.block_hash!r})"

class OutputSpendRow(NamedTuple):
    out_tx_hash: bytes
    out_idx: int
    in_tx_hash: bytes
    in_idx: int
    block_hash: Optional[bytes]

    @classmethod
    def from_output_spend_struct(cls, buf: bytes) -> 'OutputSpendRow':
        out_tx_hash, out_idx, in_tx_hash, in_idx, block_hash = output_spend_struct.unpack(buf)
        return cls(out_tx_hash, out_idx, in_tx_hash, in_idx, block_hash)


class RestorationFilterRequest(TypedDict):
    filterKeys: list[str]


class RestorationFilterJSONResponse(TypedDict):
    flags: int
    pushDataHashHex: str
    lockingTransactionId: str
    lockingTransactionIndex: int
    unlockingTransactionId: Optional[str]
    unlockingInputIndex: int


class RestorationFilterResult(NamedTuple):
    flags: int # one byte integer
    push_data_hash: bytes
    locking_transaction_hash: bytes
    locking_output_index: int
    unlocking_transaction_hash: bytes  # null hash
    unlocking_input_index: int  # 0


RESULT_UNPACK_FORMAT = ">B32s32sI32sI"
FILTER_RESPONSE_SIZE = 1 + 32 + 32 + 4 + 32 + 4
assert struct.calcsize(RESULT_UNPACK_FORMAT) == FILTER_RESPONSE_SIZE

filter_response_struct = struct.Struct(RESULT_UNPACK_FORMAT)


class IndexerPushdataRegistrationFlag(enum.IntFlag):
    NONE                            = 0
    FINALISED                       = 1 << 0
    DELETING                        = 1 << 1

    MASK_ALL = ~NONE
    MASK_DELETING_CLEAR             = ~DELETING
    MASK_FINALISED_CLEAR            = ~FINALISED
    MASK_FINALISED_DELETING_CLEAR   = ~(FINALISED | DELETING)


def le_int_to_char(le_int: int) -> bytes:
    return struct.pack('<I', le_int)[0:1]


class TxOrId(enum.IntEnum):
    TRANSACTION_ID = 0
    FULL_TRANSACTION = 1 << 0

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


def tsc_merkle_proof_json_to_binary(tsc_json: dict[str, Any], include_full_tx: bool,
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
        raise NotImplementedError(f"Invalid target_type value '{target_type}'")

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


class TipFilterRegistrationResponse(TypedDict):
    dateCreated: str


TIP_FILTER_ENTRY_FORMAT = ">32sI"
tip_filter_entry_struct = struct.Struct(TIP_FILTER_ENTRY_FORMAT)


class AccountMetadata(NamedTuple):
    account_id: int
    external_account_id: int
    account_flags: AccountFlag


class TipFilterNotificationMatch(TypedDict):
    pushDataHashHex: str
    transactionId: str
    transactionIndex: int
    flags: int


class TipFilterNotificationEntry(TypedDict):
    accountId: int
    matches: list[TipFilterNotificationMatch]


class TipFilterNotificationBatch(TypedDict):
    blockId: Optional[str]
    entries: list[TipFilterNotificationEntry]


class OutboundDataRow(NamedTuple):
    outbound_data_id: Optional[int]
    outbound_data: bytes
    outbound_data_flags: OutboundDataFlag
    date_created: int
    date_last_tried: int

class HeaderJSONType(TypedDict):
    """Compatible with 'HeaderSV' service json response object"""
    hash: str
    version: int
    prevBlockHash: str
    merkleRoot: str
    creationTimestamp: int
    difficultyTarget: int
    nonce: int
    transactionCount: int
    work: int


@dataclass
class HeaderTipState:
    LONGEST_CHAIN = "LONGEST_CHAIN"
    STALE = "STALE"

class HeaderTipJSONType(TypedDict):
    """Compatible with 'HeaderSV' service json response object"""
    header: HeaderJSONType
    state: str
    chainWork: int
    height: int
    # confirmations: int  # never not 1 - I think this field was included in error in HeaderSV
