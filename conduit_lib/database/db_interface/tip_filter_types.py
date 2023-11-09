import struct
from enum import IntFlag
from typing import NamedTuple


class DatabaseError(Exception):
    pass


class DatabaseStateModifiedError(DatabaseError):
    # The database state was not as we required it to be in some way.
    pass


class DatabaseInsertConflict(DatabaseError):
    pass


class FilterNotificationRow(NamedTuple):
    account_id: int
    pushdata_hash: bytes


class AccountFlag(IntFlag):
    NONE = 0


class OutboundDataFlag(IntFlag):
    NONE = 0
    TIP_FILTER_NOTIFICATIONS = 1 << 0
    DISPATCHED_SUCCESSFULLY = 1 << 20


class AccountMetadata(NamedTuple):
    account_id: int
    external_account_id: int


class IndexerPushdataRegistrationFlag(IntFlag):
    NONE = 0
    FINALISED = 1 << 0
    DELETING = 1 << 1

    MASK_ALL = ~NONE
    MASK_DELETING_CLEAR = ~DELETING
    MASK_FINALISED_CLEAR = ~FINALISED
    MASK_FINALISED_DELETING_CLEAR = ~(FINALISED | DELETING)


class OutboundDataRow(NamedTuple):
    outbound_data_id: int | None
    outbound_data: bytes
    outbound_data_flags: OutboundDataFlag
    date_created: int
    date_last_tried: int


OUTPUT_SPEND_FORMAT = ">32sI32sI32s"
output_spend_struct = struct.Struct(OUTPUT_SPEND_FORMAT)


class OutputSpendRow(NamedTuple):
    out_tx_hash: bytes
    out_idx: int
    in_tx_hash: bytes
    in_idx: int
    block_hash: bytes | None

    @classmethod
    def from_output_spend_struct(cls, buf: bytes) -> "OutputSpendRow":
        (
            out_tx_hash,
            out_idx,
            in_tx_hash,
            in_idx,
            block_hash,
        ) = output_spend_struct.unpack(buf)
        return cls(out_tx_hash, out_idx, in_tx_hash, in_idx, block_hash)


class TipFilterRegistrationEntry(NamedTuple):
    pushdata_hash: bytes
    duration_seconds: int  # when unregistering duration_seconds=0xffffffff

    def __str__(self) -> str:
        return (
            f"TipFilterRegistrationEntry(pushdata_hash={self.pushdata_hash.hex()}, "
            f"duration_seconds={self.duration_seconds})"
        )
