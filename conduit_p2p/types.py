# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.

import array
import enum
from enum import IntEnum
from typing import NamedTuple


class ExtendedP2PHeader(NamedTuple):
    magic: bytes
    command: bytes
    payload_size: int
    checksum: bytes
    ext_command: bytes | None
    ext_length: int | None

    def is_extended(self) -> bool:
        if self.ext_command:
            return True
        return False

    def length(self) -> int:
        if self.is_extended():
            return 44
        return 24


class BlockType(enum.IntEnum):
    SMALL_BLOCK = 1 << 0  # fits in the network buffer -> write in batches periodically
    BIG_BLOCK = 1 << 1  # overflows network buffer -> use temp file to write to disc in chunks


class BlockChunkData(NamedTuple):
    chunk_num: int
    num_chunks: int
    block_hash: bytes
    raw_block_chunk: bytes
    tx_offsets_for_chunk: "array.ArrayType[int]"
    block_size: int


class BlockDataMsg(NamedTuple):
    block_type: BlockType
    block_hash: bytes
    tx_offsets: "array.ArrayType[int]"
    block_size: int
    small_block_data: bytes | None


class DataLocation(NamedTuple):
    """This metadata must be persisted elsewhere.
    For example, a key-value store such as LMDB"""

    file_path: str
    start_offset: int
    end_offset: int


class BigBlock(NamedTuple):
    block_hash: bytes
    temp_file_location: DataLocation
    tx_count: int


SmallBlocks = list[bytes]


class InvType(IntEnum):
    TX = 1
    BLOCK = 2


class BlockHeader(NamedTuple):
    version: int
    prev_block: bytes
    merkle_root: bytes
    timestamp: int
    bits: int
    nonce: int
    raw: bytes
    hash: bytes


class BitcoinClientMode(IntEnum):
    HIGH_LEVEL = 1
    LOW_LEVEL = 2


class Reject(NamedTuple):
    message: str
    ccode_translation: str
    reason: str
    item_hash: str


class BroadcastResult(NamedTuple):
    rejected: Reject | None
    relaying_peer_ids: list[int]
