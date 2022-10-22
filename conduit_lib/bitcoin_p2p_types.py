import array
import asyncio
import enum
from pathlib import Path
from typing import NamedTuple

from conduit_lib.database.ffdb.flat_file_db import DataLocation


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
    tx_offsets_for_chunk: list[int]


class BlockMsgData(NamedTuple):
    block_type: BlockType
    block_hash: bytes
    tx_offsets: list[int]
    block_size: int
    # SMALL BLOCK
    small_block_data: bytes | None
    # BIG BLOCK
    big_block_filepath: Path | None = None


class BigBlock(NamedTuple):
    block_hash: bytes
    temp_file_location: DataLocation
    tx_count: int


SmallBlocks = list[bytes]


class BitcoinPeerInstance(NamedTuple):
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    host: str
    port: int

    async def send_message(self, message: bytes):
        self.writer.write(message)
