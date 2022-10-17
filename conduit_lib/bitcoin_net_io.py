from __future__ import annotations

import asyncio
import io
from asyncio import StreamReader, \
    StreamWriter
from multiprocessing import shared_memory
from typing import Callable, cast, NamedTuple
from bitcoinx import read_varint
import logging
import struct

from .commands import BLOCK_BIN
from .constants import HEADER_LENGTH
from .utils import create_task


class P2PHeader(NamedTuple):
    magic: bytes
    command: bytes
    payload_size: int
    checksum: bytes


class BlockCallback(NamedTuple):
    cur_msg_start_pos: int
    cur_msg_end_pos: int
    raw_block_header: bytes
    block_tx_count: int


logger = logging.getLogger("bitcoin-p2p-socket")
logger.setLevel(logging.DEBUG)


async def get_bitcoin_p2p_socket(host: str, port: int,
        on_msg_callback: Callable[[bytes, BlockCallback | bytes], None],
        on_connection_made_callback: Callable[[], None],
        on_connection_lost_callback: Callable[[], None]) -> BitcoinNetSocket:
    try:
        reader, writer = await asyncio.open_connection(host, port)
        logger.info("Connection made")
        on_connection_made_callback()
        bitcoin_p2p_socket = BitcoinNetSocket(reader, writer, on_msg_callback)
        create_task(bitcoin_p2p_socket.start_session())
        return bitcoin_p2p_socket
    except ConnectionResetError:
        on_connection_lost_callback()
        raise


def unpack_msg_header(header: bytes) -> P2PHeader:
    return P2PHeader(*struct.unpack_from("<4s12sI4s", header, offset=0))


class BitcoinNetSocket:

    BUFFER_SIZE = (1024**2)*256
    _pos = 0
    _last_msg_end_pos = 0
    _payload_size = 0

    def __init__(self, reader: StreamReader, writer: StreamWriter,
            on_msg_callback: Callable[[bytes, BlockCallback | bytes], None]) -> None:
        self.reader: StreamReader = reader
        self.writer: StreamWriter = writer
        self.on_msg_callback = on_msg_callback

        self.shm_buffer = shared_memory.SharedMemory(create=True, size=self.BUFFER_SIZE)

    def get_block_tx_count(self, buffer: memoryview, end_blk_header_pos: int) -> int:
        # largest varint (tx_count) is 9 bytes
        block_tx_count_view = buffer[end_blk_header_pos: end_blk_header_pos + 10].tobytes()
        return cast(int, read_varint(io.BytesIO(block_tx_count_view).read))

    def make_special_blk_msg(self, buffer: memoryview,
            cur_msg_start_pos: int, cur_msg_end_pos: int) -> BlockCallback:
        end_blk_header_pos = cur_msg_start_pos + 80
        raw_block_header = buffer[cur_msg_start_pos:end_blk_header_pos].tobytes()
        block_tx_count = self.get_block_tx_count(buffer, end_blk_header_pos)
        return BlockCallback(
            cur_msg_start_pos,
            cur_msg_end_pos,
            raw_block_header,
            block_tx_count,
        )

    async def send_message(self, message: bytes) -> None:
        self.writer.write(message)

    def available_network_buffer_bytes(self) -> int:
        return self.BUFFER_SIZE - self._last_msg_end_pos

    def new_buffer(self, pos: int, last_msg_end_pos: int) -> tuple[int, int]:
        """takes the remainder of buffer and places it at the start then extends to size"""
        assert self.BUFFER_SIZE - pos == 0
        remainder = self.shm_buffer.buf[last_msg_end_pos:].tobytes()
        self.shm_buffer.buf[0: len(remainder)] = remainder
        # new block_offset for most recent, partially read msg
        pos = pos - last_msg_end_pos
        last_msg_end_pos = 0
        return pos, last_msg_end_pos

    async def start_session(self) -> None:
        def next_header_available(pos: int, last_msg_end_pos: int) -> bool:
            return pos - last_msg_end_pos >= HEADER_LENGTH

        def next_payload_available(pos: int, last_msg_end_pos: int, payload_size: int) -> bool:
            return pos - last_msg_end_pos >= HEADER_LENGTH + payload_size

        pos = 0  # how many bytes have been read into the buffer
        last_msg_end_pos = 0
        cur_header = P2PHeader(b"", b"", 0, b"")
        while True:
            data = await self.reader.read(self.BUFFER_SIZE - pos)
            if not data:
                logger.error(f"Bitcoin node disconnected")
                self.close_connection()
                return

            self.shm_buffer.buf[pos:pos+len(data)] = data
            pos += len(data)

            while next_header_available(pos, last_msg_end_pos):
                header_data = self.shm_buffer.buf[last_msg_end_pos: last_msg_end_pos + HEADER_LENGTH].tobytes()
                cur_header = unpack_msg_header(header_data)
                if next_payload_available(pos, last_msg_end_pos, cur_header.payload_size):
                    cur_msg_start_pos = last_msg_end_pos + HEADER_LENGTH
                    cur_msg_end_pos   = last_msg_end_pos + HEADER_LENGTH + cur_header.payload_size

                    if cur_header.command not in {BLOCK_BIN}:
                        full_message = self.shm_buffer.buf[cur_msg_start_pos:cur_msg_end_pos].tobytes()
                        self.on_msg_callback(cur_header.command, full_message)
                    elif cur_header.command == BLOCK_BIN:
                        full_raw_block: BlockCallback = self.make_special_blk_msg(self.shm_buffer.buf,
                            cur_msg_start_pos, cur_msg_end_pos)
                        self.on_msg_callback(cur_header.command, full_raw_block)
                    else:
                        logger.warning("Unrecognized message type: %s", cur_header.command)
                    last_msg_end_pos = cur_msg_end_pos
                else:
                    break

            if self.BUFFER_SIZE - pos == 0:
                logger.debug(f"Buffer is full: {cur_header}")
                pos, last_msg_end_pos = self.new_buffer(pos, last_msg_end_pos)

    def close_connection(self) -> None:
        logger.info("Closing bitcoin p2p socket connection gracefully")
        if self.writer:
            self.writer.close()
