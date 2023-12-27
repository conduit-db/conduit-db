# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.

import array
import asyncio
from concurrent.futures import ThreadPoolExecutor
from bitcoinx import double_sha256, hash_to_hex_str
import math
import os
from asyncio import StreamReader, StreamWriter, Task
from pathlib import Path
from typing import BinaryIO, cast, Any
import logging
import struct

from pyzstd import SeekableZstdFile

from . import NetworkConfig, Serializer
from .algorithms import unpack_varint, preprocessor
from .bitcoin_p2p_types import (
    BlockType,
    BlockChunkData,
    BlockDataMsg,
    BitcoinPeerInstance,
    ExtendedP2PHeader,
)
from .commands import BLOCK_BIN, VERACK_BIN, EXTMSG_BIN
from .constants import HEADER_LENGTH, EXTENDED_HEADER_LENGTH
from .database.ffdb.compression import (
    open_seekable_writer_zstd,
    update_compresson_stats,
    CompressionStats,
    write_compression_stats,
    CompressionBlockInfo,
)
from .handlers import MessageHandlerProtocol
from .utils import create_task, bin_p2p_command_to_ascii

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))


class GracefulDisconnect(Exception):
    pass


class BitcoinP2PClientError(Exception):
    pass


def unpack_msg_header(header: bytes) -> ExtendedP2PHeader:
    magic, command, length, checksum = struct.unpack_from("<4s12sI4s", header, offset=0)
    ext_command: bytes | None = None
    ext_length: int | None = None
    return ExtendedP2PHeader(
        magic=magic,
        command=command,
        payload_size=length,
        checksum=checksum,
        ext_command=ext_command,
        ext_length=ext_length,
    )


def unpack_extended_msg_header(header: bytes) -> ExtendedP2PHeader:
    (
        magic,
        command,
        length,
        checksum,
        ext_command,
        ext_length,
    ) = struct.unpack_from("<4s12sI4s12sQ", header, offset=0)
    assert command == "extmsg"
    assert length == 0xFFFFFFFF
    assert checksum == 0x00000000
    return ExtendedP2PHeader(
        magic=magic,
        command=command,
        payload_size=length,
        checksum=checksum,
        ext_command=ext_command,
        ext_length=ext_length,
    )


class BitcoinP2PClient:
    """
    Big blocks are blocks larger than size.BUFFER_SIZE and are streamed directly to a
    temporary file and just need to be os.move'd into the
    correct final resting place.

    Small blocks are blocks less than or equal to size.BUFFER_SIZE and are better to be
    concatenated in memory before writing them all to the same file. Otherwise the spinning HDD
    discs will 'stutter' with too many tiny writes.
    """

    def __init__(
        self,
        id: int,
        remote_host: str,
        remote_port: int,
        message_handler: MessageHandlerProtocol,
        net_config: NetworkConfig,
        reader: StreamReader | None = None,
        writer: StreamWriter | None = None,
        use_compression: bool = True,
    ) -> None:
        self.id = id
        self.logger = logging.getLogger(f"bitcoin-p2p-socket(id={self.id})")
        self.logger.setLevel(logging.DEBUG)
        self.remote_host = remote_host
        self.remote_port = remote_port
        self.message_handler = message_handler
        self.net_config = net_config
        self.serializer = Serializer(net_config)
        self.use_compression = use_compression

        # Any blocks that exceed the size of the network buffer are instead writted to file
        self.big_block_write_directory = Path(os.environ.get("DATADIR_HDD", MODULE_DIR)) / "big_blocks"
        if not self.big_block_write_directory.exists():
            os.makedirs(self.big_block_write_directory, exist_ok=True)

        self.message_handler_task_count = 4
        self.message_queue: asyncio.Queue[tuple[bytes, bytes]] = asyncio.Queue(maxsize=100)
        self.reader: StreamReader | None = reader
        self.writer: StreamWriter | None = writer

        self.connection_lost_event = asyncio.Event()
        self.handshake_complete_event = asyncio.Event()
        self.connected = False

        self.tasks: list[Task[Any]] = []

        # Network Buffer Manipulation
        try:
            self.BUFFER_SIZE = int(os.environ["NETWORK_BUFFER_SIZE"])
        except KeyError:
            raise BitcoinP2PClientError("The environment variable 'NETWORK_BUFFER_SIZE' " "needs to be set")
        self.network_buffer: bytearray = bytearray(self.BUFFER_SIZE)
        self.cur_msg_end_pos = 0
        self.cur_msg_start_pos = 0
        self.pos = 0
        self.last_msg_end_pos = 0
        self.cur_header = ExtendedP2PHeader(b"", b"", 0, b"", b"", 0)

        # Preprocessing of tx byte offsets in each raw block
        self.tx_offsets_array = array.array("Q", bytes(0))
        self.tx_count_done: int = 0

        # There must only be a single write thread! Otherwise appending to the same file for
        # big blocks will cause corruption
        self.file_write_executor = ThreadPoolExecutor(max_workers=1)

    async def connect(self) -> None:
        """raises `ConnectionResetError`"""
        reader, writer = await asyncio.open_connection(host=self.remote_host, port=self.remote_port)
        self.connected = True
        self.logger.debug(
            f"Connection made to peer: {self.remote_host}:{self.remote_port} (peer_id={self.id})"
        )
        self.reader = reader
        self.writer = writer
        self.peer = BitcoinPeerInstance(self.reader, self.writer, self.remote_host, self.remote_port)
        self.tasks.append(create_task(self.start_session()))

    async def wait_for_connection(self) -> None:
        """Keep retrying until the node comes online"""
        while True:
            try:
                await self.connect()
                return
            except ConnectionRefusedError:
                self.logger.debug(
                    f"Bitcoin node on:  {self.remote_host}:{self.remote_port} currently unavailable "
                    f"- waiting..."
                )
                await asyncio.sleep(5)

    async def send_message(self, message: bytes) -> None:
        assert self.writer is not None
        self.writer.write(message)

    async def close_connection(self) -> None:
        self.logger.info("Closing bitcoin p2p socket connection gracefully")
        if self.writer and not self.writer.is_closing():
            self.writer.close()
        self.connected = False
        self.connection_lost_event.set()
        for task in self.tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    async def handle_message_task_async(self) -> None:
        while True:
            command, message = await self.message_queue.get()
            handler_func_name = "on_" + command.rstrip(b"\0").decode("ascii")
            try:
                handler_func = getattr(self.message_handler, handler_func_name)
                await handler_func(message, self.peer)
            except AttributeError:
                self.logger.debug(f"Handler not implemented for command: {command!r}")
            if command == VERACK_BIN:
                self.handshake_complete_event.set()

    def rotate_buffer(self) -> tuple[int, int]:
        """takes the remainder of buffer and places it at the start then extends to size"""
        assert self.BUFFER_SIZE - self.pos == 0
        remainder = self.network_buffer[self.last_msg_end_pos :]
        self.network_buffer[0 : len(remainder)] = remainder
        self.pos = self.pos - self.last_msg_end_pos
        self.last_msg_end_pos = 0
        return self.pos, self.last_msg_end_pos

    def next_header_available(self) -> bool:
        return self.pos - self.last_msg_end_pos >= HEADER_LENGTH

    def next_extended_header_available(self) -> bool:
        return self.pos - self.last_msg_end_pos >= EXTENDED_HEADER_LENGTH

    def next_payload_available(self) -> bool:
        return self.pos - self.last_msg_end_pos >= HEADER_LENGTH + self.cur_header.payload_size

    def next_extended_payload_available(self) -> bool:
        return self.pos - self.last_msg_end_pos >= EXTENDED_HEADER_LENGTH + self.cur_header.payload_size

    async def read_block_chunk(self, total_block_bytes_read: int) -> bytes:
        assert self.reader is not None
        remaining_bytes = self.cur_header.payload_size - total_block_bytes_read
        if remaining_bytes >= self.BUFFER_SIZE:
            n_to_read = self.BUFFER_SIZE
        else:
            n_to_read = remaining_bytes
        data = await self.reader.readexactly(n_to_read)
        if not data:
            raise ConnectionResetError
        return data

    async def read_until_buffer_full(self) -> bytes:
        assert self.reader is not None
        data = await self.reader.readexactly(self.BUFFER_SIZE - self.pos)
        if not data:
            raise ConnectionResetError
        self.network_buffer[self.pos :] = data
        self.pos += len(data)
        assert self.pos == self.BUFFER_SIZE
        return data

    def clip_p2p_header_from_front_of_buffer(self) -> None:
        self.network_buffer[: -self.cur_header.length()] = self.network_buffer[self.cur_header.length() :]
        self.pos = self.pos - self.cur_header.length()

    async def handle_block(self, block_type: BlockType) -> None:
        """
        If `block_type` is BlockType.SMALL_BLOCK, process as a single chunk in memory and
        call the `on_block` handler immediately (without any calls to `on_block_chunk`

        If `block_type` is BlockType.BIG_BLOCK, read from the network socket in chunks
        up to self.BUFFER_SIZE in size and call the `on_block_chunk` handler for each chunk
        until the final chunk, in which case both the `on_block_chunk` and `on_block` handlers
        are called.

        The `on_block_chunk` allows for intercepting of the chunks whilst still in memory for e.g:
        - writing the chunks incrementally to disc
        - handing off the chunks to worker processes

        In this way, arbitrarily large blocks can be handled without exceeding memory allocation
        limits.

        raises `ConnectionResetError`
        """
        # Init local variables - Keeping them local avoids polluting instance state
        tx_offsets_all: "array.ArrayType[int]" = array.array("Q")
        block_hash = bytes()
        last_tx_offset_in_chunk: int | None = None
        adjustment = 0
        if block_type & BlockType.SMALL_BLOCK:
            raw_block = self.get_next_payload()
            raw_block_header = raw_block[0:80]
            block_hash = double_sha256(raw_block_header)
            tx_count, var_int_size = unpack_varint(raw_block[80:89], 0)
            offset = 80 + var_int_size
            tx_offsets_for_chunk, last_tx_offset_in_chunk = preprocessor(raw_block, offset, adjustment)
            tx_offsets_all.extend(tx_offsets_for_chunk)
            assert last_tx_offset_in_chunk == len(raw_block) == self.cur_header.payload_size
            block_data_msg = BlockDataMsg(
                block_type,
                block_hash,
                array.array("Q", tx_offsets_all),
                self.cur_header.payload_size,
                raw_block,
                big_block_filepath=None,
            )
            await self.message_handler.on_block(block_data_msg, self.peer)
            return

        self.logger.debug(f"Handling a 'Big Block' (>{self.BUFFER_SIZE})")
        chunk_num = 0
        num_chunks = math.ceil(self.cur_header.payload_size / self.BUFFER_SIZE)
        expected_tx_count_for_block = 0
        total_block_bytes_read = 0
        remainder = b""  # the bit left over due to a tx going off the end of the current chunk
        big_block_filepath = self.big_block_write_directory / os.urandom(16).hex()
        uncompressed_bytes_written_total = 0  #
        file: BinaryIO | None = None
        file_zstd: SeekableZstdFile | None = None
        compression_stats = CompressionStats()
        if self.use_compression:
            file_zstd = open_seekable_writer_zstd(big_block_filepath, mode='wb')
        else:
            file = open(big_block_filepath, "ab")

        worker_id = None
        try:
            assert self.reader is not None
            worker_id = await self.message_handler.acquire_next_available_worker_id()
            while total_block_bytes_read < self.cur_header.payload_size:
                chunk_num += 1
                if chunk_num == 1:
                    self.clip_p2p_header_from_front_of_buffer()
                    _data = await self.read_until_buffer_full()
                    self.pos, self.last_msg_end_pos = self.rotate_buffer()
                    next_chunk: bytes = cast(bytes, self.network_buffer[0 : self.BUFFER_SIZE])
                    total_block_bytes_read += len(next_chunk)
                    self.logger.debug(f"total_block_bytes_read = {total_block_bytes_read}")
                else:
                    next_chunk = await self.read_block_chunk(total_block_bytes_read)
                    total_block_bytes_read += len(next_chunk)
                    self.logger.debug(f"total_block_bytes_read = {total_block_bytes_read}")

                # ---------- TxOffsets logic start ---------- #
                if chunk_num == 1:
                    raw_block_header = next_chunk[0:80]
                    block_hash = double_sha256(raw_block_header)
                    expected_tx_count_for_block, var_int_size = unpack_varint(next_chunk[80:89], 0)
                    offset = 80 + var_int_size
                else:
                    offset = 0
                    assert last_tx_offset_in_chunk is not None
                    adjustment = last_tx_offset_in_chunk

                modified_chunk = remainder + next_chunk
                tx_offsets_for_chunk, last_tx_offset_in_chunk = preprocessor(
                    modified_chunk, offset, adjustment
                )
                tx_offsets_all.extend(tx_offsets_for_chunk)
                len_slice = last_tx_offset_in_chunk - adjustment
                remainder = modified_chunk[len_slice:]

                # `tx_offsets_for_chunk` corresponds exactly to `slice_for_worker`
                slice_for_worker = modified_chunk[:len_slice]
                # ---------- TxOffsets logic end ---------- #

                # Big blocks use a file for writing to incrementally
                if self.use_compression:
                    assert file_zstd is not None
                    uncompressed_size, compressed_size = await self.write_file_async_zstd(
                        file_zstd, next_chunk
                    )
                    update_compresson_stats(
                        big_block_filepath, uncompressed_size, compressed_size, compression_stats
                    )
                else:
                    assert file is not None
                    await self.write_file_async(file, next_chunk)

                block_chunk_data = BlockChunkData(
                    chunk_num,
                    num_chunks,
                    block_hash,
                    slice_for_worker,
                    tx_offsets_for_chunk,
                )
                await self.message_handler.on_block_chunk(block_chunk_data, self.peer, worker_id)

                if chunk_num == num_chunks:
                    block_metadata = CompressionBlockInfo(
                        block_id=hash_to_hex_str(block_hash),
                        tx_count=expected_tx_count_for_block,
                        size_mb=self.cur_header.payload_size,
                    )
                    compression_stats.block_metadata = [block_metadata]
                    write_compression_stats(compression_stats)
                    self.last_msg_end_pos = len(next_chunk)
                    self.pos = len(next_chunk)
                    break

            # Safety checks
            assert len(block_hash) == 32
            assert total_block_bytes_read == self.cur_header.payload_size
            if self.use_compression:
                assert file_zstd is not None
                assert file_zstd.tell() == self.cur_header.payload_size
            else:
                assert file is not None
                assert file.tell() == self.cur_header.payload_size
        finally:
            if worker_id:
                await self.message_handler.release_worker_id(worker_id)
            if file:
                file.close()
            if file_zstd:
                file_zstd.close()

        assert chunk_num == num_chunks
        self.logger.debug(f"Finished streaming big block to disc")

        # Sanity checks and reset buffer, to be ready to receive the next p2p message
        assert total_block_bytes_read == self.cur_header.payload_size
        assert len(remainder) == 0
        assert len(tx_offsets_all) == expected_tx_count_for_block

        block_data_msg = BlockDataMsg(
            block_type,
            block_hash,
            tx_offsets_all,
            self.cur_header.payload_size,
            small_block_data=None,
            big_block_filepath=big_block_filepath,
        )
        # If the server is killed before this line then no Ack is sent to the controller
        # and the tip is not updated.
        await self.message_handler.on_block(block_data_msg, self.peer)

    async def write_file_async(self, file: BinaryIO, data: bytes) -> None:
        await asyncio.get_running_loop().run_in_executor(self.file_write_executor, file.write, data)

    def write_data_zstd(self, seekable_zstd: SeekableZstdFile, data: bytes) -> tuple[int, int]:
        uncompressed_start_offset = 0  # SeekableZstdFile.tell() always 0 in append mode
        compressed_start_offset = seekable_zstd._fp.tell()  # type: ignore[attr-defined]

        seekable_zstd.write(data)

        uncompressed_end_offset = len(data)
        seekable_zstd.flush(SeekableZstdFile.FLUSH_FRAME)
        uncompressed_size = uncompressed_end_offset - uncompressed_start_offset
        compressed_size = seekable_zstd._fp.tell() - compressed_start_offset  # type: ignore[attr-defined]
        return uncompressed_size, compressed_size

    async def write_file_async_zstd(self, file: SeekableZstdFile, data: bytes) -> tuple[int, int]:
        return await asyncio.get_running_loop().run_in_executor(
            self.file_write_executor, self.write_data_zstd, file, data
        )

    def log_buffer_full_message(self) -> None:
        self.logger.debug(
            f"Buffer is full for command: "
            f"'{bin_p2p_command_to_ascii(self.cur_header.command)}', "
            f"payload size: {self.cur_header.payload_size}"
        )

    def get_next_p2p_header(self) -> ExtendedP2PHeader:
        from_offset = self.last_msg_end_pos
        to_offset = self.last_msg_end_pos + HEADER_LENGTH
        header_data = self.network_buffer[from_offset:to_offset]
        self.cur_header = unpack_msg_header(header_data)
        return self.cur_header

    def get_next_extended_p2p_header(self) -> ExtendedP2PHeader:
        from_offset = self.last_msg_end_pos
        to_offset = self.last_msg_end_pos + EXTENDED_HEADER_LENGTH
        header_data = self.network_buffer[from_offset:to_offset]
        self.cur_header = unpack_extended_msg_header(header_data)
        return self.cur_header

    def get_next_payload(self) -> bytes:
        return self.network_buffer[self.cur_msg_start_pos : self.cur_msg_end_pos]

    async def send_version(self, local_host: str, local_port: int) -> None:
        message = self.serializer.version(
            recv_host=self.remote_host,
            recv_port=self.remote_port,
            send_host=local_host,
            send_port=local_port,
        )
        await self.send_message(message)

    async def handshake(self, local_host: str, local_port: int) -> None:
        create_task(self.send_version(local_host, local_port))
        await self.handshake_complete_event.wait()

    async def start_session(self) -> None:
        try:
            await self._start_session()
        except ConnectionResetError:
            self.logger.error(f"Bitcoin node disconnected")
        finally:
            if self.writer and not self.writer.is_closing():
                await self.close_connection()

    async def _start_session(self) -> None:
        """raises `ConnectionResetError`"""
        assert self.reader is not None
        assert self.writer is not None
        assert self.peer is not None

        for i in range(self.message_handler_task_count):
            self.tasks.append(create_task(self.handle_message_task_async()))
        self.tasks.append(create_task(self.keepalive_ping_loop()))

        self.pos = 0  # how many bytes have been read into the buffer
        self.last_msg_end_pos = 0
        while True:
            assert (self.BUFFER_SIZE - self.pos) != 0, "Tried to read zero bytes from socket"
            data = await self.reader.read(self.BUFFER_SIZE - self.pos)
            if not data:
                raise ConnectionResetError

            self.network_buffer[self.pos : self.pos + len(data)] = data
            self.pos += len(data)

            while self.next_header_available():
                self.cur_header = self.get_next_p2p_header()
                next_payload_available = self.next_payload_available()
                if self.cur_header.command == EXTMSG_BIN:
                    if self.next_extended_header_available():
                        self.cur_header = self.get_next_extended_p2p_header()
                        next_payload_available = self.next_extended_payload_available()
                    else:
                        break  # read more data -> try to get enough for the full next header

                if next_payload_available:
                    self.cur_msg_start_pos = self.last_msg_end_pos + HEADER_LENGTH
                    self.cur_msg_end_pos = (
                        self.last_msg_end_pos + HEADER_LENGTH + self.cur_header.payload_size
                    )
                    if self.cur_header.command == BLOCK_BIN or self.cur_header.ext_command == BLOCK_BIN:
                        await self.handle_block(block_type=BlockType.SMALL_BLOCK)
                    else:
                        await self.message_queue.put((self.cur_header.command, self.get_next_payload()))
                    self.last_msg_end_pos = self.cur_msg_end_pos
                else:
                    break  # read more data -> try to get enough for the full next payload

            if self.BUFFER_SIZE - self.pos == 0:
                self.log_buffer_full_message()
                self.pos, self.last_msg_end_pos = self.rotate_buffer()

                # Big Block exceeding network buffer size -> next full payload is not possible
                if (
                    self.cur_header.command == BLOCK_BIN or self.cur_header.ext_command == BLOCK_BIN
                ) and self.cur_header.payload_size > self.BUFFER_SIZE:
                    await self.handle_block(block_type=BlockType.BIG_BLOCK)

    async def keepalive_ping_loop(self) -> None:
        await self.handshake_complete_event.wait()
        self.logger.debug(f"Handshake event complete")
        await asyncio.sleep(10)
        while True:
            ping_msg = self.serializer.ping()
            await self.send_message(ping_msg)
            await asyncio.sleep(2 * 60)  # Matches bitcoin-sv/net/net.h constant PING_INTERVAL


class PeerManagerFailedError(Exception):
    pass


class PeerManager:
    """This can also manage multiple connections to the same node on the same port to overcome
    latency effects on data transfer rates.
    See: https://en.wikipedia.org/wiki/Bandwidth-delay_product"""

    def __init__(self, clients: list[BitcoinP2PClient]) -> None:
        self._logger = logging.getLogger("bitcoin-p2p-client-pool")
        self.clients = clients
        self._connection_pool: list[BitcoinP2PClient] = []
        self._disconnected_pool: list[BitcoinP2PClient] = []
        self._currently_selected_peer_index: int = 0

    def add_client(self, client: BitcoinP2PClient) -> None:
        self.clients.append(client)

    def select_next_peer(self) -> None:
        if self._currently_selected_peer_index == len(self._connection_pool) - 1:
            self._currently_selected_peer_index = 0
        else:
            self._currently_selected_peer_index += 1

    def get_connected_peers(self) -> list[BitcoinP2PClient]:
        return self._connection_pool

    def get_next_peer(self) -> BitcoinP2PClient:
        self.select_next_peer()
        return self.get_current_peer()

    def get_current_peer(self) -> BitcoinP2PClient:
        return self._connection_pool[self._currently_selected_peer_index]

    async def try_connect_to_all_peers(self) -> None:
        """Should have tolerance for some failed connections to offline peers"""
        for client in self.clients:
            try:
                await client.wait_for_connection()
                self._connection_pool.append(client)
            except ConnectionResetError:
                self._logger.error(
                    f"Failed to connect to peer. host: {client.peer.host}, " f"port: {client.peer.port}"
                )
                self._disconnected_pool.append(client)
        if len(self._connection_pool) == 0:
            raise PeerManagerFailedError("All connections failed")

    async def try_handshake_for_all_peers(self, local_host: str, local_port: int) -> None:
        for client in self._connection_pool:
            await client.handshake(local_host, local_port)

    async def close(self) -> None:
        # Force close all outstanding connections
        outstanding_connections = list(self._connection_pool)
        for conn in outstanding_connections:
            if conn.connected:
                await conn.close_connection()
