import asyncio
import threading
from asyncio import BufferedProtocol

import bitcoinx
from bitcoinx import hex_str_to_hash, MissingHeader, Headers, Chain
from collections import namedtuple
import logging
import struct
from typing import Optional, List, Dict, Callable

from .commands import (
    VERSION,
    GETHEADERS,
    GETBLOCKS,
    GETDATA,
    BLOCK_BIN,
    TX_BIN,
)
from .handlers import Handlers
from .constants import (
    HEADER_LENGTH,
    ZERO_HASH,
)
from .deserializer import Deserializer
from .networks import NetworkConfig
from .peers import Peer
from .serializer import Serializer

Header = namedtuple("Header", "magic command payload_size checksum")


class BitcoinFramer(BufferedProtocol):
    logger = logging.getLogger("bitcoin-framer")
    _HWM = 1024 * 1024 * 1024  # 1GB - needs to accommodate largest conceivable block
    BUFFER_OVERFLOW_SIZE = 1024 * 1024 * 50  # 50MB
    BUFFER_SIZE = _HWM + BUFFER_OVERFLOW_SIZE  # high water mark level
    _buffer = bytearray(BUFFER_SIZE)  # initial buffer
    _buffer_view = memoryview(_buffer)
    _pos = 0
    _last_msg_end_pos = 0
    _msg_received_count = 0
    _msg_handled_count = 0

    def _new_buffer(self):
        assert self._pos >= self._HWM
        # take the remainder of buffer and place it at the start then extend to size
        remainder = self._buffer[self._last_msg_end_pos :]
        self._buffer = remainder + bytearray(self.BUFFER_SIZE - len(remainder))
        self._buffer_view = memoryview(self._buffer)
        self._pos = 0
        self._last_msg_end_pos = 0

    def get_buffer(self, sizehint):
        return self._buffer_view[self._pos :]

    def _unpack_msg_header(self) -> Header:
        cur_header_view = self._buffer_view[
            self._last_msg_end_pos : self._last_msg_end_pos + HEADER_LENGTH
        ]
        return Header(*struct.unpack_from("<4s12sI4s", cur_header_view, offset=0))

    def is_next_header_available(self) -> bool:
        return self._pos - self._last_msg_end_pos >= HEADER_LENGTH

    def is_next_payload_available(self) -> bool:
        return self._pos - self._last_msg_end_pos >= HEADER_LENGTH + self._payload_size

    def buffer_updated(self, nbytes):
        self._pos += nbytes

        while self.is_next_header_available():
            cur_header_bytes = self._unpack_msg_header()
            self._payload_size = cur_header_bytes.payload_size
            if self.is_next_payload_available():

                # we have the next full msg
                cur_msg_end_pos = (
                    self._last_msg_end_pos + HEADER_LENGTH + self._payload_size
                )
                sub_view_payload = self._buffer_view[
                    self._last_msg_end_pos + HEADER_LENGTH : cur_msg_end_pos
                ]
                self.message_received(cur_header_bytes.command, sub_view_payload)
                self._last_msg_end_pos = cur_msg_end_pos
            else:
                break  # recv more data until full payload available

        if self._pos >= self._HWM:
            self.logger.debug("buffer reached high-water mark, pausing reading...")
            self.transport.pause_reading()  # and wait for the 'go' signal
            loop = asyncio.get_running_loop()
            coro = self.wait_for_go_signal()
            asyncio.run_coroutine_threadsafe(coro, loop)

        def message_received(self):
            raise NotImplementedError

        async def wait_for_go_signal(self):
            raise NotImplementedError

        def reset_msg_counts(self):
            raise NotImplementedError


class BufferedSession(BitcoinFramer):
    """Designed to sync the blockchain as fast as possible.

    The main responsibility is for coordinating the networking component with a
    single peer (bitcoind daemon).But also coordinates the outsourcing of the other work
    such as parsing and committing block data to the (postgres) database - processes
    that can run in parallel leveraging a shared, memory view of each block."""

    WORKER_COUNT = 1

    def __init__(self, config: NetworkConfig, peer: Peer, host, port, storage):
        self.logger = logging.getLogger("session")
        self.loop = asyncio.get_event_loop()
        self.storage = storage

        # Bitcoin network/peer config
        self.config = config
        self.peer = peer
        self.host = host
        self.port = port
        self.protocol: BufferedProtocol = None
        self.protocol_factory = None

        # Serialization/Deserialization
        self.handlers = Handlers(self, self.config, self.storage)
        self.serializer = Serializer(self.config, self.storage)
        self.deserializer = Deserializer(self.config, self.storage)

        # Connection entry/exit
        self.handshake_complete_event = asyncio.Event()
        self.con_lost_event = asyncio.Event()

        # Syncing of chain
        self._headers_msg_processed_event = asyncio.Event()
        self._all_headers_synced_event = asyncio.Event()
        self._all_blocks_synced_event = asyncio.Event()
        self.target_header_height: Optional[int] = None
        self.target_block_height: Optional[int] = None
        self.local_tip_height: int = self.update_local_tip_height()
        self.local_block_tip_height: int = self.get_local_block_tip_height()
        self._pending_blocks_queue = asyncio.Queue()

        # Parallel block processing related
        self._blocks_batch_set = set()
        self._blocks_done_queue = asyncio.Queue()

        # Message queue
        self._incoming_msg_queue = asyncio.Queue()
        self._msg_received_count = 0
        self._msg_handled_count = 0
        self._msg_received_count_lock = threading.Lock()
        self._msg_handled_count_lock = threading.Lock()

        # self._block_parsed_event = asyncio.Event()
        self._merkle_tree_done_event = asyncio.Event()
        self._block_committed_event = asyncio.Event()
        self._work_done_queue = asyncio.Queue()
        self.block_partition_size: Optional[int] = None
        self.workers_map: Dict[int:Callable] = {}

    def run_coro_threadsafe(self, coro, *args, **kwargs):
        asyncio.run_coroutine_threadsafe(coro(*args, **kwargs), self.loop)

    def connection_made(self, transport):
        self.transport = transport
        self._payload_size = None
        self.logger.info("connection made")

        _fut = asyncio.create_task(self.handle())
        _fut = asyncio.create_task(self.manage_jobs())

    def connection_lost(self, exc):
        self.con_lost_event.set()
        self.logger.warning("The server closed the connection")

    def message_received(self, command: bytes, message: memoryview):
        self.logger.debug("received: %s message", command.rstrip(b"\0"))
        self.incr_msg_received_count()
        self._incoming_msg_queue.put_nowait((command, message))

    async def wait_for_go_signal(self):
        """spawned when buffer goes above HWM level. will then wait for count of
        messages received to == count of messages done"""
        while self._msg_received_count != self._msg_handled_count:
            await asyncio.sleep(0)

        self.reset_msg_counts()
        self._new_buffer()
        self.transport.resume_reading()

    def reset_msg_counts(self):
        with self._msg_handled_count_lock:
            self._msg_handled_count = 0
        with self._msg_received_count_lock:
            self._msg_received_count = 0

    def incr_msg_received_count(self):
        with self._msg_received_count_lock:
            self._msg_received_count += 1

    def incr_msg_handled_count(self):
        with self._msg_handled_count_lock:
            self._msg_handled_count += 1

    async def handle(self):
        while True:
            command, message = await self._incoming_msg_queue.get()
            try:
                print(f"command={command}")
                handler_func_name = "on_" + command.rstrip(b"\0").decode("ascii")
                handler_func = getattr(self.handlers, handler_func_name)
                await handler_func(message)
                if command not in [TX_BIN, BLOCK_BIN]:
                    self.incr_msg_handled_count()
            except Exception as e:
                self.logger.exception("handle: ", e)
                raise

    def is_synchronized(self):
        return self.get_local_block_tip_height() == self.target_block_height

    def send_message(self, message):
        self.transport.write(message)

    async def send_request(self, command_name: str, message: bytes):
        # self.logger.debug(f"sending to {self.peer}, msg type: {command_name}")
        self.send_message(message)

    def get_local_tip_height(self):
        return self.local_tip_height

    def get_local_block_tip_height(self) -> int:
        return self.storage.block_headers.longest_chain().tip.height

    def update_local_tip_height(self) -> int:
        self.local_tip_height = self.storage.headers.longest_chain().tip.height
        return self.local_tip_height

    def set_target_header_height(self, height) -> None:
        self.target_header_height = height

    async def manage_jobs(self):
        try:
            await self.handshake_complete_event.wait()
            _sync_headers_task = asyncio.create_task(self.sync_headers_job())

            if self.get_local_tip_height() != self.target_header_height:
                self.logger.debug("pre-waiting for _all_headers_synced_event")
                await self._all_headers_synced_event.wait()

            self.target_block_height = self.get_local_tip_height()  # blocks lag headers

            if not self.get_local_block_tip_height() == self.target_block_height:
                _sync_blocks_task = asyncio.create_task(self.sync_all_blocks_job())
                await self._all_blocks_synced_event.wait()
        except Exception as e:
            self.logger.exception(e)
            raise

    async def sync_headers_job(self):
        """supervises completion of syncing all headers to target height"""
        self.logger.debug("starting sync_headers_job...")
        while self.local_tip_height < self.target_header_height:
            block_locator_hashes = [self.storage.headers.longest_chain().tip.hash]
            hash_count = len(block_locator_hashes)
            await self.send_request(
                GETHEADERS,
                self.serializer.getheaders(hash_count, block_locator_hashes, ZERO_HASH),
            )
            await self._headers_msg_processed_event.wait()
            self.local_tip_height = self.update_local_tip_height()
            self.logger.debug(
                "headers message processed - new chain tip height: %s",
                self.local_tip_height,
            )
            self._headers_msg_processed_event.clear()

        self._all_headers_synced_event.set()
        self.logger.debug("headers synced. chain tip height=%s", self.local_tip_height)

    def get_header_for_hash(self, block_hash: bytes):
        header, chain = self.storage.headers.lookup(block_hash)
        return header

    def get_header_for_height(self, height: int) -> bitcoinx.Header:
        headers = self.storage.headers
        chain = self.storage.headers.longest_chain()
        header = headers.header_at_height(chain, height)
        return header

    async def join_batched_blocks(self):
        """This should run in a thread so that it can block on a multiprocessing queue
        - i.e. the _blocks_done_queue.get() and calls the self.incr_msg_handled_count()
        which has a threading.Lock around the count."""

        # join block parser workers
        self.logger.debug("waiting for block parsers to complete work...")
        done_block_heights = []
        while True:
            # This queue should transition to a multiprocessing queue
            block_hash, block_height = await self._blocks_done_queue.get()
            done_block_heights.append(block_height)
            self.incr_msg_handled_count()  # ensure merkle tree done too later..
            try:
                self._blocks_batch_set.remove(block_hash)
            except KeyError:
                header = self.get_header_for_hash(block_hash)
                self.logger.debug(f"also parsed block: {header.height}")

            # all blocks in batch parsed
            if len(self._blocks_batch_set) == 0:
                # in a crash prior to this point, the last 500 blocks will just get
                # re-done on start-up because the block_headers.mmap file
                # has not been updated with any headers. It's resistant to killing :)

                for height in done_block_heights:
                    header = self.get_header_for_height(height)
                    block_headers: bitcoinx.Headers = self.storage.block_headers
                    block_headers.connect(header.raw)
                return

    async def sync_batched_blocks(self) -> None:
        # one block per loop
        while True:
            inv = await self._pending_blocks_queue.get()
            try:
                header = self.get_header_for_hash(hex_str_to_hash(inv.get("inv_hash")))
            except MissingHeader as e:
                self.logger.warning(
                    "header with hash=%s not found", inv.get("inv_hash")
                )
                if self._pending_blocks_queue.empty():
                    break
                else:
                    continue

            if not header.height <= self.stop_header_height:
                self.logger.debug(f"ignoring block height={header.height} until sync'd")
                continue

            await self.send_request(
                GETDATA, self.serializer.getdata([inv]),
            )
            pending_blocks = self._pending_blocks_queue.qsize()
            self.logger.debug(f"pending blocks=" f"{pending_blocks}")

            if pending_blocks == 0:
                break
        await self.join_batched_blocks()

    def reset_pending_blocks_batch_set(self, hash_stop):
        self._blocks_batch_set = set()
        headers: Headers = self.storage.headers
        chain = self.storage.headers.longest_chain()

        local_tip_height = self.get_local_block_tip_height()
        if hash_stop == ZERO_HASH:
            self.stop_header_height = local_tip_height + 500
        else:
            self.stop_header_height = headers.lookup(hash_stop).height

        expected_batch_size = self.stop_header_height - local_tip_height
        for i in range(1, expected_batch_size + 1):
            block_header = headers.header_at_height(chain, local_tip_height + i)
            self._blocks_batch_set.add(block_header.hash)

    async def sync_all_blocks_job(self):
        """supervises completion of syncing all blocks to target height"""
        try:
            # up to 500 blocks per loop
            while self.get_local_block_tip_height() < self.target_block_height:
                chain = self.storage.block_headers.longest_chain()
                block_locator_hashes = [chain.tip.hash]
                hash_count = len(block_locator_hashes)
                self.logger.debug(
                    "requesting max number of blocks (up to 500) for chain tip "
                    "height=%s",
                    self.get_local_block_tip_height(),
                )
                hash_stop = ZERO_HASH  # get max
                self.reset_pending_blocks_batch_set(hash_stop)
                await self.send_request(
                    GETBLOCKS,
                    self.serializer.getblocks(
                        hash_count, block_locator_hashes, hash_stop
                    ),
                )
                await self.sync_batched_blocks()

            self._all_blocks_synced_event.set()
            self.logger.debug(
                "blocks synced. new local block height is: %s",
                self.get_local_block_tip_height(),
            )
        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger.exception("sync_blocks_job raised an exception", e)
            raise

    # -- Message Types -- #
    async def send_version(
        self, recv_host=None, recv_port=None, send_host=None, send_port=None
    ):
        message = self.serializer.version(
            recv_host=recv_host,
            recv_port=recv_port,
            send_host=send_host,
            send_port=send_port,
        )
        await self.send_request(VERSION, message)

    async def send_inv(self, inv_vects: List[Dict]):
        await self.serializer.inv(inv_vects)
