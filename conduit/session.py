import asyncio
import io
import multiprocessing
import threading
from asyncio import BufferedProtocol
from concurrent.futures.thread import ThreadPoolExecutor
import bitcoinx
from bitcoinx import hex_str_to_hash, MissingHeader, Headers, read_varint
from collections import namedtuple
from multiprocessing import shared_memory
import logging
import struct
from typing import Optional, List, Dict

from .database import PG_Database, load_pg_database
from .workers import BlockPreProcessor, TxParser, MTreeCalculator, BlockWriter
from .commands import (
    VERSION,
    GETHEADERS,
    GETBLOCKS,
    GETDATA,
    BLOCK_BIN,
)
from .handlers import Handlers
from .constants import (
    HEADER_LENGTH,
    ZERO_HASH,
    WORKER_COUNT_PREPROCESSORS,
    WORKER_COUNT_TX_PARSERS,
    WORKER_COUNT_MTREE_CALCULATORS,
    WORKER_COUNT_BLK_WRITER,
)
from .deserializer import Deserializer
from .networks import NetworkConfig
from .peers import Peer
from .serializer import Serializer

Header = namedtuple("Header", "magic command payload_size checksum")


class BitcoinFramer(BufferedProtocol):
    logger = logging.getLogger("bitcoin-framer")
    HIGH_WATER = 1024 * 1024 * 128
    BUFFER_OVERFLOW_SIZE = 1024 * 1024 * 4
    BUFFER_SIZE = HIGH_WATER + BUFFER_OVERFLOW_SIZE
    shm_buffer = shared_memory.SharedMemory(create=True, size=BUFFER_SIZE)
    shm_buffer_view = shm_buffer.buf
    _pos = 0
    _last_msg_end_pos = 0
    _msg_received_count = 0
    _msg_handled_count = 0

    def _new_buffer(self):
        assert self._pos >= self.HIGH_WATER
        # take the remainder of buffer and place it at the start then extend to size
        remainder = self.shm_buffer_view[self._last_msg_end_pos :].tobytes()
        self.shm_buffer_view[0 : len(remainder)] = remainder

        # For debugging only in development
        fresh_buf_len = self.BUFFER_SIZE - len(remainder)
        self.shm_buffer_view[len(remainder) : self.BUFFER_SIZE] = b"x" * fresh_buf_len

        # new offset for most recent, partially read msg
        self._pos = self._pos - self._last_msg_end_pos
        self._last_msg_end_pos = 0

    def get_buffer(self, sizehint):
        return self.shm_buffer_view[self._pos :]

    def _unpack_msg_header(self) -> Header:
        cur_header_view = self.shm_buffer_view[
            self._last_msg_end_pos : self._last_msg_end_pos + HEADER_LENGTH
        ]
        return Header(*struct.unpack_from("<4s12sI4s", cur_header_view, offset=0))

    def is_next_header_available(self) -> bool:
        return self._pos - self._last_msg_end_pos >= HEADER_LENGTH

    def is_next_payload_available(self) -> bool:
        return self._pos - self._last_msg_end_pos >= HEADER_LENGTH + self._payload_size

    def get_block_tx_count(self, end_blk_header_pos):
        # largest varint (tx_count) is 9 bytes
        block_tx_count_view = self.shm_buffer_view[
            end_blk_header_pos : end_blk_header_pos + 10
        ]
        return read_varint(io.BytesIO(block_tx_count_view).read)

    def make_special_blk_msg(self, cur_msg_start_pos, cur_msg_end_pos):
        end_blk_header_pos = cur_msg_start_pos + 80
        raw_block_header = self.shm_buffer_view[
            cur_msg_start_pos:end_blk_header_pos
        ].tobytes()
        block_tx_count = self.get_block_tx_count(end_blk_header_pos)
        return (
            cur_msg_start_pos,
            cur_msg_end_pos,
            raw_block_header,
            block_tx_count,
        )

    def buffer_updated(self, nbytes):
        self._pos += nbytes

        while self.is_next_header_available():
            cur_header = self._unpack_msg_header()
            self._payload_size = cur_header.payload_size
            if self.is_next_payload_available():
                cur_msg_end_pos = (
                    self._last_msg_end_pos + HEADER_LENGTH + self._payload_size
                )
                cur_msg_start_pos = self._last_msg_end_pos + HEADER_LENGTH

                # Block messages
                if cur_header.command == BLOCK_BIN:
                    self.message_received(
                        cur_header.command,
                        self.make_special_blk_msg(cur_msg_start_pos, cur_msg_end_pos),
                    )

                # Non-block messages
                else:
                    sub_view_payload = self.shm_buffer_view[
                        cur_msg_start_pos:cur_msg_end_pos
                    ]
                    self.message_received(cur_header.command, sub_view_payload)
                self._last_msg_end_pos = cur_msg_end_pos
            else:
                break  # recv more data until next full payload available

        if self._pos >= self.HIGH_WATER:
            self.logger.debug("buffer reached high-water mark, pausing reading...")
            self.transport.pause_reading()
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

    def __init__(self, config: NetworkConfig, peer: Peer, host, port, storage):
        self.logger = logging.getLogger("session")
        self.loop = asyncio.get_event_loop()
        self.storage = storage

        # Bitcoin network/peer config
        self.net_config = config
        self.peer = peer
        self.host = host
        self.port = port
        self.protocol: BufferedProtocol = None
        self.protocol_factory = None

        # Serialization/Deserialization
        self.handlers = Handlers(self, self.net_config, self.storage)
        self.serializer = Serializer(self.net_config, self.storage)
        self.deserializer = Deserializer(self.net_config, self.storage)

        # Connection entry/exit
        self.handshake_complete_event = asyncio.Event()
        self.con_lost_event = asyncio.Event()

        # Syncing of chain - two bitcoinx.Headers stores (headers and block_headers)
        self._headers_msg_processed_event = asyncio.Event()
        self._all_headers_synced_event = asyncio.Event()
        self._all_blocks_synced_event = asyncio.Event()
        self._target_header_height: Optional[int] = None
        self._target_block_header_height: Optional[int] = None
        self._local_tip_height: int = self.update_local_tip_height()
        self._local_block_tip_height: int = self.get_local_block_tip_height()

        # Accounting and ack'ing for non-block msgs
        self._incoming_msg_queue = asyncio.Queue()
        self._msg_received_count = 0
        self._msg_handled_count = 0
        self._msg_received_count_lock = threading.Lock()
        self._msg_handled_count_lock = threading.Lock()

        # Accounting and ack'ing for block msgs
        self._pending_blocks_batch_size = 0
        self._pending_blocks_received = {}  # blk_hash: total_tx_count
        self._pending_blocks_inv_queue = asyncio.Queue()
        self._pending_blocks_batch_set = set()  # usually a set of 500 hashes
        self._pending_blocks_progress_counter = {}
        self._batched_blocks_exec = ThreadPoolExecutor(1, "join-batched-blocks")

        # Worker queues
        self.worker_in_queue_preproc = multiprocessing.Queue()  # no ack needed
        self.worker_in_queue_tx_parse = multiprocessing.Queue()
        self.worker_in_queue_mtree = multiprocessing.Queue()
        self.worker_in_queue_blk_writer = multiprocessing.Queue()
        self.worker_ack_queue_tx_parse = multiprocessing.Queue()  # blk_hash:tx_count
        self.worker_ack_queue_mtree = multiprocessing.Queue()
        self.worker_ack_queue_blk_writer = multiprocessing.Queue()

        self.pg_db: Optional[PG_Database] = None

    def run_coro_threadsafe(self, coro, *args, **kwargs):
        asyncio.run_coroutine_threadsafe(coro(*args, **kwargs), self.loop)

    def connection_made(self, transport):
        self.transport = transport
        self._payload_size = None
        self.logger.info("connection made")

        _fut = asyncio.create_task(self.handle())
        _fut = asyncio.create_task(self.start_jobs())

    def connection_lost(self, exc):
        self.con_lost_event.set()
        self.logger.warning("The server closed the connection")

    def message_received(self, command: bytes, message: memoryview):
        # self.logger.debug("received: %s message", command.rstrip(b"\0"))
        if command != BLOCK_BIN:
            self.incr_msg_received_count()
        self._incoming_msg_queue.put_nowait((command, message))

    def have_processed_received_blocks(self) -> bool:
        # Todo - cover MTree and BlockWriter workers too
        expected_blocks_processed_count = self._pending_blocks_batch_size - len(
            self._pending_blocks_batch_set
        )
        for blk_hash, count in self._pending_blocks_progress_counter.items():
            if not self._pending_blocks_received[blk_hash] == count:
                return False  # not all txs in block ack'd

        if expected_blocks_processed_count != len(self.done_block_heights):
            return False

        return True

    def have_processed_received_non_block_msgs(self) -> bool:
        return self._msg_received_count == self._msg_handled_count

    async def wait_for_go_signal(self):
        """spawned when buffer goes above HWM level. will then depend on the accounting
        system to ensure all messages have been processed before resetting the buffer."""

        while not (
            self.have_processed_received_non_block_msgs()
            and self.have_processed_received_blocks()
        ):
            await asyncio.sleep(0.05)

        self.logger.debug("resuming reading...")
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

    def add_pending_block(self, blk_hash, total_tx_count):
        self._pending_blocks_received[blk_hash] = total_tx_count
        self._pending_blocks_progress_counter[blk_hash] = 0

    def reset_pending_blocks(self):
        self._pending_blocks_received = {}
        self._pending_blocks_progress_counter = {}

    async def handle(self):
        while True:
            command, message = await self._incoming_msg_queue.get()
            try:
                # self.logger.debug("command=%s", command.rstrip(b'\0').decode('ascii'))
                handler_func_name = "on_" + command.rstrip(b"\0").decode("ascii")
                handler_func = getattr(self.handlers, handler_func_name)
                await handler_func(message)
                if command != BLOCK_BIN:
                    self.incr_msg_handled_count()
            except Exception as e:
                self.logger.exception("handle: ", e)
                raise

    def is_synchronized(self):
        return self.get_local_block_tip_height() == self._target_block_header_height

    def send_message(self, message):
        self.transport.write(message)

    async def send_request(self, command_name: str, message: bytes):
        self.send_message(message)

    def get_local_tip_height(self):
        return self._local_tip_height

    def get_local_block_tip_height(self) -> int:
        return self.storage.block_headers.longest_chain().tip.height

    def update_local_tip_height(self) -> int:
        self._local_tip_height = self.storage.headers.longest_chain().tip.height
        return self._local_tip_height

    def set_target_header_height(self, height) -> None:
        self._target_header_height = height

    def start_workers(self):
        self.processes = []
        for i in range(WORKER_COUNT_PREPROCESSORS):
            p = BlockPreProcessor(
                self.shm_buffer.name,
                self.worker_in_queue_preproc,
                self.worker_in_queue_tx_parse,
                self.worker_in_queue_mtree,
            )
            p.start()
            self.processes.append(p)
        for i in range(WORKER_COUNT_TX_PARSERS):
            p = TxParser(
                self.shm_buffer.name,
                self.worker_in_queue_tx_parse,
                self.worker_ack_queue_tx_parse,
            )
            p.start()
            self.processes.append(p)
        for i in range(WORKER_COUNT_MTREE_CALCULATORS):
            p = MTreeCalculator(
                self.shm_buffer.name,
                self.worker_in_queue_mtree,
                self.worker_ack_queue_mtree,
            )
            p.start()
            self.processes.append(p)
        for i in range(WORKER_COUNT_BLK_WRITER):
            p = BlockWriter(
                self.shm_buffer.name,
                self.worker_in_queue_blk_writer,
                self.worker_ack_queue_blk_writer,
            )
            p.start()
            self.processes.append(p)

    async def start_jobs(self):
        try:
            self.pg_db: PG_Database = await load_pg_database()

            await self.handshake_complete_event.wait()
            self.start_workers()

            _sync_headers_task = asyncio.create_task(self.sync_headers_job())
            if self.get_local_tip_height() != self._target_header_height:
                await self._all_headers_synced_event.wait()

            self._target_block_header_height = (
                self.get_local_tip_height()
            )  # blocks lag headers
            if self.get_local_block_tip_height() != self._target_block_header_height:
                _sync_blocks_task = asyncio.create_task(self.sync_all_blocks_job())
                await self._all_blocks_synced_event.wait()
        except Exception as e:
            self.logger.exception(e)
            raise

    async def sync_headers_job(self):
        """supervises completion of syncing all headers to target height"""
        self.logger.debug("starting sync_headers_job...")
        while self._local_tip_height < self._target_header_height:
            block_locator_hashes = [self.storage.headers.longest_chain().tip.hash]
            hash_count = len(block_locator_hashes)
            await self.send_request(
                GETHEADERS,
                self.serializer.getheaders(hash_count, block_locator_hashes, ZERO_HASH),
            )
            await self._headers_msg_processed_event.wait()
            self._local_tip_height = self.update_local_tip_height()
            self.logger.debug(
                "headers message processed - new chain tip height: %s",
                self._local_tip_height,
            )
            self._headers_msg_processed_event.clear()

        self._all_headers_synced_event.set()
        self.logger.debug("headers synced. chain tip height=%s", self._local_tip_height)

    def get_header_for_hash(self, block_hash: bytes):
        header, chain = self.storage.headers.lookup(block_hash)
        return header

    def get_header_for_height(self, height: int) -> bitcoinx.Header:
        headers = self.storage.headers
        chain = self.storage.headers.longest_chain()
        header = headers.header_at_height(chain, height)
        return header

    def join_batched_blocks(self):
        """Runs in a thread so it can block on the multiprocessing queue"""
        # join block parser workers
        self.logger.debug("waiting for block parsers to complete work...")
        self.done_block_heights = []

        while True:
            try:
                # This queue should transition to a multiprocessing queue
                block_hash, txs_done_count = self.worker_ack_queue_tx_parse.get()
                self._pending_blocks_progress_counter[block_hash] += txs_done_count

                header = self.get_header_for_hash(block_hash)
                self.logger.debug(f"block height={header.height} done!")

                self.done_block_heights.append(header.height)

                try:
                    self._pending_blocks_batch_set.remove(block_hash)
                except KeyError:
                    header = self.get_header_for_hash(block_hash)
                    self.logger.debug(f"also parsed block: {header.height}")

                # all blocks in batch parsed
                if len(self._pending_blocks_batch_set) == 0:
                    # in a crash prior to this point, the last 500 blocks will just get
                    # re-done on start-up because the block_headers.mmap file has not
                    # been updated with any headers.

                    for height in sorted(self.done_block_heights):
                        self.logger.debug(f"connecting height: {height}")
                        header = self.get_header_for_height(height)
                        block_headers: bitcoinx.Headers = self.storage.block_headers
                        block_headers.connect(header.raw)
                    self.reset_pending_blocks()
                    return
            except Exception as e:
                self.logger.exception(e)

    async def sync_batched_blocks(self) -> None:
        # one block per loop
        pending_getdata_requests = list(self._pending_blocks_batch_set)
        count_requested = 0
        while True:
            inv = await self._pending_blocks_inv_queue.get()
            # self.logger.debug(f"got inv: {inv}")
            try:
                header = self.get_header_for_hash(hex_str_to_hash(inv.get("inv_hash")))
            except MissingHeader as e:
                self.logger.warning(
                    "header with hash=%s not found", inv.get("inv_hash")
                )
                if self._pending_blocks_inv_queue.empty():
                    break
                else:
                    continue

            if not header.height <= self.stop_header_height:
                self.logger.debug(f"ignoring block height={header.height} until sync'd")
                self.logger.debug(
                    f"len(pending_getdata_requests)={len(pending_getdata_requests)}"
                )
                if len(pending_getdata_requests) == 0:
                    break
                else:
                    continue

            await self.send_request(
                GETDATA, self.serializer.getdata([inv]),
            )
            count_requested += 1
            pending_getdata_requests.pop()
            if len(pending_getdata_requests) == 0:
                break

        # detect when an unsolicited new block has thrown a spanner in the works
        if not count_requested == 0:
            await asyncio.get_running_loop().run_in_executor(
                self._batched_blocks_exec, self.join_batched_blocks
            )
        else:
            # it was just a 'rogue' unsolicited block so still need to wait for
            # the 500 odd requested blocks...
            await self.sync_batched_blocks()

    def reset_pending_blocks_batch_set(self, hash_stop):
        self._pending_blocks_batch_set = set()
        headers: Headers = self.storage.headers
        chain = self.storage.headers.longest_chain()

        local_tip_height = self.get_local_block_tip_height()
        if hash_stop == ZERO_HASH:
            self.stop_header_height = local_tip_height + 500
        else:
            self.stop_header_height = headers.lookup(hash_stop).height

        self._pending_blocks_batch_size = self.stop_header_height - local_tip_height
        for i in range(1, self._pending_blocks_batch_size + 1):
            block_header = headers.header_at_height(chain, local_tip_height + i)
            self._pending_blocks_batch_set.add(block_header.hash)

    async def sync_all_blocks_job(self):
        """supervises completion of syncing all blocks to target height"""
        try:
            # up to 500 blocks per loop
            while self.get_local_block_tip_height() < self._target_block_header_height:
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
