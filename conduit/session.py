import io
import json
import math
from asyncio import BufferedProtocol
from collections import namedtuple
from functools import partial
import queue
from typing import Optional, List, Dict, Callable, Union

import bitcoinx

import utils
from commands import VERSION, GETHEADERS, GETBLOCKS, GETDATA
from handlers import Handlers
import logging
import struct
import asyncio

from constants import LOGGING_FORMAT, HEADER_LENGTH, ZERO_HASH
from deserializer import Deserializer
from networks import NetworkConfig
from peers import Peer
from serializer import Serializer
from utils import is_block_msg

Header = namedtuple("Header", "magic command payload_size checksum")

logging.basicConfig(
    format=LOGGING_FORMAT, level=logging.DEBUG, datefmt="%Y-%m-%d %H-%M-%S"
)


class BitcoinFramer(BufferedProtocol):
    def _new_buffer(self, size):
        # not sure if new buf is necessary each msg. can maybe re-use if pos maintained
        self.buffer = bytearray(size)
        # can swap to shared_memory for multi-cpu reads
        self.buffer_view = memoryview(self.buffer)
        self.pos = 0

    def reset_buffer(self):
        """if the buffer is not reset (with self.pos = 0) then the other messages
        wait in the underlying transport"""
        self._new_buffer(HEADER_LENGTH)  # header size
        self._payload_size = None

    def get_buffer(self, sizehint):
        return self.buffer_view[self.pos :]

    def _unpack_msg_header(self) -> Header:
        return Header(*struct.unpack_from("<4s12sI4s", self.buffer_view, offset=0))

    def allocate_block_fragment(self):
        loop = asyncio.get_running_loop()
        coro = self.allocate_work(
            self.buffer_view, self.pos, self._payload_size, self.current_command
        )
        _fut = asyncio.run_coroutine_threadsafe(coro, loop)

    def buffer_updated(self, nbytes):
        self.pos += nbytes

        # get msg header
        if self._payload_size is None:
            if self.pos == HEADER_LENGTH:
                self.msg_header = self._unpack_msg_header()
                self._payload_size = self.msg_header.payload_size
                if self._payload_size == 0:  # e.g. ping/version etc
                    self.reset_buffer()
                else:
                    self._new_buffer(self._payload_size)

        # get payload
        else:
            self.current_command = self.msg_header.command.rstrip(b"\0")

            # blocks never pass through "message_received" - handled as a special case
            # in theory can start work early on memory view of the block
            if is_block_msg(self.current_command):
                self.allocate_block_fragment()
                if self.pos == self._payload_size:
                    return  # buffer is reset when block is parsed and committed

            # full payload in buffer
            if self.pos == self._payload_size:
                message = self.buffer
                self.message_received(self.current_command, message)
                self.reset_buffer()


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

        # Framer
        self.buffer: Optional[bytearray] = None
        self.buffer_view: Optional[memoryview] = None  # will change to shared_memory
        self.pos: int = 0

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

        # Events
        self.handshake_complete_event = asyncio.Event()
        self.con_lost_event = (
            asyncio.Event()
        )  # waited on by session manager at higher LOA

        # - Syncing of chain
        self._all_headers_synced_event = asyncio.Event()
        self._all_blocks_synced_event = asyncio.Event()  # -> listen for inv type 2
        self.target_header_height: Optional[int] = None
        self.target_block_height: Optional[int] = None
        self.local_tip_height: int = self.get_local_tip_height()
        self.local_block_tip_height: int = self.get_local_block_tip_height()
        self._pending_blocks_queue = asyncio.Queue()

        self._headers_msg_processed_event = asyncio.Event()

        # - Parallel block processing (events in sequential order)
        self._block_parsed_event = asyncio.Event()
        self._merkle_tree_done_event = asyncio.Event()
        self._block_committed_event = asyncio.Event()

        self._work_done_queue = asyncio.Queue()

        self.block_partition_size: Optional[int] = None
        self.workers_map: Dict[int:Callable] = {}

    def connection_made(self, transport):
        self.transport = transport
        self._payload_size = None
        self._new_buffer(HEADER_LENGTH)  # bitcoin message header size
        self.logger.info("connection made")

        _fut = asyncio.create_task(self.manage_jobs())

    def connection_lost(self, exc):
        self.con_lost_event.set()
        self.logger.info("The server closed the connection:", exc)

    def run_coro_threadsafe(self, coro, *args, **kwargs):
        asyncio.run_coroutine_threadsafe(coro(*args, **kwargs), self.loop)

    def message_received(self, command: bytes, message: bytes):
        self.logger.debug("received msg type: %s", command.decode("ascii"))
        self.run_coro_threadsafe(self.handle, command, message)

    async def handle(self, command, message=None):
        logger = logging.getLogger("handle")
        try:
            handler_func_name = "on_" + command.decode("ascii")
            handler_func = getattr(self.handlers, handler_func_name)
            await handler_func(message)
        except Exception as e:
            logger.exception(e)
            raise

    def send_message(self, message):
        self.transport.write(message)

    async def send_request(self, command_name: str, message: bytes):
        self.logger.debug(f"sending to {self.peer}, msg type: {command_name}")
        self.send_message(message)

    def get_local_tip_height(self):
        return self.storage.headers.longest_chain().tip.height

    def get_local_block_tip_height(self):
        # Todo - find out from database
        return 0

    def update_local_tip_height(self) -> int:
        self.local_tip_height = self.storage.headers.longest_chain().tip.height
        return self.local_tip_height

    def set_target_header_height(self, height) -> None:
        self.target_header_height = height

    async def manage_jobs(self):
        _sync_headers_task = asyncio.create_task(self.sync_headers_job())
        await self._all_headers_synced_event.wait()
        self._all_headers_synced_event.clear()

        self.target_block_height = self.get_local_tip_height()  # blocks lag headers
        _sync_blocks_task = asyncio.create_task(self.sync_blocks_job())
        await self._all_blocks_synced_event.wait()

    async def sync_headers_job(self):
        """supervises completion of syncing all headers to target height"""

        await self.handshake_complete_event.wait()

        while self.local_tip_height < self.target_header_height:
            block_locator_hashes = [self.storage.headers.longest_chain().tip.hash]
            hash_count = len(block_locator_hashes)
            await self.send_request(
                GETHEADERS,
                self.serializer.getheaders(hash_count, block_locator_hashes, ZERO_HASH),
            )
            await self._headers_msg_processed_event.wait()
            self._headers_msg_processed_event.clear()
            self.storage.headers.flush()
            self.local_tip_height = self.update_local_tip_height()
            self.logger.debug(
                "headers message processed - new chain tip height: %s, " "hash: %s",
                self.local_tip_height,
                self.storage.headers.longest_chain().tip.hash,
            )

        self._all_headers_synced_event.set()
        self.logger.debug(
            "headers synced. new chain tip height is: %s", self.local_tip_height
        )

    async def wait_for_block_parsing(self):
        """wait for a *single" block to complete the parsing stage"""
        for i in range(self.WORKER_COUNT):
            # needs changed to a multiprocessing queue type later...
            _worker_id = await self._work_done_queue.get()
            self.logger.debug(f"worker: {_worker_id} done!")
        self._block_parsed_event.set()

    async def sync_blocks_job(self):
        """supervises completion of syncing all blocks to target height"""

        while self.local_block_tip_height < self.target_block_height:
            chain = self.storage.headers.longest_chain()
            block_locator_hashes = [
                self.storage.headers.header_at_height(chain, self.target_block_height)
            ]
            hash_count = len(block_locator_hashes)
            await self.send_request(
                GETBLOCKS,
                self.serializer.getblocks(hash_count, block_locator_hashes, ZERO_HASH),
            )  # responds via inv with up to 500 blocks

            # NOTE: these inv packets will be handled by the standard 'handlers.py'
            # MSG_BLOCK type == 2 -> fills the _pending_blocks_queue
            inv_vects = await self._pending_blocks_queue.get()

            # NOW -> requests the blocks via GETDATA messages
            await self.send_request(
                GETDATA,
                self.serializer.getdata(inv_vects=inv_vects),
            )  # responds via inv with up to 500 blocks

            await self.wait_for_block_parsing()

        self._all_blocks_synced_event.set()
        self.logger.debug(
            "blocks synced. new local block height is: %s", self.local_tip_height
        )

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

    def init_workers_map(self, payload_size, worker_count):
        part_size = math.floor(payload_size / worker_count)
        self.block_partition_size = part_size

        for i in range(1, worker_count + 1):
            if i != worker_count:  # last part just takes the remainder
                start_pos = (i - 1) * part_size
                stop_pos = i * part_size
            else:
                start_pos = (i - 1) * part_size
                stop_pos = payload_size

            worker_id = i
            func = partial(
                self.parse_block_partition,
                worker_id,
                worker_count,
                self.buffer_view,
                start_pos,
                stop_pos,
            )
            self.workers_map.update({worker_id: func})

    def ensure_workers_started(self, from_worker_id, to_worker_id):
        for i in range(from_worker_id, to_worker_id + 1):
            try:
                func = self.workers_map.pop(i)

                # Todo this function is the target for multiprocessing
                #  but at present it will just run on the same core
                func()  # this cpu-intensive work will block the event loop until fixed
            except KeyError:
                # work already allocated to this worker
                pass

    async def allocate_work(
        self, buffer_view: memoryview, pos: int, payload_size: int, current_command: str
    ):
        """purely for parallel block processing using the multiprocessing module
        - this function will be called multiple times for a large block as the buffer is
        filled."""
        # NOTE1: at present deferring multiprocessing implementation
        # NOTE2: also deferring "early" processing of the block on the memory
        # view before the full block is in the buffer. Instead just waits until the
        # whole block is in the buffer for a simple, first implementation.
        from_worker_id = None
        to_worker_id = None
        if len(self.workers_map) == 0:  # first receipt of block fragment
            self.init_workers_map(payload_size, self.WORKER_COUNT)
            self.logger.debug(f"received message type: {current_command}")
            self.logger.debug(
                f"payload size={payload_size}, current pos={pos}, "
                f"workers={self.WORKER_COUNT}"
            )
            self.logger.debug(f"block_partition_size={self.block_partition_size} bytes")

        ready_partitions = math.floor(self.pos / self.block_partition_size)
        self.logger.debug(f"ready_partitions count={ready_partitions}")
        if ready_partitions > 0:
            from_worker_id = 1
            to_worker_id = ready_partitions

        if ready_partitions == 0:
            self.logger.debug("no work can be allocated yet. need to 'recv' more block")
        else:
            self.logger.debug(
                f"therefore allocating work from worker id={from_worker_id} to worker "
                f"id={to_worker_id}"
            )
            self.ensure_workers_started(from_worker_id, to_worker_id)

        if self.pos == payload_size:
            await self._block_parsed_event.wait()
            self.logger.debug("block parsing complete")
            # Todo - trigger workers to calculate merkle tree

            # await self._merkle_tree_done_event.wait()
            # self.logger.debug("merkle tree calculation complete")
            # # Todo - trigger commits to database
            #
            # await self._block_committed_event.wait()
            # self.logger.debug("block txs and merkle tree committed to database")
            # # Todo - trigger the aiorpcx-based API server (not built yet) to update
            # #  to the new chain tip so that wallets can get tx confirmations etc.

            self.reset_buffer()  # and get next blocks/messages
            self.workers_map = {}

    def parse_block_partition(
        self,
        worker_id: int,
        worker_count: int,
        buffer_view: Union[memoryview, bytes],
        start_pos: int,
        stop_pos: int,
    ):
        """Where all the cpu intensive work gets done (ideally across many cores)"""
        # NOTE: The workers can overrun their stop_pos to complete their last tx
        #  and similarly the start_pos is only indicative - the worker needs to
        #  run an algorithm to finding it's "initial binding location" (not implemented)
        self.logger.debug(
            f"called with arguments: worker_id={worker_id}, worker_count={worker_count}, "
            f"buffer_view={buffer_view}, start_pos={start_pos}, stop_pos={stop_pos}"
        )

        if worker_id == 1:
            header_hash = bitcoinx.double_sha256(buffer_view[0:80].tobytes())
            current_block_header = self.storage.headers.lookup(header_hash)

            tx_count, varint_size = utils.unpack_varint_from_mv(buffer_view[80:])
            self.logger.debug("current_block_header = %s", current_block_header)

            first_tx_pos = HEADER_LENGTH + varint_size + start_pos
            stream: io.BytesIO = io.BytesIO(buffer_view[first_tx_pos:stop_pos])
        else:
            stream: io.BytesIO = io.BytesIO(buffer_view[start_pos:stop_pos])

        txs = []

        # Todo - add logic for finding "binding location" so that can increase from
        #  1 worker to many workers
        stream.seek(start_pos)
        while stream.tell() >= stop_pos:
            txs.append(bitcoinx.Tx.read(stream.read))

        with open("block1.json", "w") as f:
            f.write(json.dumps(txs))

        self._work_done_queue.put_nowait(worker_id)
