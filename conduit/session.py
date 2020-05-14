import asyncio
from asyncio import BufferedProtocol
import bitcoinx
from bitcoinx import hex_str_to_hash, MissingHeader
from collections import namedtuple
from functools import partial
import io
import logging
import math
import struct
from typing import Optional, List, Dict, Callable, Union

from .logs import logs
from . import utils
from .commands import VERSION, GETHEADERS, GETBLOCKS, GETDATA
from .handlers import Handlers
from .constants import HEADER_LENGTH, ZERO_HASH, BLOCK_HEADER_LENGTH
from .deserializer import Deserializer
from .networks import NetworkConfig
from .peers import Peer
from .serializer import Serializer
from .utils import is_block_msg

Header = namedtuple("Header", "magic command payload_size checksum")


logger = logs.get_logger("session")


class BitcoinFramer(BufferedProtocol):
    logger = logging.getLogger("bitcoin-framer")

    def _new_buffer(self, size):
        self.buffer = bytearray(size)
        self.buffer_view = memoryview(self.buffer)
        self.pos = 0

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
                    self._new_buffer(HEADER_LENGTH)  # header size
                    self._payload_size = None
                else:
                    self._new_buffer(self._payload_size)

        # get payload
        else:
            self.current_command = self.msg_header.command.rstrip(b"\0")

            # full payload in buffer
            if self.pos == self._payload_size:
                # blocks never pass through "message_received" - handled as a special case

                if is_block_msg(self.current_command):
                    if self.pos == self._payload_size:
                        self.transport.pause_reading()
                    self.allocate_block_fragment()
                    return

                message = self.buffer
                self.message_received(self.current_command, message)
                self._new_buffer(HEADER_LENGTH)  # header size
                self._payload_size = None


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
        self.logger.warning("The server closed the connection")

    def run_coro_threadsafe(self, coro, *args, **kwargs):
        asyncio.run_coroutine_threadsafe(coro(*args, **kwargs), self.loop)

    def message_received(self, command: bytes, message: bytes):
        self.run_coro_threadsafe(self.handle, command, message)

    async def handle(self, command, message=None):
        logger = logging.getLogger("handle")
        try:
            # ignore mempool activity until headers sync'd
            if command == b"inv" and not self._all_headers_synced_event.is_set():
                return
            handler_func_name = "on_" + command.decode("ascii")
            handler_func = getattr(self.handlers, handler_func_name)
            await handler_func(message)
        except Exception as e:
            logger.exception(e)
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
                _sync_blocks_task = asyncio.create_task(self.sync_blocks_job())
                await self._all_blocks_synced_event.wait()
        except Exception as e:
            self.logger.exception(e)
            raise

    async def sync_headers_job(self):
        """supervises completion of syncing all headers to target height"""

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
        try:
            while self.get_local_block_tip_height() < self.target_block_height:
                block_locator_hashes = [
                    self.storage.block_headers.longest_chain().tip.hash
                ]
                hash_count = len(block_locator_hashes)
                self.logger.debug(
                    "requesting max number of blocks (up to 500) for chain tip "
                    "height=%s",
                    self.get_local_block_tip_height(),
                )
                await self.send_request(
                    GETBLOCKS,
                    self.serializer.getblocks(
                        hash_count, block_locator_hashes, ZERO_HASH
                    ),
                )
                while True:
                    # one loop per block retrieval and parsing
                    inv = await self._pending_blocks_queue.get()
                    try:
                        header, chain = self.storage.headers.lookup(
                            hex_str_to_hash(inv.get("inv_hash"))
                        )
                        logger.debug(
                            f"got inv from queue for block height={header.height}"
                        )
                    except MissingHeader as e:
                        logger.warning(
                            "could not find header with hash=%s", inv.get("inv_hash")
                        )
                        if self._pending_blocks_queue.empty():
                            break
                        else:
                            continue

                    def is_at_or_below_block_headers_tip(header) -> bool:
                        block_headers_tip = self.get_local_block_tip_height()
                        if header.height <= block_headers_tip + 1:
                            return True
                        else:
                            self.logger.debug(
                                f"ignoring block with height={header.height} until "
                                f"initial block download completed"
                            )
                            return False

                    if not is_at_or_below_block_headers_tip(header):
                        continue

                    await self.send_request(
                        GETDATA, self.serializer.getdata([inv]),
                    )
                    await self.wait_for_block_parsing()
                    self.logger.debug(
                        f"_pending_blocks_queue size="
                        f"{self._pending_blocks_queue.qsize()}"
                    )
                    if self._pending_blocks_queue.empty():
                        break

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
            parse_block_part = partial(
                self.parse_block_partition,
                worker_id,
                worker_count,
                self.buffer_view,
                start_pos,
                stop_pos,
            )
            self.workers_map.update({worker_id: parse_block_part})

    def ensure_workers_started(self, from_worker_id, to_worker_id):
        for i in range(from_worker_id, to_worker_id + 1):
            try:
                parse_block_part = self.workers_map.pop(i)

                # Todo this function is the target for multiprocessing
                #  but at present it will just run on the same core (on the event loop!)
                parse_block_part()
            except KeyError:
                # work already allocated to this worker
                pass

    async def allocate_work(
        self, buffer_view: memoryview, pos: int, payload_size: int, current_command: str
    ):
        """purely for parallel block processing using the multiprocessing module
        - this function will be called multiple times for a large block as the buffer is
        filled."""
        # NOTE: at present deferring multiprocessing implementation
        from_worker_id = None
        to_worker_id = None
        if len(self.workers_map) == 0:  # first receipt of block fragment
            self.init_workers_map(payload_size, self.WORKER_COUNT)

        ready_partitions = math.floor(self.pos / self.block_partition_size)
        if ready_partitions > 0:
            from_worker_id = 1
            to_worker_id = ready_partitions

        if ready_partitions == 0:
            self.logger.debug("no work can be allocated yet. need to 'recv' more block")
        else:
            self.ensure_workers_started(from_worker_id, to_worker_id)

        if self.pos == payload_size:
            await self._block_parsed_event.wait()
            self.transport.resume_reading()
            # await self._merkle_tree_done_event.wait()
            # await self._block_committed_event.wait()
            # signal to aiorpcx-based API server (not built yet) mew chain tip fit for
            # consumption -> pub-subs etc.

            self._new_buffer(HEADER_LENGTH)
            self._payload_size = None
            self.workers_map = {}

    def parse_block_partition(
        self,
        worker_id: int,
        worker_count: int,
        buffer_view: Union[memoryview, bytes],
        start_pos: int,
        stop_pos: int,
    ):
        """Where all the cpu intensive work gets done (ideally across many cores)

        NOTE: The workers can overrun their stop_pos to complete *their last tx*
        and similarly the start_pos is only indicative - the worker needs to run an
        algorithm to finding it's "initial binding location" (not implemented)"""
        try:
            header_hash = bitcoinx.double_sha256(buffer_view[0:80].tobytes())
            current_block_header, chain = self.storage.headers.lookup(header_hash)

            if worker_id == 1:
                tx_count, varint_size = utils.unpack_varint_from_mv(buffer_view[80:])
                self.logger.debug("current_block_header = %s", current_block_header)
                self.logger.info("block size=%s bytes", self._payload_size)

                # updates local_block_tip
                self.deserializer.connect_block_header(buffer_view[0:80].tobytes())

                first_tx_pos = BLOCK_HEADER_LENGTH + varint_size
                len_stream = stop_pos - first_tx_pos
                stream: io.BytesIO = io.BytesIO(buffer_view[first_tx_pos:stop_pos])

                txs = []
                while stream.tell() < len_stream:
                    tx = bitcoinx.Tx.read(stream.read)
                    txn_tuple = (tx.hash(), current_block_header.height, tx.to_bytes())
                    txs.append(txn_tuple)

                with self.storage.pg_database.atomic():
                    self.storage.insert_many_txs(txs)
            else:
                # these workers have the tough job of finding a "binding location"
                stream: io.BytesIO = io.BytesIO(buffer_view[start_pos:stop_pos])
                raise NotImplementedError

            self._work_done_queue.put_nowait(worker_id)
        except Exception as e:
            self.logger.exception(e)
