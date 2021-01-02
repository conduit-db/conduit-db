import asyncio
import multiprocessing
from asyncio import BufferedProtocol
import bitcoinx
from bitcoinx import hex_str_to_hash, MissingHeader
import logging
from typing import Optional, List, Dict

from .sync_state import SyncState
from .bitcoin_net_io import BitcoinNetIO
from .database.postgres_database import PG_Database
from .workers.preprocessor import BlockPreProcessor
from .workers.transaction_parser import TxParser
from .workers.merkle_tree import MTreeCalculator
from .workers.raw_blocks import BlockWriter
from .store import setup_storage
from .commands import VERSION, GETHEADERS, GETBLOCKS, GETDATA, BLOCK_BIN, MEMPOOL
from .handlers import Handlers
from .constants import (
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


class Controller:
    """Designed to sync the blockchain as fast as possible.

    Coordinates:
    - network via BitcoinNetIO
    - outsources parsing of block data to workers in workers.py (which work in parallel reading
    from a shared memory buffer)
    - synchronizes the refreshing of the shared memory buffer which holds multiple raw blocks)
    """

    def __init__(self, config: Dict, net_config: NetworkConfig, host="127.0.0.1", port=8000):
        self.logger = logging.getLogger("controller")
        self.loop = asyncio.get_event_loop()

        self.config = config  # cli args & env_vars

        # Defined in async method at startup (self.run)
        self.storage = None
        self.handlers = None  # Handlers(self, self.net_config, self.storage)
        self.serializer = None  # Serializer(self.net_config, self.storage)
        self.deserializer = None  # Deserializer(self.net_config, self.storage)

        # Bitcoin network/peer net_config
        self.net_config = net_config
        self.peers = self.net_config.peers
        self.peer = self.get_peer()
        self.host = host  # bind address
        self.port = port  # bind port
        self.protocol: BufferedProtocol = None
        self.protocol_factory = None

        # Connection entry/exit
        self.handshake_complete_event = asyncio.Event()
        self.con_lost_event = asyncio.Event()

        # Worker queues & events
        self.worker_in_queue_preproc = multiprocessing.Queue()  # no ack needed
        self.worker_in_queue_tx_parse = multiprocessing.Queue()
        self.worker_in_queue_mtree = multiprocessing.Queue()
        self.worker_in_queue_blk_writer = multiprocessing.Queue()
        self.worker_ack_queue_tx_parse_confirmed = multiprocessing.Queue()  # blk_hash:tx_count
        # self.worker_ack_queue_tx_parse_mempool = multiprocessing.Queue()  # tx_count
        self.worker_ack_queue_mtree = multiprocessing.Queue()
        self.worker_ack_queue_blk_writer = multiprocessing.Queue()
        self.worker_in_queue_logging = multiprocessing.Queue()  # only for clean shutdown
        self.initial_block_download_event_mp = multiprocessing.Event()

        # Mempool and API state
        self.mempool_tx_hash_set = set()

        # Database Interfaces
        self.pg_db: Optional[PG_Database] = None

        # Bitcoin Network IO + callbacks
        self.bitcoin_net_io = BitcoinNetIO(self.on_buffer_full, self.on_msg,
            self.on_connection_made, self.on_connection_lost)
        self.shm_buffer_view = self.bitcoin_net_io.shm_buffer_view
        self.shm_buffer = self.bitcoin_net_io.shm_buffer

    async def setup(self):
        self.storage = await setup_storage(self.config, self.net_config)
        self.handlers = Handlers(self, self.net_config, self.storage)
        self.serializer = Serializer(self.net_config, self.storage)
        self.deserializer = Deserializer(self.net_config, self.storage)
        self.sync_state = SyncState(self.storage)

    async def connect_session(self):
        loop = asyncio.get_event_loop()
        peer = self.get_peer()
        self.logger.debug("connecting to (%s, %s) [%s]", peer.host, peer.port, self.net_config.NET)
        protocol_factory = lambda: self.bitcoin_net_io
        self.transport, self.session = await loop.create_connection(
            protocol_factory, peer.host, peer.port
        )
        return self.transport, self.session

    async def run(self):
        try:
            await self.setup()
            await self.connect_session()
            init_handshake = asyncio.create_task(
                self.send_version(
                    self.peer.host,
                    self.peer.port,
                    self.host,
                    self.port,
                )
            )
            wait_until_conn_lost = asyncio.create_task(
                self.con_lost_event.wait()
            )
            await asyncio.wait([init_handshake, wait_until_conn_lost])
        finally:
            if self.transport:
                self.transport.close()
            await self.storage.close()

    def get_peer(self) -> Peer:
        return self.peers[0]

    # ---------- Callbacks ---------- #
    def on_buffer_full(self):
        coro = self._on_buffer_full()
        asyncio.run_coroutine_threadsafe(coro, self.loop)

    def on_msg(self, command: bytes, message: memoryview):
        if command != BLOCK_BIN:
            self.sync_state.incr_msg_received_count()
        self.sync_state.incoming_msg_queue.put_nowait((command, message))

    def on_connection_made(self):
        _fut = asyncio.create_task(self.start_jobs())

    def on_connection_lost(self):
        self.con_lost_event.set()

    # ---------- end callbacks ---------- #
    async def _on_buffer_full(self):
        """Waits until all messages in the shared memory buffer have been processed before
        resuming (and therefore resetting the buffer) BitcoinNetIO."""
        while not self.sync_state.have_processed_all_msgs_in_buffer():
            await asyncio.sleep(0.05)

        self.sync_state.reset_msg_counts()
        self.bitcoin_net_io.resume()

    def run_coro_threadsafe(self, coro, *args, **kwargs):
        asyncio.run_coroutine_threadsafe(coro(*args, **kwargs), self.loop)

    async def handle(self):
        while True:
            command, message = await self.sync_state.incoming_msg_queue.get()
            try:
                # self.logger.debug("command=%s", command.rstrip(b'\0').decode('ascii'))
                handler_func_name = "on_" + command.rstrip(b"\0").decode("ascii")
                handler_func = getattr(self.handlers, handler_func_name)
                await handler_func(message)
                if command != BLOCK_BIN:
                    self.sync_state.incr_msg_handled_count()
            except Exception as e:
                self.logger.exception("handle: ", e)
                raise

    async def send_request(self, command_name: str, message: bytes):
        self.bitcoin_net_io.send_message(message)

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
                self.worker_ack_queue_tx_parse_confirmed,
                self.initial_block_download_event_mp,
            )
            p.start()
            self.processes.append(p)
        for i in range(WORKER_COUNT_MTREE_CALCULATORS):
            p = MTreeCalculator(
                self.shm_buffer.name, self.worker_in_queue_mtree, self.worker_ack_queue_mtree,
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

    async def spawn_handler_tasks(self):
        """spawn 4 tasks so that if one handler is waiting on an event that depends on
        another handler, progress will continue to be made."""
        for i in range(4):
            _task = asyncio.create_task(self.handle())

    async def spawn_sync_headers_task(self):
        """runs once at startup and is re-spawned for new unsolicited block tips"""
        _sync_headers_task = asyncio.create_task(self.sync_headers_job())

    async def spawn_initial_block_download(self):
        """runs once at startup and is re-spawned for new unsolicited block tips"""
        _sync_blocks_task = asyncio.create_task(self.sync_all_blocks_job())

    async def spawn_request_mempool(self):
        """after initial block download requests the full mempool.

        NOTE: once the sync_state.initial_block_download_event is set, relayed mempool txs will also
        begin getting processed by the "on_inv" handler -> triggering a 'getdata' for the txs."""
        _request_mempool_task = asyncio.create_task(self.request_mempool_job())

    async def start_jobs(self):
        try:
            self.pg_db: PG_Database = self.storage.pg_database
            await self.spawn_handler_tasks()
            await self.handshake_complete_event.wait()

            self.start_workers()
            await self.spawn_sync_headers_task()
            await self.sync_state.headers_event_initial_sync.wait()  # one-off

            await self.spawn_initial_block_download()
            await self.sync_state.initial_block_download_event.wait()
            await self.spawn_request_mempool()
        except Exception as e:
            self.logger.exception(e)
            raise

    async def request_mempool_job(self):
        await self.send_request(MEMPOOL, self.serializer.mempool())

    async def _get_max_headers(self):
        block_locator_hashes = [self.storage.headers.longest_chain().tip.hash]
        hash_count = len(block_locator_hashes)
        await self.send_request(
            GETHEADERS, self.serializer.getheaders(hash_count, block_locator_hashes, ZERO_HASH)
        )

    async def sync_headers_job(self):
        """supervises completion of syncing all headers to target height"""
        self.logger.debug("starting sync_headers_job...")

        self.sync_state.target_block_header_height = self.sync_state.get_local_tip_height()
        while True:
            if not self.sync_state.local_tip_height < self.sync_state.target_header_height:
                self.sync_state.headers_event_initial_sync.set()
                await self.sync_state.headers_event_new_tip.wait()
                self.sync_state.headers_event_new_tip.clear()
            await self._get_max_headers()
            await self.sync_state.headers_msg_processed_event.wait()
            self.sync_state.headers_msg_processed_event.clear()
            self.sync_state.local_tip_height = self.sync_state.update_local_tip_height()
            self.logger.debug(
                "new headers tip height: %s", self.sync_state.local_tip_height,
            )
            self.sync_state.blocks_event_new_tip.set()

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
        self.sync_state.done_block_heights = []

        while True:
            try:
                # This queue should transition to a multiprocessing queue
                block_hash, txs_done_count = self.worker_ack_queue_tx_parse_confirmed.get()
                self.sync_state._pending_blocks_progress_counter[block_hash] += txs_done_count

                header = self.get_header_for_hash(block_hash)
                self.logger.debug(f"block height={header.height} done!")

                self.sync_state.done_block_heights.append(header.height)

                try:
                    self.sync_state.pending_blocks_batch_set.remove(block_hash)
                except KeyError:
                    header = self.get_header_for_hash(block_hash)
                    self.logger.debug(f"also parsed block: {header.height}")

                # all blocks in batch parsed
                if len(self.sync_state.pending_blocks_batch_set) == 0:
                    # in a crash prior to this point, the last 500 blocks will just get
                    # re-done on start-up because the block_headers.mmap file has not
                    # been updated with any headers.

                    for height in sorted(self.sync_state.done_block_heights):
                        self.logger.debug(f"new block tip height: {height}")
                        header = self.get_header_for_height(height)
                        block_headers: bitcoinx.Headers = self.storage.block_headers
                        block_headers.connect(header.raw)
                    self.sync_state.reset_pending_blocks()
                    return
            except Exception as e:
                self.logger.exception(e)

    async def sync_batched_blocks(self) -> None:
        # one block per loop
        pending_getdata_requests = list(self.sync_state.pending_blocks_batch_set)
        count_requested = 0
        while True:
            inv = await self.sync_state._pending_blocks_inv_queue.get()
            # self.logger.debug(f"got inv: {inv}")
            try:
                header = self.get_header_for_hash(hex_str_to_hash(inv.get("inv_hash")))
            except MissingHeader as e:
                self.logger.warning("header with hash=%s not found", inv.get("inv_hash"))
                if self.sync_state._pending_blocks_inv_queue.empty():
                    break
                else:
                    continue

            if not header.height <= self.sync_state.stop_header_height:
                self.logger.debug(f"ignoring block height={header.height} until sync'd")
                self.logger.debug(f"len(pending_getdata_requests)={len(pending_getdata_requests)}")
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
            coro = asyncio.get_running_loop().run_in_executor(
                self.sync_state._batched_blocks_exec, self.join_batched_blocks
            )
            try:
                # if a batch of blocks takes more than 5 minutes - it will print out a
                # diagnostic overview to assist with troubleshooting the blockage
                await asyncio.wait_for(coro, timeout=300.0)
            except asyncio.TimeoutError:
                self.sync_state.readout_sync_state()
        else:
            # it was an unsolicited block therefore -> recurse (to continue with batch)
            await self.sync_batched_blocks()

    async def sync_all_blocks_job(self):
        """supervises completion of syncing all blocks to target height"""
        try:
            # up to 500 blocks per loop
            while True:
                if self.sync_state.is_synchronized():
                    self.logger.debug("block tip synced to match headers tip")
                    await self.sync_state.wait_for_new_tip()
                chain = self.storage.block_headers.longest_chain()
                block_locator_hashes = [chain.tip.hash]
                hash_count = len(block_locator_hashes)
                self.logger.debug(
                    "requesting max number of blocks (up to 500) for chain tip " "height=%s",
                    self.sync_state.get_local_block_tip_height(),
                )
                hash_stop = ZERO_HASH  # get max

                self.sync_state.reset_pending_blocks_batch_set()

                await self.send_request(
                    GETBLOCKS,
                    self.serializer.getblocks(hash_count, block_locator_hashes, hash_stop),
                )
                await self.sync_batched_blocks()

        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger.exception("sync_blocks_job raised an exception", e)
            raise

    # -- Message Types -- #
    async def send_version(self, recv_host=None, recv_port=None, send_host=None, send_port=None):
        message = self.serializer.version(
            recv_host=recv_host, recv_port=recv_port, send_host=send_host, send_port=send_port,
        )
        await self.send_request(VERSION, message)

    async def send_inv(self, inv_vects: List[Dict]):
        await self.serializer.inv(inv_vects)
