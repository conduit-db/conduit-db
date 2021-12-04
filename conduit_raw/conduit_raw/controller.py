import aiohttp
import asyncio
from asyncio import BaseTransport, BaseProtocol
import bitcoinx
from bitcoinx import hex_str_to_hash, MissingHeader, hash_to_hex_str, Header
from concurrent.futures.thread import ThreadPoolExecutor
import os
import queue
import threading
import time
import typing
from typing import Optional, List, Dict
import multiprocessing
from multiprocessing.process import BaseProcess
from pathlib import Path
import logging
import zmq

from conduit_lib import (setup_storage, Storage, IPCSocketClient, LMDB_Database, cast_to_valid_ipv4,
    Serializer, Deserializer, Handlers, BitcoinNetIO, wait_for_node)
from conduit_lib.commands import BLOCK_BIN, GETHEADERS, GETDATA, GETBLOCKS, VERSION
from conduit_lib.constants import CONDUIT_RAW_SERVICE_NAME, WORKER_COUNT_MTREE_CALCULATORS, \
    WORKER_COUNT_BLK_WRITER, REGTEST, ZERO_HASH
from .aiohttp_api import server_main
from .batch_completion import BatchCompletionRaw, BatchCompletionMtree, BatchCompletionPreprocessor
from .regtest_support import RegtestSupport
from .sock_server.ipc_sock_server import ThreadedTCPServer, ThreadedTCPRequestHandler
from .sync_state import SyncState
from .preprocessor import BlockPreProcessor  # threading.Thread
from .workers import MTreeCalculator, BlockWriter  # multiprocessing.Process

if typing.TYPE_CHECKING:
    from conduit_lib import NetworkConfig, Peer

MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))


try:
    # Todo - this is messy
    CONDUIT_RAW_API_HOST: str = os.environ.get('CONDUIT_RAW_API_HOST', 'localhost:50000')
    CONDUIT_RAW_HOST = cast_to_valid_ipv4(CONDUIT_RAW_API_HOST.split(":")[0])
    CONDUIT_RAW_PORT = int(CONDUIT_RAW_API_HOST.split(":")[1])
except Exception:
    logger = logging.getLogger('sync-state-env-vars')
    logger.exception("unexpected exception")


class Controller:
    """Designed to sync the blockchain as fast as possible.

    Coordinates:
    - network via BitcoinNetIO
    - outsources parsing of block data to subordinate workers (which work in parallel reading
    from a shared memory buffer)
    - synchronizes the refreshing of the shared memory buffer which holds multiple raw blocks at
    a time up to HIGH_WATER)
    """

    def __init__(self, config: Dict, net_config: 'NetworkConfig', host="127.0.0.1", port=8000,
            logging_server_proc=None, loop_type=None):
        self.service_name = CONDUIT_RAW_SERVICE_NAME
        self.running = False
        self.logging_server_proc: BaseProcess = logging_server_proc
        self.processes: List[BaseProcess] = [self.logging_server_proc]
        self.tasks = []
        self.logger = logging.getLogger("controller")
        self.loop = asyncio.get_event_loop()
        if loop_type == 'uvloop':
            self.logger.debug(f"Using uvloop")
        elif not loop_type:
            self.logger.debug(f"Using default asyncio event loop")

        self.config = config  # cli args & env_vars

        # Defined in async method at startup (self.run)
        self.storage: Optional[Storage] = None
        self.handlers: Optional[Handlers] = None
        self.serializer: Optional[Serializer] = None
        self.deserializer: Optional[Deserializer] = None
        self.regtest_support: Optional[RegtestSupport] = None
        self.lmdb: Optional[LMDB_Database] = None
        self.ipc_sock_client: Optional[IPCSocketClient] = None
        self.executor = ThreadPoolExecutor(max_workers=1)

        # Bitcoin network/peer net_config
        self.net_config = net_config
        self.peers = self.net_config.peers
        self.peer = self.get_peer()
        self.host = host  # bind address
        self.port = port  # bind port

        # Connection entry/exit
        self.handshake_complete_event = asyncio.Event()
        self.con_lost_event = asyncio.Event()

        # Worker queues & events
        self.worker_in_queue_preproc = queue.Queue()  # no ack needed
        self.worker_ack_queue_preproc = queue.Queue()

        # PUB-SUB from Controller to worker to kill the worker
        # Todo - this will be very unreliable due to how ZMQ works... Need a better solution
        context = zmq.Context()
        self.kill_worker_socket = context.socket(zmq.PUB)
        self.kill_worker_socket.bind("tcp://127.0.0.1:46464")

        self.worker_in_queue_mtree = multiprocessing.Queue()
        self.worker_in_queue_blk_writer = multiprocessing.Queue()
        self.worker_ack_queue_mtree = multiprocessing.Queue()
        self.worker_ack_queue_blk_writer = multiprocessing.Queue()

        self.blocks_batch_set_queue_raw = queue.Queue()
        self.blocks_batch_set_queue_mtree = queue.Queue()
        self.blocks_batch_set_queue_preproc = queue.Queue()

        # Database Interfaces
        # self.mysql_db: Optional[MySQLDatabase] = None

        # Bitcoin Network IO + callbacks
        self.transport: Optional[BaseTransport] = None
        self.session: Optional[BaseProtocol] = None
        self.bitcoin_net_io = BitcoinNetIO(self.on_buffer_full, self.on_msg,
            self.on_connection_made, self.on_connection_lost)
        self.shm_buffer_view = self.bitcoin_net_io.shm_buffer_view
        self.shm_buffer = self.bitcoin_net_io.shm_buffer

        self.sync_state: Optional[SyncState] = None
        self.estimated_moving_av_block_size = 181  # bytes
        self.aiohttp_client_session: Optional[aiohttp.ClientSession] = None
        self.new_headers_queue: asyncio.Queue[tuple[bool, Header, Header]] = asyncio.Queue()

    async def setup(self):
        headers_dir = MODULE_DIR.parent
        self.storage = setup_storage(self.config, self.net_config, headers_dir)
        self.handlers = Handlers(self, self.net_config, self.storage)
        self.serializer = Serializer(self.net_config, self.storage)
        self.deserializer = Deserializer(self.net_config, self.storage)
        self.regtest_support = RegtestSupport(self)
        self.sync_state = SyncState(self.storage, self)
        self.lmdb = self.storage.lmdb
        self.batch_completion_raw = BatchCompletionRaw(
            self,
            self.sync_state,
            self.worker_ack_queue_blk_writer,
            self.blocks_batch_set_queue_raw
        )
        self.batch_completion_mtree = BatchCompletionMtree(
            self,
            self.sync_state,
            self.worker_ack_queue_mtree,
            self.blocks_batch_set_queue_mtree
        )
        self.batch_completion_preprocessor = BatchCompletionPreprocessor(
            self,
            self.sync_state,
            self.worker_ack_queue_preproc,
            self.blocks_batch_set_queue_preproc
        )

        self.batch_completion_raw.start()
        self.batch_completion_mtree.start()
        self.batch_completion_preprocessor.start()

    async def connect_session(self):
        peer = self.get_peer()
        self.logger.debug("Connecting to (%s, %s) [%s]", peer.host, peer.port, self.net_config.NET)
        self.transport, self.session = await self.bitcoin_net_io.connect(peer.host, peer.port)
        return self.transport, self.session

    async def run(self):
        self.running = True
        try:
            await self._get_aiohttp_client_session()
            await self.setup()
            await wait_for_node(node_host=self.config['node_host'],
                serializer=self.serializer, deserializer=self.deserializer)
            await self.connect_session()  # on_connection_made callback -> starts jobs
            init_handshake = asyncio.create_task(self.send_version(self.peer.host, self.peer.port,
                self.host, self.port))
            self.tasks.append(init_handshake)
            wait_until_conn_lost = asyncio.create_task(self.con_lost_event.wait())
            self.tasks.append(wait_until_conn_lost)
            await asyncio.wait([init_handshake, wait_until_conn_lost])
        except Exception:
            self.logger.exception("Unexpected exception")
        finally:
            await self.stop()

    async def stop(self):
        self.running = False
        try:
            await self._close_aiohttp_client_session()
            if self.transport:
                self.transport.close()
            if self.storage:
                await self.storage.close()
            with self.kill_worker_socket as sock:
                sock.send(b"stop_signal")

            await asyncio.sleep(1)

            for p in self.processes:
                p.terminate()
                p.join()

            self.shm_buffer.close()
            self.shm_buffer.unlink()

            self.ipc_sock_client.stop()  # stops server
            time.sleep(0.5)  # allow time for server to stop (and close lmdb handle)
            self.ipc_sock_client.close()  # closes client channel
            self.lmdb.close()


            for task in self.tasks:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        except Exception:
            self.logger.exception("Suppressing raised exceptions on cleanup")

    def get_peer(self) -> 'Peer':
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
        self.tasks.append(asyncio.create_task(self.start_jobs()))

    def on_connection_lost(self):
        self.con_lost_event.set()

    # ---------- end callbacks ---------- #
    async def _on_buffer_full(self):
        """Waits until all messages in the shared memory buffer have been processed before
        resuming (and therefore resetting the buffer) BitcoinNetIO."""
        try:
            while not self.sync_state.have_processed_all_msgs_in_buffer():
                await asyncio.sleep(0.1)

            self.sync_state.reset_msg_counts()
            with self.sync_state.done_blocks_raw_lock:
                self.sync_state.done_blocks_raw = set()
            with self.sync_state.done_blocks_mtree_lock:
                self.sync_state.done_blocks_mtree = set()
            with self.sync_state.done_blocks_preproc_lock:
                self.sync_state.done_blocks_preproc = set()
            self.sync_state.received_blocks = set()
            self.bitcoin_net_io.resume()
        except Exception:
            self.logger.exception("Unexpected problem in '_on_buffer_full'")
            raise

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
                self.logger.exception("Handle: ", e)
                raise

    async def send_request(self, command_name: str, message: bytes):
        self.bitcoin_net_io.send_message(message)

    def start_workers(self):
        # Thread (lightweight task -> lower latency this way)
        t = BlockPreProcessor(self.shm_buffer.name, self.worker_in_queue_preproc,
                self.worker_in_queue_mtree, self.worker_ack_queue_preproc)
        t.start()

        # Processes
        for i in range(WORKER_COUNT_MTREE_CALCULATORS):
            worker_id = i+1
            p = MTreeCalculator(worker_id, self.shm_buffer.name, self.worker_in_queue_mtree,
                self.worker_ack_queue_mtree)
            p.start()
            self.processes.append(p)
        for i in range(WORKER_COUNT_BLK_WRITER):
            p = BlockWriter(self.shm_buffer.name, self.worker_in_queue_blk_writer,
                self.worker_ack_queue_blk_writer)
            p.start()
            self.processes.append(p)

    async def spawn_handler_tasks(self):
        """spawn 4 tasks so that if one handler is waiting on an event that depends on
        another handler, progress will continue to be made."""
        for i in range(4):
            self.tasks.append(asyncio.create_task(self.handle()))

    async def spawn_sync_headers_task(self):
        """runs once at startup and is re-spawned for new unsolicited block tips"""
        self.tasks.append(asyncio.create_task(self.sync_headers_job()))

    async def spawn_initial_block_download(self):
        """runs once at startup and is re-spawned for new unsolicited block tips"""
        self.tasks.append(asyncio.create_task(self.sync_all_blocks_job()))

    def ipc_sock_server_thread(self):
        self.ipc_sock_server = ThreadedTCPServer(addr=(CONDUIT_RAW_HOST, CONDUIT_RAW_PORT),
            handler=ThreadedTCPRequestHandler, storage_path=Path(self.lmdb._storage_path),
            block_headers=self.storage.block_headers)
        self.ipc_sock_server.serve_forever()

    async def start_jobs(self):
        try:
            thread = threading.Thread(target=self.ipc_sock_server_thread)
            thread.start()
            await self.spawn_aiohttp_api()
            await self.spawn_handler_tasks()
            await self.handshake_complete_event.wait()

            self.start_workers()

            # In regtest old block timestamps (submitted to node) will not take the node out of IBD
            # This works around it by polling the node's RPC. This is never needed in prod.
            # if self.net_config.NET == REGTEST:
            #     await self.spawn_poll_node_for_header_job()

            await self.spawn_sync_headers_task()
            await self.sync_state.headers_event_initial_sync.wait()  # one-off
            await self.spawn_initial_block_download()
            # await self.sync_state.initial_block_download_event.wait()
            # await self.request_mempool()
        except Exception as e:
            self.logger.exception(e)
            raise

    # async def request_mempool(self):
    #     # NOTE: if the -rejectmempoolrequest=0 option is not set on the node, the node disconnects
    #     self.logger.debug("Requesting mempool...")
    #     await self.send_request(MEMPOOL, self.serializer.mempool())

    async def _get_max_headers(self):
        tip = self.storage.headers.longest_chain().tip
        block_locator_hashes = []
        for i in range(0, 25, 2):
            height = tip.height - i**2
            if i == 0 or height > 0:
                locator_hash = self.get_header_for_height(height).hash
                block_locator_hashes.append(locator_hash)
        hash_count = len(block_locator_hashes)
        await self.send_request(
            GETHEADERS, self.serializer.getheaders(hash_count, block_locator_hashes, ZERO_HASH)
        )

    async def _get_aiohttp_client_session(self):
        if not self.aiohttp_client_session:
            self.aiohttp_client_session = aiohttp.ClientSession()
        return self.aiohttp_client_session

    async def _close_aiohttp_client_session(self):
        await self.aiohttp_client_session.close()

    async def sync_headers_job(self):
        try:
            """supervises completion of syncing all headers to target height"""
            self.logger.debug("Starting sync_headers_job...")
            self.sync_state.target_block_header_height = self.sync_state.get_local_tip_height()
            while True:
                if not self.sync_state.local_tip_height < self.sync_state.target_header_height:
                    inv = await self.sync_state.wait_for_new_headers_tip()

                await self._get_max_headers()  # Gets ignored when node is in IBD
                msg = await self.sync_state.headers_msg_processed_queue.get()
                if msg is None:
                    continue

                self.sync_state.local_tip_height = self.sync_state.update_local_tip_height()
                self.logger.debug(
                    "New headers tip height: %s", self.sync_state.local_tip_height,
                )
                self.new_headers_queue.put_nowait(msg)
        except Exception as e:
            self.logger.exception(f"Unexpected exception in sync_headers_job")

    def get_header_for_hash(self, block_hash: bytes) -> bitcoinx.Header:
        header, chain = self.storage.headers.lookup(block_hash)
        return header

    def get_header_for_height(self, height: int) -> bitcoinx.Header:
        headers = self.storage.headers
        chain = self.storage.headers.longest_chain()
        header = headers.header_at_height(chain, height)
        return header

    async def request_all_batch_block_data(self, batch_id, blocks_batch_set, stop_header_height):
        """
        This method relies on the node responding to the prior getblocks request with up to 500
        inv messages.
        """
        count_pending = len(blocks_batch_set)
        count_requested = 0
        while True:
            inv = await self.sync_state.pending_blocks_inv_queue.get()
            # self.logger.debug(f"got block inv: {inv}, batch_id={batch_id}")
            try:
                header = self.get_header_for_hash(hex_str_to_hash(inv.get("inv_hash")))
            except MissingHeader as e:
                if self.sync_state.pending_blocks_inv_queue.empty() and count_requested != 0:
                    self.logger.exception(f"Fatal error. An unsolicited block "
                        f"({inv.get('inv_hash')}) was pushed to the "
                        f"pending_blocks_inv_queue - this should never happen.")
                else:
                    continue

            if not header.height <= stop_header_height:
                self.logger.debug(f"Ignoring block height={header.height} until sync'd")
                self.logger.debug(f"len(pending_getdata_requests)={count_pending}")
                if count_pending == 0:
                    break
                else:
                    continue
            await self.send_request(
                GETDATA, self.serializer.getdata([inv]),
            )

            count_requested += 1
            count_pending -= 1
            if count_pending == 0:
                break

        assert count_requested == len(blocks_batch_set)

    def find_common_parent(self, reorg_node_tip: bitcoinx.Header,
            orphaned_tip: bitcoinx.Header) -> tuple[bitcoinx.Chain, int]:

        chains: list[bitcoinx.Chain] = self.storage.headers.chains()

        # Get orphan an reorg chains
        orphaned_chain = None
        reorg_chain = None
        for chain in chains:
            if chain.tip.hash == reorg_node_tip.hash:
                reorg_chain = chain
            elif chain.tip.hash == orphaned_tip.hash:
                orphaned_chain = chain

        if reorg_chain is not None and orphaned_chain is not None:
            chain, common_parent_height = reorg_chain.common_chain_and_height(orphaned_chain)
            return reorg_chain, common_parent_height
        elif reorg_chain is not None and orphaned_chain is None:
            return reorg_chain, 0
        else:
            # Should never happen
            raise ValueError("No common parent block header could be found")

    def backfill_headers(self, first_missing_header: int, reorg_tip_height: int):
        for height in range(first_missing_header, reorg_tip_height + 1):
            header = self.get_header_for_height(height)
            self.storage.block_headers.connect(header.raw)
        self.storage.block_headers.flush()

    def reorg_detect(self, old_tip: bitcoinx.Header, new_tip: bitcoinx.Header) \
            -> Optional[tuple[int, Header, Header]]:
        try:
            assert new_tip.height > old_tip.height
            common_parent_chain, common_parent_height = self.find_common_parent(new_tip, old_tip)

            if common_parent_height < old_tip.height:
                depth = old_tip.height - common_parent_height
                self.logger.debug(f"Reorg detected of depth: {depth}. "
                                  f"Syncing missing blocks from height: "
                                  f"{common_parent_height + 1} to {new_tip.height}")
                return common_parent_height, new_tip, old_tip
        except Exception:
            self.logger.exception("unexpected exception in reorg_detect")

    async def connect_done_block_headers(self, blocks_batch_set) -> None:
        """If this returns False, it means a reorg happened"""
        sorted_headers = sorted([(self.get_header_for_hash(h).height, h) for h in
            blocks_batch_set])
        sorted_heights = [height for height, h in sorted_headers]
        # Assert the resultant blocks are consecutive with no gaps
        max_height = max(sorted_heights)
        expected_block_heights = [i+1 for i in range(max_height-len(sorted_heights), max_height)]
        assert sorted_heights == expected_block_heights

        block_headers: bitcoinx.Headers = self.storage.block_headers
        try:
            for height, _hash in sorted_headers:
                header = self.get_header_for_hash(_hash)
                block_headers.connect(header.raw)
        except bitcoinx.errors.MissingHeader as e:
            # This should indicate a reorg but we will confirm it
            self.logger.error(e)
            raise

        self.storage.block_headers.flush()

        tip = self.sync_state.get_local_block_tip()
        self.logger.debug(f"Connected up to header height: {tip.height}, "
                          f"hash: {hash_to_hex_str(tip.hash)}")

    def update_moving_average(self, current_tip_height: int):
        # sample every 72nd block for its size over the last 2 weeks (two samples per day)
        block_hashes = []
        for height in range(current_tip_height-2016, current_tip_height, 72):
            header = self.get_header_for_height(height)
            block_hashes.append(header.hash)

        with IPCSocketClient() as ipc_sock_client:
            block_metadata_batch = ipc_sock_client.block_metadata_batched(block_hashes).block_metadata_batch
        self.estimated_moving_av_block_size = sum([m.block_size for m in block_metadata_batch]) / len(block_metadata_batch)
        self.logger.debug(f"Updated estimated_moving_av_block_size: "
                          f"{int(self.estimated_moving_av_block_size / (1024**2))} MB")

    def get_hash_stop(self, stop_header_height: int):
        # We should only really request enough blocks at at time to keep our
        # recv buffer filling at the max rate. No point requesting 500 x 4GB blocks!!!
        node_longest_chain = self.storage.headers.longest_chain()
        if stop_header_height >= node_longest_chain.tip.height:
            hash_stop = ZERO_HASH
        else:
            stop_height = stop_header_height + 1
            header: bitcoinx.Header = self.storage.headers.header_at_height(node_longest_chain,
                stop_height)
            hash_stop = header.hash
        return hash_stop

    async def wait_for_batched_blocks_completion(self, batch_id, all_pending_block_hashes,
            stop_header_height) -> bool:
        """all_pending_block_hashes is copied into these threads to prevent mutation"""
        try:
            await self.request_all_batch_block_data(batch_id, all_pending_block_hashes.copy(),
                stop_header_height)

            self.blocks_batch_set_queue_raw.put_nowait(all_pending_block_hashes.copy())
            self.blocks_batch_set_queue_mtree.put_nowait(all_pending_block_hashes.copy())
            self.blocks_batch_set_queue_preproc.put_nowait(all_pending_block_hashes.copy())

            # Wait for batch completion for all worker types (via ACK messages)
            await self.sync_state.done_blocks_raw_event.wait()
            await self.sync_state.done_blocks_mtree_event.wait()
            await self.sync_state.done_blocks_preproc_event.wait()
            self.sync_state.done_blocks_raw_event.clear()
            self.sync_state.done_blocks_mtree_event.clear()
            self.sync_state.done_blocks_preproc_event.clear()

            await self.enforce_lmdb_flush()  # Until this completes a crash leads to rollback
            await self.connect_done_block_headers(all_pending_block_hashes.copy())

        except Exception:
            self.logger.exception("Unexpected exception in 'wait_for_batched_blocks_completion' ")


    async def sync_all_blocks_job(self):
        """supervises completion of syncing all blocks to target height"""

        try:
            # up to 500 blocks per loop
            batch_id = 0
            while True:
                # ------------------------- Batch Start ------------------------- #
                # If there's a reorg, starting header can be less than longest_chain().tip
                msg = await self.new_headers_queue.get()
                is_reorg, start_header, stop_header = msg

                if start_header.height <= self.sync_state.get_local_block_tip_height() and not is_reorg:
                    continue

                original_stop_height = stop_header.height
                while True:
                    first_locator = self.get_header_for_height(start_header.height - 1)

                    if batch_id == 0:
                        self.logger.info(f"Starting Initial Block Download")
                    else:
                        self.logger.debug(f"Controller Batch {batch_id} Start")
                    chain = self.storage.block_headers.longest_chain()

                    block_locator_hashes = [first_locator.hash]
                    hash_count = len(block_locator_hashes)
                    hash_stop = ZERO_HASH  # get max

                    # Allocate next batch of blocks - reassigns new global sync_state.blocks_batch_set
                    if chain.tip.height > 2016:
                        self.update_moving_average(chain.tip.height)

                    from_height = self.get_header_for_hash(start_header.hash).height - 1
                    to_height = stop_header.height
                    next_batch = self.sync_state.get_next_batched_blocks(from_height, to_height)
                    batch_count, all_pending_block_hashes, stop_header_height = next_batch

                    if batch_count != 500:  # i.e. max allowed by p2p proto is 500 at a time
                        hash_stop = self.get_hash_stop(stop_header_height)

                    await self.send_request(GETBLOCKS,
                        self.serializer.getblocks(hash_count, block_locator_hashes, hash_stop))

                    # Workers are loaded by Handlers.on_block handler as messages are received
                    await self.wait_for_batched_blocks_completion(batch_id,
                        all_pending_block_hashes, stop_header_height)

                    # ------------------------- Batch complete ------------------------- #
                    if batch_id == 0:
                        self.logger.debug(f"Initial Block Download Complete. New block tip height: "
                            f"{self.sync_state.get_local_block_tip_height()}")
                    else:
                        self.logger.debug(f"Controller Batch {batch_id} Complete."
                            f" New tip height: {self.sync_state.get_local_block_tip_height()}")
                    batch_id += 1
                    start_header = self.get_header_for_height((start_header.height - 1) + batch_count)
                    if self.sync_state.get_local_block_tip_height() == original_stop_height:
                        break

        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger.exception("sync_blocks_job raised an exception")
            raise

    # -- Message Types -- #
    async def send_version(self, recv_host=None, recv_port=None, send_host=None, send_port=None):
        try:
            message = self.serializer.version(
                recv_host=recv_host, recv_port=recv_port, send_host=send_host, send_port=send_port,
            )
            await self.send_request(VERSION, message)
        except Exception:
            self.logger.exception("Unexpected exception")
            raise

    async def send_inv(self, inv_vects: List[Dict]):
        await self.serializer.inv(inv_vects)

    async def enforce_lmdb_flush(self):
        t0 = time.perf_counter()
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self.executor, self.lmdb.sync)
        t1 = time.perf_counter() - t0
        self.logger.debug(f"Flush for batch took {t1} seconds")

    async def spawn_aiohttp_api(self):
        self.tasks.append(asyncio.create_task(server_main.main()))

    async def spawn_poll_node_for_header_job(self):
        self.tasks.append(asyncio.create_task(self.regtest_support.regtest_poll_node_for_tip_job()))
