import asyncio
import os
import queue
import time
import typing
from asyncio import BaseTransport, BaseProtocol
from concurrent.futures.thread import ThreadPoolExecutor
import multiprocessing
from multiprocessing.process import BaseProcess
from pathlib import Path
import logging
from typing import Optional, List, Dict

import bitcoinx
from bitcoinx import hex_str_to_hash, MissingHeader, hash_to_hex_str
import zmq
from confluent_kafka import Producer

from conduit_lib.conduit_raw_api_client import ConduitRawAPIClient
from conduit_lib.database.lmdb.lmdb_database import LMDB_Database
from conduit_lib.wait_for_dependencies import wait_for_node, wait_for_kafka

from .batch_completion import BatchCompletionRaw, BatchCompletionMtree, BatchCompletionPreprocessor
from .grpc_server.grpc_server import ConduitRaw
from .sync_state import SyncState
from conduit_lib.bitcoin_net_io import BitcoinNetIO
from .preprocessor import BlockPreProcessor
from .workers.merkle_tree import MTreeCalculator
from .workers.raw_blocks import BlockWriter
from conduit_lib.store import setup_storage, Storage
from conduit_lib.commands import VERSION, GETHEADERS, GETBLOCKS, GETDATA, BLOCK_BIN, MEMPOOL
from conduit_lib.handlers import Handlers
from conduit_lib.constants import (
    ZERO_HASH, WORKER_COUNT_MTREE_CALCULATORS, WORKER_COUNT_BLK_WRITER
)
from conduit_lib.deserializer import Deserializer
from conduit_lib.serializer import Serializer

if typing.TYPE_CHECKING:
    from conduit_lib.networks import NetworkConfig
    from conduit_lib.peers import Peer


MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))


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
            logging_server_proc=None):

        self.running = False
        self.logging_server_proc: BaseProcess = logging_server_proc
        self.processes: List[BaseProcess] = [self.logging_server_proc]
        self.tasks = []
        self.logger = logging.getLogger("controller")
        self.loop = asyncio.get_event_loop()

        self.config = config  # cli args & env_vars

        # Defined in async method at startup (self.run)
        self.storage: Optional[Storage] = None
        self.handlers: Optional[Handlers] = None
        self.serializer: Optional[Serializer] = None
        self.deserializer: Optional[Deserializer] = None
        self.lmdb: Optional[LMDB_Database] = None
        self.lmdb_grpc_client: Optional[ConduitRawAPIClient] = None
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
        self.headers_producer: Optional[Producer] = None
        self.mempool_tx_producer: Optional[Producer] = None


    async def setup(self):
        headers_dir = MODULE_DIR.parent
        self.storage = setup_storage(self.config, self.net_config, headers_dir)
        self.handlers = Handlers(self, self.net_config, self.storage)
        self.serializer = Serializer(self.net_config, self.storage)
        self.deserializer = Deserializer(self.net_config, self.storage)
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

        # Headers State is shared between ConduitRaw and ConduitIndex via this server
        tip = self.storage.block_headers.longest_chain().tip
        kafka_producer_config = {
            'bootstrap.servers': os.environ.get('KAFKA_HOST', "127.0.0.1:26638"),
        }
        # Push initial tip (if this message gets pushed more than once on restarts the consumer
        # should be able to handle it)
        self.headers_producer = Producer(**kafka_producer_config)
        self.headers_producer.produce(topic="conduit-raw-headers-state", value=tip.raw)
        self.mempool_tx_producer = Producer(**kafka_producer_config)

    async def connect_session(self):
        peer = self.get_peer()
        self.logger.debug("connecting to (%s, %s) [%s]", peer.host, peer.port, self.net_config.NET)
        self.transport, self.session = await self.bitcoin_net_io.connect(peer.host, peer.port)
        return self.transport, self.session

    async def run(self):
        self.running = True
        try:
            await wait_for_kafka(kafka_host=self.config['kafka_host'])
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
            self.logger.exception("unexpected exception")
        finally:
            await self.stop()

    async def stop(self):
        self.running = False
        try:
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

            self.lmdb_grpc_client.stop()  # stops server
            time.sleep(0.5)  # allow time for server to stop (and close lmdb handle)
            self.lmdb_grpc_client.close()  # closes client channel
            self.lmdb.close()


            for task in self.tasks:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        except Exception:
            self.logger.exception("suppressing raised exceptions on cleanup")

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
            self.logger.exception("unexpected problem in '_on_buffer_full'")
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
                self.logger.exception("handle: ", e)
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

    # Asyncio-based Tasks that run on the __main__ thread
    async def spawn_lmdb_grpc_server(self):
        self.tasks.append(asyncio.create_task(self.lmdb_grpc_server.serve()))

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

    async def start_jobs(self):
        try:
            self.lmdb_grpc_server = ConduitRaw(Path(self.lmdb._storage_path))
            await self.spawn_lmdb_grpc_server()
            await self.spawn_handler_tasks()
            await self.handshake_complete_event.wait()

            self.start_workers()
            await self.spawn_sync_headers_task()
            await self.sync_state.headers_event_initial_sync.wait()  # one-off
            await self.spawn_initial_block_download()
            await self.sync_state.initial_block_download_event.wait()
            await self.request_mempool()
        except Exception as e:
            self.logger.exception(e)
            raise

    async def request_mempool(self):
        # NOTE: if the -rejectmempoolrequest=0 option is not set on the node, the node disconnects
        self.logger.debug("Requesting mempool...")
        await self.send_request(MEMPOOL, self.serializer.mempool())

    async def _get_max_headers(self):
        block_locator_hashes = [self.storage.headers.longest_chain().tip.hash]
        hash_count = len(block_locator_hashes)
        await self.send_request(
            GETHEADERS, self.serializer.getheaders(hash_count, block_locator_hashes, ZERO_HASH)
        )

    async def sync_headers_job(self):
        try:
            """supervises completion of syncing all headers to target height"""
            self.logger.debug("Starting sync_headers_job...")

            self.sync_state.target_block_header_height = self.sync_state.get_local_tip_height()
            while True:
                if not self.sync_state.local_tip_height < self.sync_state.target_header_height:
                    await self.sync_state.wait_for_new_headers_tip()
                await self._get_max_headers()
                await self.sync_state.headers_msg_processed_event.wait()
                self.sync_state.headers_msg_processed_event.clear()
                self.sync_state.local_tip_height = self.sync_state.update_local_tip_height()
                self.logger.debug(
                    "new headers tip height: %s", self.sync_state.local_tip_height,
                )
                self.sync_state.blocks_event_new_tip.set()
        except Exception as e:
            self.logger.exception(f"unexpected exception in sync_headers_job")

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
                self.logger.debug(f"ignoring block height={header.height} until sync'd")
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

    async def connect_done_block_headers(self, blocks_batch_set):
        sorted_headers = sorted([(self.get_header_for_hash(h).height, h) for h in
            blocks_batch_set])
        sorted_heights = [height for height, h in sorted_headers]
        # Assert the resultant blocks are consecutive with no gaps
        max_height = max(sorted_heights)
        expected_block_heights = [i+1 for i in range(max_height-len(sorted_heights), max_height)]
        assert sorted_heights == expected_block_heights

        block_headers: bitcoinx.Headers = self.storage.block_headers
        for height, hash in sorted_headers:
            header = self.get_header_for_hash(hash)
            block_headers.connect(header.raw)

        self.storage.block_headers.flush()
        # ? Add reorg and other sanity checks later here...

        for height, hash in sorted_headers:
            # must push message AFTER the flush to local headers store and NOT before
            header = self.get_header_for_hash(hash)
            # TODO - this is actually blocking and should probably be run in a threadpool executor
            #  in case kafka goes offline and this blocks the entire event loop
            self.headers_producer.produce(topic="conduit-raw-headers-state", value=header.raw)

        tip = self.sync_state.get_local_block_tip()
        self.logger.debug(f"Connected up to header.hash, header.height) = "
                          f"{(tip.height, hash_to_hex_str(tip.hash))}")

    async def sanity_checks(self):
        api_block_tip_height = self.sync_state.get_local_block_tip_height()
        self.logger.debug(f"new block tip height: {api_block_tip_height}")
        await self.sync_state.wait_for_new_block_tip()

    async def sync_all_blocks_job(self):
        """supervises completion of syncing all blocks to target height"""

        async def wait_for_batched_blocks_completion(batch_id, global_blocks_batch_set, stop_header_height) -> None:
            """global_blocks_batch_set is copied into these threads to prevent mutation"""
            try:
                await self.request_all_batch_block_data(batch_id, global_blocks_batch_set.copy(),
                    stop_header_height)

                self.blocks_batch_set_queue_raw.put_nowait(global_blocks_batch_set.copy())
                self.blocks_batch_set_queue_mtree.put_nowait(global_blocks_batch_set.copy())
                self.blocks_batch_set_queue_preproc.put_nowait(global_blocks_batch_set.copy())

                # Wait for batch completion for all worker types (via ACK messages)
                await self.sync_state.done_blocks_raw_event.wait()
                await self.sync_state.done_blocks_mtree_event.wait()
                await self.sync_state.done_blocks_preproc_event.wait()
                self.sync_state.done_blocks_raw_event.clear()
                self.sync_state.done_blocks_mtree_event.clear()
                self.sync_state.done_blocks_preproc_event.clear()

                await self.enforce_lmdb_flush()  # Until this completes a crash leads to rollback
                await self.connect_done_block_headers(global_blocks_batch_set.copy())
            except Exception:
                self.logger.exception("unexpected exception in 'wait_for_batched_blocks_completion' ")

        try:
            # up to 500 blocks per loop
            batch_id = 0
            while True:
                # ------------------------- Batch Start ------------------------- #
                if self.sync_state.is_synchronized() or \
                        self.sync_state.initial_block_download_event_mp.is_set():
                    await self.sanity_checks()

                if batch_id == 0:
                    self.logger.info(f"Starting Initial Block Download")
                else:
                    batch_id += 1
                    self.logger.debug(f"Controller Batch {batch_id} Start")
                chain = self.storage.block_headers.longest_chain()

                block_locator_hashes = [chain.tip.hash]
                hash_count = len(block_locator_hashes)
                hash_stop = ZERO_HASH  # get max

                # Allocate next batch of blocks - reassigns new global sync_state.blocks_batch_set
                next_batch = self.sync_state.get_next_batched_blocks()
                batch_count, global_blocks_batch_set, stop_header_height = next_batch

                await self.send_request(
                    GETBLOCKS,
                    self.serializer.getblocks(hash_count, block_locator_hashes, hash_stop),
                )
                # Workers are loaded by Handlers.on_block handler as messages are received
                await wait_for_batched_blocks_completion(batch_id, global_blocks_batch_set,
                    stop_header_height)
                # ------------------------- Batch complete ------------------------- #
                if batch_id == 0:
                    self.logger.debug(f"Initial Block Download Complete. New block tip height: "
                        f"{self.sync_state.get_local_block_tip_height()}")
                else:
                    self.logger.debug(f"Controller Batch {batch_id} Complete."
                        f" New tip height: {self.sync_state.get_local_block_tip_height()}")


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
            self.logger.exception("unexpected exception")
            raise

    async def send_inv(self, inv_vects: List[Dict]):
        await self.serializer.inv(inv_vects)

    async def enforce_lmdb_flush(self):
        t0 = time.perf_counter()
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self.executor, self.lmdb.sync)
        t1 = time.perf_counter() - t0
        self.logger.debug(f"mtree and tx_offsets flush for batch took {t1} seconds")
