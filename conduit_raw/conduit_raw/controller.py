import aiohttp
import asyncio
import bitcoinx
from bitcoinx import hex_str_to_hash, MissingHeader, hash_to_hex_str, Header
from concurrent.futures.thread import ThreadPoolExecutor
import os
import queue
import threading
import time
from typing import Coroutine, Any, Callable, TypeVar, Sequence
import multiprocessing
from multiprocessing.process import BaseProcess
from pathlib import Path
import logging
import zmq
from zmq.asyncio import Socket as AsyncZMQSocket

from conduit_lib import (setup_storage, IPCSocketClient, Serializer, Deserializer, Handlers,
    NetworkConfig, Peer)
from conduit_lib.bitcoin_p2p_client import BitcoinP2PClient
from conduit_lib.constants import CONDUIT_RAW_SERVICE_NAME, REGTEST, ZERO_HASH
from conduit_lib.controller_base import ControllerBase
from conduit_lib.deserializer_types import Inv
from conduit_lib.headers_api_threadsafe import HeadersAPIThreadsafe
from conduit_lib.types import HeaderSpan, MultiprocessingQueue
from conduit_lib.utils import create_task, get_conduit_raw_host_and_port
from conduit_lib.wait_for_dependencies import wait_for_mysql
from conduit_lib.zmq_sockets import bind_async_zmq_socket

from .aiohttp_api import server_main
from .batch_completion import BatchCompletionMtree, BatchCompletionRaw
from .regtest_support import RegtestSupport
from .sock_server.ipc_sock_server import ThreadedTCPServer, ThreadedTCPRequestHandler
from .sync_state import SyncState
from .workers import MTreeCalculator

T2 = TypeVar("T2")

MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))


def get_headers_dir_conduit_raw() -> Path:
    return Path(os.getenv("DATADIR_SSD", str(MODULE_DIR.parent))) / 'headers_conduit_raw'


class Controller(ControllerBase):

    def __init__(self, net_config: NetworkConfig, loop_type: str | None=None) -> None:
        self.service_name = CONDUIT_RAW_SERVICE_NAME
        self.running = False
        self.processes: list[BaseProcess] = []
        self.tasks: list[asyncio.Task[Any]] = []
        self.logger = logging.getLogger("controller")
        self.loop = asyncio.get_event_loop()
        if loop_type == 'uvloop':
            self.logger.debug(f"Using uvloop")
        elif not loop_type:
            self.logger.debug(f"Using default asyncio event loop")

        # Bitcoin network/peer net_config
        self.net_config = net_config
        self.peers = self.net_config.peers
        self.peer = self.get_peer()

        self.general_executor = ThreadPoolExecutor(max_workers=4)

        # ZMQ
        self.zmq_async_context = zmq.asyncio.Context.instance()
        self.socket_kill_workers = bind_async_zmq_socket(self.zmq_async_context,
            'tcp://127.0.0.1:46464', zmq.SocketType.PUB)
        self.merkle_tree_worker_sockets: dict[int, AsyncZMQSocket] = {}  # worker_id: zmq_socket
        self.WORKER_COUNT_MTREE_CALCULATORS = int(os.getenv('WORKER_COUNT_MTREE_CALCULATORS', '4'))
        self.bind_merkle_tree_worker_zmq_listeners()
        self.current_mtree_worker_id = 1  # rotate through them evenly 1 -> 4

        headers_dir = get_headers_dir_conduit_raw()
        self.storage = setup_storage(self.net_config, headers_dir)
        self.headers_threadsafe = HeadersAPIThreadsafe(self.storage.headers,
            self.storage.headers_lock)
        self.headers_threadsafe_blocks = HeadersAPIThreadsafe(self.storage.block_headers,
            self.storage.block_headers_lock)
        self.handlers = Handlers(self, self.net_config, self.storage)
        self.serializer = Serializer(self.net_config)
        self.deserializer = Deserializer(self.net_config)
        self.sync_state = SyncState(self.storage, self)
        self.lmdb = self.storage.lmdb
        self.ipc_sock_client: IPCSocketClient | None = None
        self.ipc_sock_server: ThreadedTCPServer | None = None

        # Worker queues & events
        self.worker_in_queue_preproc: queue.Queue[tuple[bytes, int, int, int]] = queue.Queue()  # no ack needed
        self.worker_ack_queue_preproc: queue.Queue[bytes] = queue.Queue()
        self.worker_ack_queue_blk_writer: queue.Queue[bytes] = queue.Queue()
        self.worker_ack_queue_mtree: MultiprocessingQueue[bytes] = multiprocessing.Queue()
        self.blocks_batch_set_queue_raw = queue.Queue[set[bytes]]()
        self.blocks_batch_set_queue_mtree = queue.Queue[set[bytes]]()

        # Bitcoin Network Socket
        self.bitcoin_p2p_client: BitcoinP2PClient | None = None

        self.estimated_moving_av_block_size_mb = 0.1 if \
            self.sync_state.get_local_block_tip_height() < 2016 else 500
        self.aiohttp_client_session: aiohttp.ClientSession | None = None
        self.new_headers_queue: asyncio.Queue[tuple[bool, Header, Header]] = asyncio.Queue()

    def bind_merkle_tree_worker_zmq_listeners(self) -> None:
        # The Merkle Tree worker needs individual ZMQ sockets so we can ensure that
        # subsequent chunks for a large block go to the same worker for processing
        BASE_PORT_NUM = 41830
        for worker_id in range(1, self.WORKER_COUNT_MTREE_CALCULATORS + 1):
            uri = f"tcp://127.0.0.1:{BASE_PORT_NUM + worker_id}"
            self.logger.debug(f"Binding ZMQ merkle tree worker listener on: {uri}")
            zmq_socket = bind_async_zmq_socket(self.zmq_async_context, uri,
                zmq.SocketType.PUSH)
            self.merkle_tree_worker_sockets[worker_id] = zmq_socket

    async def setup(self) -> None:
        self.regtest_support = RegtestSupport(self)
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
        self.batch_completion_mtree.start()
        self.batch_completion_raw.start()

    async def connect_session(self) -> None:
        peer = self.get_peer()
        self.logger.debug("Connecting to (%s, %s) [%s]", peer.remote_host, peer.remote_port, self.net_config.NET)
        try:
            self.bitcoin_p2p_client = BitcoinP2PClient(peer.remote_host, peer.remote_port,
                self.handlers, self.net_config)
            await self.bitcoin_p2p_client.wait_for_connection()
        except ConnectionResetError:
            await self.stop()

    async def run(self) -> None:
        self.running = True
        await self._get_aiohttp_client_session()
        await self.setup()
        wait_for_mysql()
        self.tasks.append(create_task(self.start_jobs()))
        await self.connect_session()
        assert self.bitcoin_p2p_client is not None
        await self.bitcoin_p2p_client.handshake("127.0.0.1", self.net_config.PORT)
        wait_until_connection_lost = create_task(self.bitcoin_p2p_client.connection_lost_event.wait())
        self.tasks.append(wait_until_connection_lost)
        await asyncio.gather(wait_until_connection_lost)

    async def stop(self) -> None:
        self.running = False
        await self._close_aiohttp_client_session()
        if self.bitcoin_p2p_client:
            await self.bitcoin_p2p_client.close_connection()
        if self.storage:
            await self.storage.close()

        self.general_executor.shutdown(wait=False, cancel_futures=True)

        if self.socket_kill_workers:
            with self.socket_kill_workers as sock:
                try:
                    await sock.send(b"stop_signal")
                except zmq.error.ZMQError as zmq_error:
                    # We silence the error if this socket is already closed. But other cases we want
                    # to know about them and maybe fix them or add them to this list.
                    if str(zmq_error) != "not a socket":
                        raise

            await asyncio.sleep(1)
            self.socket_kill_workers.close()

        if self.merkle_tree_worker_sockets:
            for zmq_socket in self.merkle_tree_worker_sockets.values():
                zmq_socket.close()

        for p in self.processes:
            p.terminate()
            p.join()

        if self.ipc_sock_client is not None:
            self.ipc_sock_client.stop()  # stops server
            time.sleep(0.5)  # allow time for server to stop (and close lmdb handle)
            self.ipc_sock_client.close()  # closes client channel

        if self.ipc_sock_server is not None:
            self.ipc_sock_server.shutdown()

        if self.lmdb is not None:
            self.lmdb.close()

        for task in self.tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    def get_peer(self) -> Peer:
        return self.peers[0]

    def run_coro_threadsafe(self, coro: Callable[..., Coroutine[Any, Any, T2]],
            *args: Sequence[str], **kwargs: dict[Any, Any]) -> None:
        asyncio.run_coroutine_threadsafe(coro(*args, **kwargs), self.loop)

    async def handle(self) -> None:
        while True:
            command, message = await self.sync_state.incoming_msg_queue.get()
            try:
                # self.logger.debug("command=%s", command.rstrip(b'\0').decode('ascii'))
                handler_func_name = "on_" + command.rstrip(b"\0").decode("ascii")
                handler_func = getattr(self.handlers, handler_func_name)
                await handler_func(message)
            except Exception as e:
                self.logger.exception("Handler exception")
                raise

    def start_workers(self) -> None:
        for i in range(self.WORKER_COUNT_MTREE_CALCULATORS):
            worker_id = i+1
            mtree_proc = MTreeCalculator(worker_id, self.worker_ack_queue_mtree)
            mtree_proc.start()
            self.processes.append(mtree_proc)

    async def spawn_sync_headers_task(self) -> None:
        self.tasks.append(create_task(self.sync_headers_job()))

    def ipc_sock_server_thread(self) -> None:
        assert self.lmdb is not None
        host, port = get_conduit_raw_host_and_port()
        self.ipc_sock_server = ThreadedTCPServer(addr=(host, port),
            handler=ThreadedTCPRequestHandler,
            headers_threadsafe_blocks=self.headers_threadsafe_blocks,
            lmdb=self.lmdb)
        self.ipc_sock_server.serve_forever()

    def database_integrity_check(self) -> None:
        """Check the last flushed block"""
        assert self.lmdb is not None
        tip_height = self.sync_state.get_local_block_tip_height()
        height = tip_height
        while True:
            header = self.headers_threadsafe.get_header_for_height(height)
            try:
                self.logger.info(f"Checking database integrity...")
                self.lmdb.check_block(header.hash)
                self.logger.info(f"Database integrity check passed.")

                # Some blocks failed integrity checks
                # Re-synchronizing them will overwrite the old records
                # There is no strict guarantee that the block num for these
                # blocks will not change which could invalidate ConduitIndex
                # records.
                if height < tip_height:
                    self.logger.debug(f"Re-synchronising blocks from from: "
                                      f"{height} to {tip_height}")
                    start_header = self.headers_threadsafe.get_header_for_height(height)
                    stop_header = self.headers_threadsafe.get_header_for_height(tip_height)
                    self.new_headers_queue.put_nowait(HeaderSpan(is_reorg=False,
                        start_header=start_header, stop_header=stop_header))
                return None
            except AssertionError:
                self.logger.debug(f"Block {hash_to_hex_str(header.hash)} failed integrity check. "
                                  f"Repairing...")
                self.lmdb.try_purge_block_data(header.hash)
                height -= 1

    async def start_jobs(self) -> None:
        assert self.bitcoin_p2p_client is not None
        self.database_integrity_check()
        thread = threading.Thread(target=self.ipc_sock_server_thread, daemon=True)
        thread.start()
        await self.spawn_aiohttp_api()
        await self.bitcoin_p2p_client.handshake_complete_event.wait()
        self.start_workers()

        # In regtest old block timestamps (submitted to node) will not take the node out of IBD
        # This works around it by polling the node's RPC. This is never needed in prod.
        if self.net_config.NET == REGTEST:
            await self.spawn_poll_node_for_header_job()

        await self.spawn_sync_headers_task()
        await self.sync_state.headers_event_initial_sync.wait()  # one-off
        self.tasks.append(create_task(self.sync_all_blocks_job()))

    async def _get_max_headers(self) -> None:
        assert self.bitcoin_p2p_client is not None
        tip = self.sync_state.get_local_tip()
        block_locator_hashes = []
        for i in range(0, 25, 2):
            height = tip.height - i**2
            if i == 0 or height > 0:
                locator_hash = self.headers_threadsafe.get_header_for_height(height).hash
                block_locator_hashes.append(locator_hash)
        hash_count = len(block_locator_hashes)
        await self.bitcoin_p2p_client.send_message(
            self.serializer.getheaders(hash_count, block_locator_hashes, ZERO_HASH))

    async def _get_aiohttp_client_session(self) -> aiohttp.ClientSession:
        if not self.aiohttp_client_session:
            self.aiohttp_client_session = aiohttp.ClientSession()
        return self.aiohttp_client_session

    async def _close_aiohttp_client_session(self) -> None:
        assert self.aiohttp_client_session is not None
        await self.aiohttp_client_session.close()

    async def sync_headers_job(self) -> None:
        """supervises completion of syncing all headers to target height"""
        try:
            self.logger.debug("Starting sync_headers_job...")

            blocks_tip = self.sync_state.get_local_block_tip()
            headers_tip = self.sync_state.get_local_tip()
            if headers_tip.height != 0:
                self.logger.debug(f"Allocating headers to sync from: {blocks_tip.height} to "
                                  f"{headers_tip.height}")
                self.new_headers_queue.put_nowait(
                    HeaderSpan(is_reorg=False, start_header=blocks_tip, stop_header=headers_tip))
            self.sync_state.target_block_header_height = self.sync_state.get_local_tip_height()
            assert self.sync_state.target_header_height is not None
            while True:
                if not self.sync_state.local_tip_height < self.sync_state.target_header_height:
                    self.sync_state.headers_event_initial_sync.set()
                    await self.sync_state.headers_new_tip_queue.get()

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

    async def request_all_batch_block_data(self, _batch_id: int, blocks_batch_set: set[bytes],
            stop_header_height: int) -> None:
        """
        This method relies on the node responding to the prior getblocks request with up to 500
        inv messages.
        """
        assert self.bitcoin_p2p_client is not None
        count_pending = len(blocks_batch_set)
        count_requested = 0
        while True:
            inv = await self.sync_state.pending_blocks_inv_queue.get()
            # self.logger.debug(f"got block inv: {inv}, batch_id={_batch_id}")
            try:
                header = self.headers_threadsafe.get_header_for_hash(
                    hex_str_to_hash(inv.get("inv_hash")))
            except MissingHeader as e:
                if self.sync_state.pending_blocks_inv_queue.empty() and count_requested != 0:
                    self.logger.exception(f"An unsolicited block "
                        f"({hash_to_hex_str(inv['inv_hash'])}) was pushed to the "
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

            await self.bitcoin_p2p_client.send_message(self.serializer.getdata([inv]))

            count_requested += 1
            count_pending -= 1

            if count_pending == 0:
                break

        assert count_requested == len(blocks_batch_set)

    def backfill_headers(self, first_missing_header_height: int, reorg_tip_height: int) -> None:
        for height in range(first_missing_header_height, reorg_tip_height + 1):
            header = self.headers_threadsafe.get_header_for_height(height, lock=False)
            self.storage.block_headers.connect(header.raw)
        self.storage.block_headers.flush()

    async def connect_done_block_headers(self, blocks_batch_set: set[bytes]) -> None:
        """If this returns False, it means a reorg happened"""
        sorted_headers = sorted([(self.headers_threadsafe.get_header_for_hash(h).height, h) for h in
            blocks_batch_set])
        sorted_heights = [height for height, h in sorted_headers]
        # Assert the resultant blocks are consecutive with no gaps
        max_height = max(sorted_heights)
        expected_block_heights = [i+1 for i in range(max_height-len(sorted_heights), max_height)]
        assert sorted_heights == expected_block_heights

        block_headers: bitcoinx.Headers = self.storage.block_headers

        for height, _hash in sorted_headers:
            header = self.headers_threadsafe.get_header_for_hash(_hash)
            try:
                block_headers.connect(header.raw)
            except bitcoinx.errors.MissingHeader as e:
                # This should indicate a reorg but we will confirm it
                self.logger.error(e)

        self.storage.block_headers.flush()

        tip = self.sync_state.get_local_block_tip()
        self.logger.debug(f"Connected up to header height: {tip.height}, "
                          f"hash: {hash_to_hex_str(tip.hash)}")

    def get_hash_stop(self, stop_header_height: int) -> bytes:
        # We should only really request enough blocks at a time to keep our
        # recv buffer filling at the max rate. No point requesting 500 x 4GB blocks!!!
        with self.storage.headers_lock:
            node_longest_chain = self.storage.headers.longest_chain()
            if stop_header_height >= node_longest_chain.tip.height:
                hash_stop = ZERO_HASH
            else:
                stop_height = stop_header_height + 1
                header: bitcoinx.Header = self.storage.headers.header_at_height(node_longest_chain,
                    stop_height)
                hash_stop = header.hash
            return hash_stop

    async def wait_for_batched_blocks_completion(self, batch_id: int,
            all_pending_block_hashes: set[bytes], stop_header_height: int) -> None:
        """all_pending_block_hashes is copied into these threads to prevent mutation"""
        try:
            await self.request_all_batch_block_data(batch_id, all_pending_block_hashes.copy(),
                stop_header_height)

            self.blocks_batch_set_queue_mtree.put_nowait(all_pending_block_hashes.copy())
            self.blocks_batch_set_queue_raw.put_nowait(all_pending_block_hashes.copy())
            await self.sync_state.done_blocks_mtree_event.wait()
            await self.sync_state.done_blocks_raw_event.wait()
            self.sync_state.done_blocks_mtree_event.clear()
            self.sync_state.done_blocks_raw_event.clear()

            await self.enforce_lmdb_flush()  # Until this completes a crash leads to rollback
            await self.connect_done_block_headers(all_pending_block_hashes.copy())

        except Exception:
            self.logger.exception("Unexpected exception in 'wait_for_batched_blocks_completion' ")

    def merge_batch(self, batch: list[HeaderSpan]) -> HeaderSpan:
        """Take the broadest possible start & stop points. is_reorg=True
        merely serves to allow syncing blocks for height <
        current block store tip"""
        is_reorg = any([x.is_reorg for x in batch])
        start_header_height = min([x.start_header.height for x in batch])
        stop_header_height = max([x.stop_header.height for x in batch])

        # We only care about the longest chain headers at this point so
        # lookups by height is fine
        start_header = self.headers_threadsafe.get_header_for_height(start_header_height)
        stop_header = self.headers_threadsafe.get_header_for_height(stop_header_height)
        return HeaderSpan(is_reorg, start_header, stop_header)

    async def sync_blocks_batch(self, batch_id: int, start_header: Header, stop_header: Header) \
            -> None:
        assert self.bitcoin_p2p_client is not None
        original_stop_height = stop_header.height
        while True:
            first_locator = self.headers_threadsafe.get_header_for_height(start_header.height - 1)

            if batch_id == 0:
                self.logger.info(f"Starting Initial Block Download")
            else:
                self.logger.debug(f"Controller Batch {batch_id} Start")

            with self.storage.block_headers_lock:
                chain = self.storage.block_headers.longest_chain()

                block_locator_hashes = [first_locator.hash]
                hash_count = len(block_locator_hashes)

                # Allocate next batch of blocks
                # reassigns new global sync_state.blocks_batch_set
                if chain.tip.height > 2016:
                    await self.update_moving_average(chain.tip.height)

                from_height = self.headers_threadsafe.get_header_for_hash(
                    start_header.hash).height - 1
                to_height = stop_header.height

            next_batch = self.sync_state.get_next_batched_blocks(from_height, to_height)
            batch_count, all_pending_block_hashes, stop_header_height = next_batch
            hash_stop = self.get_hash_stop(stop_header_height)

            await self.bitcoin_p2p_client.send_message(
                self.serializer.getblocks(hash_count, block_locator_hashes, hash_stop))

            # Workers are loaded by Handlers.on_block handler as messages are received
            await self.wait_for_batched_blocks_completion(batch_id, all_pending_block_hashes,
                stop_header_height)

            # ------------------------- Batch complete ------------------------- #
            self.logger.debug(f"Controller Batch {batch_id} Complete."
                f" New tip height: {self.sync_state.get_local_block_tip_height()}")

            batch_id += 1
            if self.sync_state.get_local_block_tip_height() == original_stop_height:
                break
            else:
                start_header = self.headers_threadsafe.get_header_for_height(
                    start_header.height + batch_count)

    async def sync_all_blocks_job(self) -> None:
        """supervises completion of syncing all blocks to target height"""
        BATCH_INTERVAL = 0.5
        batch = []
        prev_time_check = time.time()
        try:
            # up to 500 blocks per loop
            batch_id = 0
            while True:
                # ------------------------- Batch Start ------------------------- #
                # If there's a reorg, starting header can be less than longest_chain().tip
                while True:
                    try:
                        # This batching mechanism is only really needed for the case where on
                        # RegTest if you generate a ton of blocks, the node will drip feed
                        # the p2p headers arrays with only a single header in each one.
                        msg = await asyncio.wait_for(self.new_headers_queue.get(), timeout=0.3)
                        batch.append(HeaderSpan(*msg))
                    except asyncio.exceptions.TimeoutError:
                        if len(batch) != 0:
                            time_diff = time.time() - prev_time_check
                            if time_diff > BATCH_INTERVAL:
                                break
                            else:
                                continue
                        else:
                            continue

                is_reorg, start_header, stop_header = self.merge_batch(batch)
                batch = []
                if stop_header.height <= self.sync_state.get_local_block_tip_height() and not is_reorg:
                    continue

                await self.sync_blocks_batch(batch_id, start_header, stop_header)
                batch_id += 1

        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger.exception("sync_blocks_job raised an exception")
            raise

    # -- Message Types -- #
    async def send_inv(self, inv_vects: list[Inv]) -> None:
        await self.serializer.inv(inv_vects)

    async def enforce_lmdb_flush(self) -> None:
        assert self.lmdb is not None
        t0 = time.perf_counter()
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self.general_executor, self.lmdb.sync)
        t1 = time.perf_counter() - t0
        self.logger.debug(f"Flush for batch took {t1} seconds")

    async def spawn_aiohttp_api(self) -> None:
        assert self.lmdb is not None
        self.tasks.append(create_task(server_main.main(self.lmdb, self.headers_threadsafe_blocks,
            self.net_config)))

    async def spawn_poll_node_for_header_job(self) -> None:
        self.tasks.append(create_task(self.regtest_support.regtest_poll_node_for_tip_job()))
