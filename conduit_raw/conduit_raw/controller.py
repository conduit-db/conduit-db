# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.
from io import BytesIO

import asyncio
from bitcoinx import hash_to_hex_str, Header
from conduit_p2p import BitcoinClientManager, BitcoinClientMode
from concurrent.futures.thread import ThreadPoolExecutor
import os
import queue
import threading
import time
from typing import Any, TypeVar
import multiprocessing
from multiprocessing.process import BaseProcess
from pathlib import Path
import logging
import zmq
from zmq.asyncio import Socket as AsyncZMQSocket

from conduit_lib import (
    setup_storage,
    IPCSocketClient,
    DBInterface,
)
from conduit_lib.constants import (
    CONDUIT_RAW_SERVICE_NAME,
    REGTEST,
    ZERO_HASH,
    MAX_BLOCK_PER_BATCH_COUNT_CONDUIT_RAW,
    TARGET_BYTES_BLOCK_BATCH_REQUEST_SIZE_CONDUIT_RAW,
)
from conduit_lib.controller_base import ControllerBase
from conduit_lib.utils import create_task, get_conduit_raw_host_and_port
from conduit_lib.wait_for_dependencies import wait_for_db, wait_for_conduit_index_to_catch_up
from conduit_lib.zmq_sockets import bind_async_zmq_socket
from conduit_p2p.client import get_max_headers, wait_for_new_tip_reorg_aware
from conduit_p2p.deserializer import Inv
from conduit_p2p.headers import NewTipResult
from conduit_p2p.types import InvType

from .aiohttp_api import server_main
from .batch_completion import BatchCompletionMtree
from .handlers import IndexerHandlers
from .regtest_support import RegtestSupport
from .ipc_sock_server import (
    ThreadedTCPServer,
    ThreadedTCPRequestHandler,
)
from .workers import MTreeCalculator

T2 = TypeVar("T2")

MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))


class Controller(ControllerBase):
    def __init__(self, loop_type: str | None = None) -> None:
        self.service_name = CONDUIT_RAW_SERVICE_NAME
        self.running = False
        self.processes: list[BaseProcess] = []
        self.tasks: list[asyncio.Task[Any]] = []
        self.logger = logging.getLogger("controller")
        self.loop = asyncio.get_event_loop()
        if loop_type == "uvloop":
            self.logger.debug(f"Using uvloop")
        elif not loop_type:
            self.logger.debug(f"Using default asyncio event loop")

        # Bitcoin network/peer net_config
        self.general_executor = ThreadPoolExecutor(max_workers=4)

        # ZMQ
        self.zmq_async_context = zmq.asyncio.Context.instance()
        self.socket_kill_workers = bind_async_zmq_socket(
            self.zmq_async_context, "tcp://127.0.0.1:46464", zmq.SocketType.PUB
        )
        self.merkle_tree_worker_sockets: dict[int, AsyncZMQSocket] = {}  # worker_id: zmq_socket
        self.WORKER_COUNT_MTREE_CALCULATORS = int(os.getenv("WORKER_COUNT_MTREE_CALCULATORS", "4"))
        self.bind_merkle_tree_worker_zmq_listeners()

        network_type = os.environ["NETWORK"]
        headers_dir = Path(os.getenv("DATADIR_SSD", str(MODULE_DIR.parent))) / "headers_conduit_raw"
        self.storage = setup_storage(network_type, headers_dir)
        self.headers_threadsafe = self.storage.headers
        self.headers_threadsafe_blocks = self.storage.block_headers

        message_handler = IndexerHandlers(network_type, self)
        peers_list = [f"{os.getenv('NODE_HOST')}:{os.getenv('NODE_PORT')}"]
        self.peer_manager = BitcoinClientManager(message_handler, peers_list,
                mode=BitcoinClientMode.HIGH_LEVEL, relay_transactions=False,
                use_persisted_peers=True, last_known_height=self.headers_threadsafe_blocks.tip().height,
                concurrency=1)
        self.lmdb = self.storage.lmdb
        self.ipc_sock_client: IPCSocketClient | None = None
        self.ipc_sock_server: ThreadedTCPServer | None = None

        # Worker queues & events
        self.worker_in_queue_preproc: queue.Queue[
            tuple[bytes, int, int, int]
        ] = queue.Queue()  # no ack needed
        self.worker_ack_queue_mtree: 'multiprocessing.Queue[bytes]' = (
            multiprocessing.Queue()
        )  # pylint: disable=E1136
        self.blocks_batch_set_queue_mtree = queue.Queue[set[bytes]]()

        self.estimated_moving_av_block_size_mb = (
            0.1 if self.headers_threadsafe_blocks.tip().height < 2016 else 500
        )
        self.new_headers_queue: asyncio.Queue[NewTipResult] = asyncio.Queue()
        self.new_tip_event = threading.Event()

        # Done blocks Sets
        self.done_blocks_mtree_event: asyncio.Event = asyncio.Event()

    def bind_merkle_tree_worker_zmq_listeners(self) -> None:
        # The Merkle Tree worker needs individual ZMQ sockets so we can ensure that
        # subsequent chunks for a large block go to the same worker for processing
        BASE_PORT_NUM = 41830
        for worker_id in range(1, self.WORKER_COUNT_MTREE_CALCULATORS + 1):
            uri = f"tcp://127.0.0.1:{BASE_PORT_NUM + worker_id}"
            self.logger.debug(f"Binding ZMQ merkle tree worker listener on: {uri}")
            zmq_socket = bind_async_zmq_socket(self.zmq_async_context, uri, zmq.SocketType.PUSH)
            self.merkle_tree_worker_sockets[worker_id] = zmq_socket

    async def setup(self) -> None:
        if self.peer_manager.net_config.NET == REGTEST:
            self.regtest_support = RegtestSupport(self)
        self.batch_completion_mtree = BatchCompletionMtree(
            self,
            self.worker_ack_queue_mtree,
            self.blocks_batch_set_queue_mtree,
        )
        self.batch_completion_mtree.start()

    async def run(self) -> None:
        assert self.peer_manager is not None

        self.running = True
        await self.setup()
        wait_for_db()
        await self.peer_manager.connect_all_peers(wait_for_n_peers=1)

        await self.database_integrity_check()

        thread = threading.Thread(target=self.ipc_sock_server_thread, daemon=True)
        thread.start()

        assert self.lmdb is not None
        external_api = server_main.main(self.lmdb, self.headers_threadsafe_blocks, self.peer_manager.net_config)
        self.tasks.append(create_task(external_api))
        self.start_worker_processes()

        # In regtest old block timestamps (submitted to node) will not take the node out of IBD
        # This works around it by polling the node's RPC. This is never needed in prod.
        if self.peer_manager.net_config.NET == REGTEST:
            self.tasks.append(create_task(self.regtest_support.regtest_poll_node_for_tip_job()))

        self.tasks.append(create_task(self.headers_sync_task()))
        self.tasks.append(create_task(self.blocks_sync_task()))

        await self.peer_manager.listen()

    async def stop(self) -> None:
        self.running = False
        if self.peer_manager:
            await self.peer_manager.close()
        if self.storage:
            await self.storage.close()
        if self.regtest_support:
            await self.regtest_support.close()

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

    def start_worker_processes(self) -> None:
        for i in range(self.WORKER_COUNT_MTREE_CALCULATORS):
            worker_id = i + 1
            mtree_proc = MTreeCalculator(worker_id, self.worker_ack_queue_mtree)
            mtree_proc.start()
            self.processes.append(mtree_proc)

    def ipc_sock_server_thread(self) -> None:
        assert self.lmdb is not None
        host, port = get_conduit_raw_host_and_port()
        self.ipc_sock_server = ThreadedTCPServer(
            addr=(host, port),
            handler=ThreadedTCPRequestHandler,
            headers_threadsafe_blocks=self.headers_threadsafe_blocks,
            lmdb=self.lmdb,
            new_tip_event=self.new_tip_event,
        )
        self.ipc_sock_server.serve_forever()

    async def database_integrity_check(self) -> None:
        """Check the last flushed block"""
        assert self.lmdb is not None
        tip_height = self.headers_threadsafe_blocks.tip().height
        height = tip_height
        header = self.headers_threadsafe.get_header_for_height(height)
        if header.hex_str() == self.peer_manager.net_config.GENESIS_BLOCK_HASH:
            return None
        # Both ConduitRaw and ConduitIndex need to be run with this set to '1'
        # to do the recovery procedure
        if os.getenv('FORCE_REINDEX_RECENT_BLOCKS', '0') == '1':
            self.logger.debug(f"Received a forced reindex trigger. Current tip height: {tip_height}")
            # Explanation
            # -----------
            # Because blocks are received from the p2p network concurrently (and out of order)
            # it's not safe to backtrack 1 block at a time. We need to go all the way back
            # to a safe point before the corruption happened.
            #
            # The reason this needs to be manually triggered is because ConduitIndex has a
            # mapping of each transaction hash -> (block_num, tx_pos). This mapping will be
            # broken when ConduitRaw assigns new unique block_nums to each raw block.
            # It all needs to be re-indexed on both services to get the correct block_num mappings.
            #
            # The largest possible batch size is 500 blocks at a time before being committed
            # and check-pointed. If the corruption goes back deeper than this then it's probably
            # too severe to be worth even trying to recover from.
            for i in range(MAX_BLOCK_PER_BATCH_COUNT_CONDUIT_RAW):
                self.lmdb.try_purge_block_data(header.hash, height)
                height -= 1
                if height <= 1:
                    height = 0
                    break
        try:
            header = self.headers_threadsafe.get_header_for_height(height)
            if header.hex_str() == self.peer_manager.net_config.GENESIS_BLOCK_HASH:
                return None
            self.logger.info(f"Checking database integrity...")
            self.lmdb.check_block(header.hash)
            self.logger.info(f"Database integrity check passed.")

            # Some blocks failed integrity checks
            # Re-synchronizing them will overwrite the old records
            # There is no strict guarantee that the block num for these
            # blocks will not change which could invalidate ConduitIndex
            # records.
            if height < tip_height:
                self.logger.debug(f"Re-synchronising blocks from: {height} to {tip_height}")
                last_known_header = self.headers_threadsafe.get_header_for_height(height)
                stop_header = self.headers_threadsafe.get_header_for_height(tip_height)
                self.new_headers_queue.put_nowait(
                    NewTipResult(
                        is_reorg=False,
                        last_known_header=last_known_header,
                        stop_header=stop_header,
                        old_chain=None,
                        new_chain=None,
                        reorg_info=None
                    )
                )
            return None
        except AssertionError:
            self.logger.error(
                f"Block {hash_to_hex_str(header.hash)} failed integrity check. You now need to "
                f"run both ConduitRaw and ConduitIndex with FORCE_REINDEX_RECENT_BLOCKS=1"
            )
            await self.stop()

    async def headers_sync_task(self) -> None:
        # No need for reorg-awareness
        client = await self.peer_manager.get_next_available_peer()
        last_known_header = self.headers_threadsafe.tip()
        while self.headers_threadsafe.tip().height < client.remote_start_height:
            message = await get_max_headers(client, self.headers_threadsafe)
            if message:
                self.headers_threadsafe.connect_headers(BytesIO(message))
                self.logger.info(f"New headers tip height: {self.headers_threadsafe.tip().height} "
                            f"(peer={client.id})")
            else:
                self.logger.debug(f"No headers returned (peer_id={client.id})")

        new_tip = NewTipResult(is_reorg=False, last_known_header=last_known_header,
            stop_header=self.headers_threadsafe.tip(), old_chain=None, new_chain=None, reorg_info=None)
        self.logger.info(f"Finished initial headers download, height: {new_tip.stop_header.height}")
        await self.new_headers_queue.put(new_tip)

        # Reorg-awareness necessary now
        async for new_tip in wait_for_new_tip_reorg_aware(client, self.headers_threadsafe):
            if not new_tip.is_reorg:
                self.logger.info(f"New headers tip height: %s (peer={client.id})", new_tip.stop_header.height)
            else:
                assert new_tip.reorg_info is not None
                self.logger.debug(f"Reorg detected. Common parent height: {new_tip.reorg_info.commmon_height}. "
                             f"Old tip:{new_tip.reorg_info.old_tip}. "
                             f"New tip: {new_tip.reorg_info.new_tip}")
            await self.new_headers_queue.put(new_tip)

    async def wait_for_batched_blocks_completion(
        self,
        wanted_blocks: set[bytes],
    ) -> None:
        self.blocks_batch_set_queue_mtree.put_nowait(wanted_blocks.copy())
        await self.done_blocks_mtree_event.wait()
        self.done_blocks_mtree_event.clear()

        await self.enforce_lmdb_flush()  # Until this completes a crash leads to rollback

    async def get_wanted_block_data(self, wanted: list[Header], client_manager: BitcoinClientManager,
            start_locator_hash: bytes, stop_locator_hash: bytes) -> None:
        """If a BitcoinClient disconnects at any time, the queued work for that client will be re-allocated"""

        # Get an inventory of which peers have which blocks available
        clients = client_manager.get_connected_peers()
        for client in clients:
            message = client.serializer.getblocks(len([start_locator_hash]),
                [start_locator_hash], stop_locator_hash)
            client_manager.queue_message_for_peer(message, client)

        for header in wanted:
            not_tried_yet_count = len(client_manager.get_connected_peers())
            while True:
                if len(client_manager.get_connected_peers()) < 1:
                    self.logger.debug(f"No available connected peers. Waiting")
                    await asyncio.sleep(10)
                    continue

                client = await client_manager.get_next_available_peer()
                if header.hash in client.have_blocks and header.hash in client_manager.wanted_blocks:
                    self.logger.debug(f"Sending getdata for block_hash: {header.hex_str()}, "
                                      f"block height: {header.height} (peer_id={client.id})")
                    message = client.serializer.getdata(
                        [Inv(inv_type=InvType.BLOCK, inv_hash=header.hex_str())])
                    client.send_message(message)
                    break
                else:
                    self.logger.debug(f"Block {header.height} not found in peer: {client.id}")
                    not_tried_yet_count -= 1
                    if not_tried_yet_count == 0:
                        # When the blocks start getting bigger, it's probably
                        # worth sacrificing on a longer sleep here just so that
                        # all connected peers get sufficient time to respond with
                        # their inventory of available blocks.
                        await asyncio.sleep(0.5)
                        # Hopefully new connected peers which will immediately
                        # send a getblocks request to populate the `client.have_blocks`
                        not_tried_yet_count = len(client_manager.get_connected_peers())

    async def blocks_sync_task(self) -> None:
        self.logger.info(f"Starting from tip height: {self.headers_threadsafe_blocks.tip().height}")
        while True:
            if self.headers_threadsafe_blocks.tip().height == self.headers_threadsafe.tip().height:
                new_tip: NewTipResult = await self.new_headers_queue.get()
                # Two new chain tips in quick secession can do this
                if self.headers_threadsafe_blocks.have_header(new_tip.stop_header.hash):
                    continue

                # Start & Stop locator block hashes (can start below local tip for a reorg)
                start_locator_hash = new_tip.last_known_header.hash
                stop_locator_hash = ZERO_HASH
                stop_height = new_tip.stop_header.height
                last_known_height = new_tip.last_known_header.height
            else:
                # This probably looks over-complicated but during IBD the headers tip will
                # be way ahead of the indexed block tip, so we need a way of iterating
                # our way to the tip independent of new_headers_queue events.
                done_blocks_tip = self.headers_threadsafe_blocks.tip()
                remainder = self.headers_threadsafe.tip().height - done_blocks_tip.height
                last_known_height = done_blocks_tip.height
                start_locator_hash = self.headers_threadsafe.get_header_for_height(last_known_height).hash

                if done_blocks_tip.height > 2016:
                    await self.update_moving_average(last_known_height)
                ideal_count = self.get_ideal_block_batch_count(
                    TARGET_BYTES_BLOCK_BATCH_REQUEST_SIZE_CONDUIT_RAW,
                    self.service_name, done_blocks_tip.height)
                allocation_count = min(ideal_count, remainder)
                stop_height = done_blocks_tip.height + allocation_count

                if stop_height < self.headers_threadsafe.tip().height:
                    stop_locator_hash = self.headers_threadsafe.get_header_for_height(stop_height + 1).hash
                else:
                    stop_locator_hash = ZERO_HASH

            first_wanted_height = last_known_height + 1
            wanted: list[Header] = [self.headers_threadsafe.get_header_for_height(height) for height in
                range(first_wanted_height, stop_height + 1)]

            # This allows calling getblocks for newly joined or reconnecting peers
            self.peer_manager.wanted_blocks = set(x.hash for x in wanted)
            self.peer_manager.wanted_block_first = wanted[0]
            self.peer_manager.wanted_block_last = wanted[-1]
            await self.get_wanted_block_data(wanted, self.peer_manager, start_locator_hash, stop_locator_hash)

            # The raw block data will flow asynchronously into the on_block & on_block_chunk handlers now

            await self.wait_for_batched_blocks_completion(self.peer_manager.wanted_blocks)
            while len(self.peer_manager.wanted_blocks) > 0:
                self.logger.debug(f"Waiting for {len(self.peer_manager.wanted_blocks)} blocks to be processed...")
                self.logger.info(f"Connected peers: {len(self.peer_manager.get_connected_peers())}")
                self.logger.info(f"Disconnected peers: {len(self.peer_manager._disconnected_pool)}")
                await asyncio.sleep(1)

            # Connect to done block headers store only when all of them are complete
            for header in wanted:
                self.headers_threadsafe_blocks.headers.connect(header.raw, check_work=False)
            self.headers_threadsafe_blocks.write_cached_headers()

            self.logger.info(f"New tip height: {self.headers_threadsafe_blocks.tip().height}")
            self.peer_manager.wanted_blocks = set()
            self.new_tip_event.set()

            if os.environ['PRUNE_MODE'] == "1":
                if not self.storage.db:
                    self.storage.db = DBInterface.load_db("main-process")
                await wait_for_conduit_index_to_catch_up(
                    self.storage.db, self.headers_threadsafe_blocks.tip().height
                )

    async def enforce_lmdb_flush(self) -> None:
        assert self.lmdb is not None
        t0 = time.perf_counter()
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self.general_executor, self.lmdb.sync)
        t1 = time.perf_counter() - t0
        self.logger.debug(f"Flush for batch took {t1} seconds")
