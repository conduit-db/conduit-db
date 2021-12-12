import sys
import typing
from asyncio import BufferedProtocol
from io import BytesIO

import bitcoinx
import cbor2
from bitcoinx import hash_to_hex_str, double_sha256, MissingHeader, Header
import logging
import os
import struct
import time
from typing import Optional, Dict, List, Tuple, Set
from concurrent.futures.thread import ThreadPoolExecutor
import multiprocessing
from pathlib import Path
import zmq
from zmq.asyncio import Context as AsyncZMQContext
import asyncio

from conduit_lib.bitcoin_net_io import BitcoinNetIO
from conduit_lib.commands import BLOCK_BIN, MEMPOOL, VERSION, PING
from conduit_lib.ipc_sock_client import IPCSocketClient
from conduit_lib.database.mysql.mysql_database import MySQLDatabase, load_mysql_database
from conduit_lib.deserializer import Deserializer
from conduit_lib.handlers import Handlers
from conduit_lib.ipc_sock_msg_types import HeadersBatchedResponse, ReorgDifferentialResponse
from conduit_lib.serializer import Serializer
from conduit_lib.store import setup_storage
from conduit_lib.constants import WORKER_COUNT_TX_PARSERS, MsgType, NULL_HASH, \
    MAIN_BATCH_HEADERS_COUNT_LIMIT, CONDUIT_INDEX_SERVICE_NAME
from conduit_lib.logging_server import TCPLoggingServer
from conduit_lib.types import BlockHeaderRow, ChainHashes
from conduit_lib.utils import connect_headers, headers_to_p2p_struct, get_header_for_height, \
    connect_headers_reorg_safe, get_header_for_hash
from conduit_lib.wait_for_dependencies import wait_for_mysql, wait_for_conduit_raw_api, \
    wait_for_node
from contrib.scripts.export_blocks import GENESIS_HASH_HEX

from .sync_state import SyncState
from .types import WorkUnit
from .workers.transaction_parser import TxParser


MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
CONDUIT_RAW_API_HOST: str = os.environ.get('CONDUIT_RAW_API_HOST', 'localhost:50000')

if typing.TYPE_CHECKING:
    from conduit_lib.networks import NetworkConfig
    from conduit_lib.peers import Peer


class Controller:
    """Designed to sync the blockchain as fast as possible.

    Coordinates:
    - network via BitcoinNetIO
    - outsources parsing of block data to workers in workers.py (which work in parallel reading
    from a shared memory buffer)
    - synchronizes the refreshing of the shared memory buffer which holds multiple raw blocks)
    """

    def __init__(self, config: Dict, net_config: 'NetworkConfig', host="127.0.0.1", port=8000,
            logging_server_proc: TCPLoggingServer=None, loop_type=None):
        self.service_name = CONDUIT_INDEX_SERVICE_NAME
        self.running = False
        self.logging_server_proc = logging_server_proc
        self.processes = [self.logging_server_proc]
        # self.processes = []
        self.tasks = []
        self.logger = logging.getLogger("controller")
        self.loop = asyncio.get_event_loop()

        if loop_type == 'uvloop':
            self.logger.debug(f"Using uvloop")
        elif not loop_type:
            self.logger.debug(f"Using default asyncio event loop")

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
        self.protocol: Optional[BufferedProtocol] = None
        self.protocol_factory = None

        # Bitcoin Network IO + callbacks
        self.transport = None
        self.bitcoin_net_io = BitcoinNetIO(self.on_buffer_full, self.on_msg,
            self.on_connection_made, self.on_connection_lost)
        self.shm_buffer_view = self.bitcoin_net_io.shm_buffer_view
        self.shm_buffer = self.bitcoin_net_io.shm_buffer

        # Mempool and API state
        self.mempool_tx_hash_set = set()

        # Connection entry/exit
        self.handshake_complete_event = asyncio.Event()
        self.con_lost_event = asyncio.Event()

        # IPC from Controller to TxParser
        context1 = AsyncZMQContext.instance()
        self.mined_tx_socket = context1.socket(zmq.PUSH)
        self.mined_tx_socket.bind("tcp://127.0.0.1:55555")

        # IPC from Controller to TxParser
        context2 = AsyncZMQContext.instance()
        self.mempool_tx_socket: zmq.Socket = context2.socket(zmq.PUSH)
        self.mempool_tx_socket.bind("tcp://127.0.0.1:55556")

        # PUB-SUB from Controller to worker to kill the worker
        context3 = AsyncZMQContext.instance()
        self.kill_worker_socket = context3.socket(zmq.PUB)
        self.kill_worker_socket.bind("tcp://127.0.0.1:63241")

        # PUB-SUB from Controller to worker to signal when initial block download is in effect
        # Defined as when the local chain tip is within 24 hours of the best known chain tip
        # This is the same definition that the reference bitcoin-sv node uses.
        context4 = AsyncZMQContext.instance()
        self.is_ibd_socket = context4.socket(zmq.PUB)
        self.is_ibd_socket.bind("tcp://127.0.0.1:52841")
        self.ibd_signal_sent = False

        context5 = AsyncZMQContext.instance()
        self.ack_for_mined_tx_socket = context5.socket(zmq.PULL)
        self.ack_for_mined_tx_socket.bind("tcp://127.0.0.1:55889")

        context6 = AsyncZMQContext.instance()
        self.reorg_event_socket: zmq.asyncio.Socket = context6.socket(zmq.PUSH)
        self.reorg_event_socket.connect("tcp://127.0.0.1:51495")

        self.worker_ack_queue_tx_parse_confirmed = multiprocessing.Queue()  # blk_hash:tx_count

        # Batch Completion
        self.tx_parser_completion_queue = asyncio.Queue()

        self.global_tx_hashes_dict = {}  # blk_num:tx_hashes
        # self.worker_ack_queue_tx_parse_mempool = multiprocessing.Queue()  # tx_count

        # Database Interfaces
        self.mysql_db: Optional[MySQLDatabase] = None
        self.ipc_sock_client: Optional[IPCSocketClient] = None
        self.total_time_connecting_headers = 0
        self.sync_state: Optional[SyncState] = None
        self.general_executor = ThreadPoolExecutor(max_workers=1)

    async def setup(self):
        headers_dir = MODULE_DIR.parent
        self.storage = setup_storage(self.config, self.net_config, headers_dir)
        self.handlers: Handlers = Handlers(self, self.net_config, self.storage)
        self.serializer = Serializer(self.net_config, self.storage)
        self.deserializer = Deserializer(self.net_config, self.storage)
        self.sync_state = SyncState(self.storage, self)
        self.mysql_db = load_mysql_database()
        self.ipc_sock_client = IPCSocketClient()

        # Drop mempool table for now and re-fill - easiest brute force way to achieve consistency
        self.mysql_db.tables.mysql_drop_mempool_table()
        self.mysql_db.tables.mysql_create_permanent_tables()

    async def run(self):
        self.running = True
        try:
            await wait_for_conduit_raw_api(conduit_raw_api_host=CONDUIT_RAW_API_HOST)
            await wait_for_mysql()
            await self.setup()
            self.tasks.append(asyncio.create_task(self.start_jobs()))
            while True:
                await asyncio.sleep(5)

        except Exception:
            self.logger.exception("unexpected exception in Controller.run")
        finally:
            await self.stop()

    async def maintain_node_connection(self):
        first_loop = True
        while True:
            await wait_for_node(node_host=self.config['node_host'],
                serializer=self.serializer, deserializer=self.deserializer)
            if not first_loop:
                self.logger.debug(f"Bitcoin daemon disconnected. "
                                  f"Reconnecting & Re-requesting mempool (as appropriate)...")
            await self.connect_session()  # on_connection_made callback -> starts jobs
            await self.send_version(self.peer.host, self.peer.port, self.host, self.port)
            await self.handshake_complete_event.wait()
            await asyncio.sleep(2)
            if self.ibd_signal_sent:
                await self.request_mempool()

            await self.con_lost_event.wait()
            self.con_lost_event.clear()
            first_loop = False

    async def stop(self):
        self.running = False
        try:
            if self.transport:
                self.transport.close()
            if self.storage:
                await self.storage.close()

            with self.kill_worker_socket as sock:
                await sock.send(b"stop_signal")

            await asyncio.sleep(1)

            for p in self.processes:
                p.terminate()
                p.join()

            self.sync_state._batched_blocks_exec.shutdown(wait=False)

            self.shm_buffer.close()
            self.shm_buffer.unlink()
            self.mempool_tx_socket.close()
            self.is_ibd_socket.close()

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

    async def _on_buffer_full(self):
        """Waits until all mempool txs in the shared memory buffer have been processed before
        resuming (and therefore resetting the buffer) BitcoinNetIO."""
        while not self.sync_state.have_processed_all_msgs_in_buffer():
            await asyncio.sleep(0.05)

        self.sync_state.reset_msg_counts()
        self.bitcoin_net_io.resume()

    def on_msg(self, command: bytes, message: memoryview):
        if command != BLOCK_BIN:
            self.sync_state.incr_msg_received_count()
        if command == MEMPOOL:
            self.logger.debug(f"putting mempool tx to queue: "
                f"{bitcoinx.hash_to_hex_str(bitcoinx.Tx.from_bytes(message).hash())}")
        self.sync_state.incoming_msg_queue.put_nowait((command, message))

    def on_connection_made(self):
        pass

    def on_connection_lost(self):
        self.con_lost_event.set()

    # ---------- end callbacks ---------- #

    async def connect_session(self):
        loop = asyncio.get_event_loop()
        peer = self.get_peer()
        self.logger.debug("connecting to (%s, %s) [%s]", peer.host, peer.port, self.net_config.NET)
        protocol_factory = lambda: self.bitcoin_net_io
        self.transport, self.session = await loop.create_connection(
            protocol_factory, peer.host, peer.port
        )
        return self.transport, self.session

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
                self.logger.exception("unexpected exception in handle()")
                raise

    async def spawn_handler_tasks(self):
        """spawn 4 tasks so that if one handler is waiting on an event that depends on
        another handler, progress will continue to be made."""
        for i in range(4):
            self.tasks.append(asyncio.create_task(self.handle()))

    async def send_request(self, command_name: str, message: bytes):
        self.bitcoin_net_io.send_message(message)

    # Multiprocessing Workers
    def start_workers(self):
        for worker_id in range(WORKER_COUNT_TX_PARSERS):
            p = TxParser(
                worker_id+1,
                self.worker_ack_queue_tx_parse_confirmed,
            )
            p.start()
            self.processes.append(p)
        # Allow time for TxParsers to bind to the zmq socket to distribute load evenly
        time.sleep(2)

    async def spawn_sync_all_blocks_job(self):
        """runs once at startup and is re-spawned for new unsolicited block tips"""
        self.tasks.append(asyncio.create_task(self.sync_all_blocks_job()))

    def maybe_rollback(self):
        """This will have to do a full table scan to find all the transactions with height above
        what was safetly flushed to disc... It will not be fast at scan but is better than
        re-syncing the whole chain. It can be thought of as a last resort db repair process."""
        # Safe block tip height
        last_good_block_tip = self.sync_state.get_local_block_tip()
        if last_good_block_tip.height == 0:
            last_good_block_tip_num = 0
        else:
            last_good_block_tip_num = self.ipc_sock_client.block_number_batched([last_good_block_tip.hash]).block_numbers[0]
            self.logger.debug(f"last_good_block_tip_num={last_good_block_tip_num}")

        # Get max allocated tx block num
        max_allocated_block_num = self.mysql_db.queries.mysql_get_max_allocated_block_num()
        if not max_allocated_block_num:
            return
        self.logger.debug(f"max_allocated_block_num={max_allocated_block_num}")
        if max_allocated_block_num == last_good_block_tip_num:
            return

        # Check for any "Unsafe" flushed txs (above the safe tip height)
        unsafe_tx_rows = self.mysql_db.queries.mysql_get_txids_above_last_good_block_num(
            last_good_block_tip_num)

        # Do rollback
        self.mysql_db.queries.mysql_rollback_unsafe_txs(unsafe_tx_rows)

    async def request_mempool(self):
        # NOTE: if the -rejectmempoolrequest=0 option is not set on the node, the node disconnects
        self.logger.debug(f"Requesting mempool...")
        await self.send_request(MEMPOOL, self.serializer.mempool())

    async def start_jobs(self):
        try:
            self.mysql_db: MySQLDatabase = self.storage.mysql_database
            await self.spawn_handler_tasks()
            self.maybe_rollback()
            self.start_workers()
            await self.spawn_sync_all_blocks_job()
            await self.spawn_batch_completion_job()
        except Exception as e:
            self.logger.exception("unexpected exception in start jobs")
            raise

    async def spawn_batch_completion_job(self):
        asyncio.create_task(self.batch_completion_job())

    def get_header_for_height(self, height: int) -> bitcoinx.Header:
        return get_header_for_height(height, self.storage.headers, self.storage.headers_lock)

    def all_blocks_processed(self, global_tx_hashes_dict: dict[int, list[bytes]]):
        expected_block_hashes = list(self.sync_state.expected_blocks_tx_counts.keys())
        expected_block_nums = self.ipc_sock_client.block_number_batched(expected_block_hashes).block_numbers
        block_hash_to_num_map = dict(zip(expected_block_hashes, expected_block_nums))

        for block_hash in expected_block_hashes:
            block_num = block_hash_to_num_map[block_hash]
            processed_tx_count = len(global_tx_hashes_dict.get(block_num, 0))
            expected_tx_count = self.sync_state.expected_blocks_tx_counts[block_hash]
            if expected_tx_count != processed_tx_count:
                return False
        else:
            return True

    async def update_mempool_and_api_tip_atomic(self, api_block_tip_height: int,
            api_block_tip_hash: bytes, is_reorg: bool) -> None:
        # Get all tx_hashes up to api_block_tip_height
        # Must ATOMICALLY:
        #   1) invalidate mempool rows (that have been mined)
        #   2) update api chain tip

        # 1) Drain queue & Update dictionary of block_num -> tx hashes processed
        new_mined_tx_hashes = {}
        all_blocks_processed = False
        while not all_blocks_processed:
            # Todo - in theory this ack_for_mined_tx_socket buffer could overflow as it waits
            #  until the batch is done... may need a background task to store the acks
            message = await self.ack_for_mined_tx_socket.recv()
            new_mined_tx_hashes = cbor2.loads(message)

            for blk_num, new_hashes in new_mined_tx_hashes.items():
                if not self.global_tx_hashes_dict.get(blk_num):
                    self.global_tx_hashes_dict[blk_num] = []
                self.global_tx_hashes_dict[blk_num].extend(new_hashes)
            if len(self.global_tx_hashes_dict) == len(self.sync_state.expected_blocks_tx_counts):
                t0 = time.time()
                all_blocks_processed = self.all_blocks_processed(self.global_tx_hashes_dict)
                t_diff = time.time() - t0

        # 2) Transfer from dict -> Temp Table for Table join with Mempool table
        mined_tx_hashes = []
        for blk_num, new_mined_tx_hashes in self.global_tx_hashes_dict.items():
            for tx_hash in new_mined_tx_hashes:
                mined_tx_hashes.append((tx_hash.hex(), blk_num))

        self.mysql_db.mysql_load_temp_mined_tx_hashes(mined_tx_hashes=mined_tx_hashes)
        self.global_tx_hashes_dict = {}

        # 3) Invalidate mempool rows that have been mined ATOMICALLY

        # If there is a reorg, it is not as simple as just deleting the mined txs
        # we must both add and remove txs based on the differential between the old and new chain
        if is_reorg:
            self._apply_reorg_diff_to_mempool(api_block_tip_height, api_block_tip_hash)
        else:
            self._invalidate_mempool_rows(api_block_tip_height, api_block_tip_hash)

    async def sanity_checks_and_update_api_tip(self, is_reorg: bool):
        t0 = time.time()
        api_block_tip_height = self.sync_state.get_local_block_tip_height()
        api_block_tip = self.get_header_for_height(api_block_tip_height)
        api_block_tip_hash = api_block_tip.hash

        # Update API tip for filtering of queries in the internal aiohttp API
        conduit_best_tip = await self.sync_state.get_conduit_best_tip()
        if await self.sync_state.is_ibd(api_block_tip, conduit_best_tip):
            await self.update_mempool_and_api_tip_atomic(api_block_tip_height, api_block_tip_hash,
                is_reorg)

        else:
            # No txs in mempool until is_ibd == True
            self.mysql_db.mysql_update_api_tip_height_and_hash(api_block_tip_height,
                api_block_tip_hash)
        t_diff = time.time() - t0
        self.logger.debug(f"Sanity checks took: {t_diff} seconds")
        return api_block_tip_height

    async def connect_done_block_headers(self, blocks_batch_set: set):
        ipc_socket_client = IPCSocketClient()
        t0 = time.perf_counter()
        unsorted_headers = [self.storage.get_header_for_hash(h) for h in blocks_batch_set]
        sorted_headers = sorted(unsorted_headers, key=lambda x: x.height)
        sorted_heights = [h.height for h in sorted_headers]

        # Assert the resultant blocks are consecutive with no gaps
        max_height = max(sorted_heights)
        # self.logger.debug(f"block_heights={block_heights}")

        # Check for gaps
        expected_block_heights = [i+1 for i in range(max_height-len(sorted_heights), max_height)]
        # self.logger.debug(f"expected_block_heights={expected_block_heights}")
        assert sorted_heights == expected_block_heights

        block_headers: bitcoinx.Headers = self.storage.block_headers
        for header in sorted_headers:
            block_headers.connect(header.raw)

        # self.mysql_db.bulk_loads.set_rocks_db_bulk_load_off()
        self.storage.block_headers.flush()

        # Get Tx Counts & Block Sizes for the headers table
        sorted_hashes = [h.hash for h in sorted_headers]
        block_numbers = self.ipc_sock_client.block_number_batched(sorted_hashes).block_numbers
        block_hash_to_num_map = dict(zip(sorted_hashes, block_numbers))
        sorted_block_metadata_batch = ipc_socket_client.block_metadata_batched(sorted_hashes).block_metadata_batch

        header_rows: list[BlockHeaderRow] = []
        for header, block_metadata in zip(sorted_headers, sorted_block_metadata_batch):
            blk_num = block_hash_to_num_map[header.hash]
            row = BlockHeaderRow(blk_num, header.hash.hex(), header.height, header.raw.hex(),
                block_metadata.tx_count, block_metadata.block_size, is_orphaned=0)
            header_rows.append(row)
        self.mysql_db.bulk_loads.mysql_bulk_load_headers(header_rows)

        # ? Add reorg and other sanity checks later here...

        tip = self.sync_state.get_local_block_tip()
        self.logger.debug(f"Connected up to header.hash, header.height = "
                          f"{tip.height, hash_to_hex_str(tip.hash)}")
        t1 = time.perf_counter() - t0
        self.total_time_connecting_headers += t1
        self.logger.debug(f"Total time connecting headers: {self.total_time_connecting_headers}")

    def connect_conduit_headers(self, raw_header: bytes) -> bool:
        """Two mmap files - one for "headers-first download" and the other for the
        blocks we then download."""
        try:
            self.storage.headers.connect(raw_header)
            self.storage.headers.flush()
            return True
        except MissingHeader as e:
            if str(e).find(GENESIS_HASH_HEX) != -1 or str(e).find(NULL_HASH) != -1:
                self.logger.debug("skipping - prev_out == genesis block")
                return True
            else:
                self.logger.exception(e)
                raise

    async def check_for_ibd_status(self, conduit_best_tip):
        with self.storage.headers_lock:
            local_tip = self.storage.headers.longest_chain().tip
        if await self.sync_state.is_ibd(local_tip, conduit_best_tip=conduit_best_tip) \
                and not self.ibd_signal_sent:
            self.logger.debug(f"Initial block download mode completed. "
                f"Activating mempool tx processing...")
            await self.is_ibd_socket.send(b"is_ibd_signal")
            self.ibd_signal_sent = True
            asyncio.create_task(self.maintain_node_connection())
            await self.handshake_complete_event.wait()
            await self.request_mempool()

    async def long_poll_conduit_raw_chain_tip(self) -> Tuple[bool, Header, Header,
            Optional[ChainHashes], Optional[ChainHashes]]:
        OVERKILL_REORG_DEPTH = 500  # Virtually zero chance of a reorg more deep than this.
        self.handlers: Handlers

        old_hashes = None
        new_hashes = None
        ipc_sock_client = None
        while True:
            is_reorg = False
            try:
                start_height = self.sync_state.get_local_block_tip_height() + 1

                # Long-polling
                ipc_sock_client = await self.loop.run_in_executor(self.general_executor, IPCSocketClient)
                result: HeadersBatchedResponse = await self.loop.run_in_executor(self.general_executor,
                    ipc_sock_client.headers_batched, start_height, MAIN_BATCH_HEADERS_COUNT_LIMIT)

                headers_p2p_msg = headers_to_p2p_struct(result.headers_batch)
                first_header_of_batch, success = connect_headers(BytesIO(headers_p2p_msg), self.storage.headers)
                if success:
                    start_header = get_header_for_hash(double_sha256(first_header_of_batch),
                        self.storage.headers, lock=None)
                    stop_header = self.sync_state.get_local_tip()
                else:
                    self.logger.debug(f"Potential reorg detected")
                    # This should mean there has been a reorg. The tip should always connect
                    from_height = max(start_height - OVERKILL_REORG_DEPTH, 1)
                    count = MAIN_BATCH_HEADERS_COUNT_LIMIT + OVERKILL_REORG_DEPTH
                    result: HeadersBatchedResponse = await self.loop.run_in_executor(
                        self.general_executor, ipc_sock_client.headers_batched, from_height, count)

                    # Try again
                    headers_p2p_msg = headers_to_p2p_struct(result.headers_batch)
                    is_reorg, start_header, stop_header, old_hashes, new_hashes = \
                        connect_headers_reorg_safe(headers_p2p_msg,
                        self.storage.headers, self.storage.headers_lock)
                    self.logger.exception("Reorg confirmed")

                if stop_header:
                    self.logger.debug(f"Got new tip from ConduitRaw service for "
                                      f"parsing at height: {stop_header.height}")

                    await self.check_for_ibd_status(stop_header)
                    return is_reorg, start_header, stop_header, old_hashes, new_hashes
                else:
                    continue
            except Exception:
                self.logger.exception("unexpected exception in long_poll_conduit_raw_chain_tip")
                time.sleep(0.2)
            finally:
                if ipc_sock_client:
                    ipc_sock_client.close()

    async def push_chip_away_work(self, work_units: List[WorkUnit]) -> None:
        # Push to workers only a subset of the 'full_batch_for_deficit' to chip away
        for is_reorg, part_size, work_item_id, block_hash, block_num, first_tx_pos_batch, \
                part_end_offset, tx_offsets_array in work_units:
            len_arr = len(tx_offsets_array) * 8  # 8 byte uint64_t
            packed_array = tx_offsets_array.tobytes()
            packed_msg = struct.pack(f"<IIII32sIIQ{len_arr}s", MsgType.MSG_BLOCK, len_arr,
                work_item_id, is_reorg, block_hash, block_num, first_tx_pos_batch, part_end_offset, packed_array)
            await self.mined_tx_socket.send(packed_msg)

    async def chip_away(self, remaining_work_units: List[WorkUnit]):
        """This breaks up blocks into smaller 'WorkUnits'. This allows tailoring workloads and
        max memory allocations to be safe with available system resources)"""
        remaining_work, work_for_this_batch = \
            self.sync_state.get_work_units_chip_away(remaining_work_units)
        await self.push_chip_away_work(work_for_this_batch)
        return remaining_work

    async def _get_differential_post_reorg(self, old_hashes: List[bytes], new_hashes: List[bytes])\
            -> Tuple[Set[bytes], Set[bytes], Set[bytes]]:
        # The transaction_parser.py needs the "removals_from_mempool"
        # to only include these in the inputs, outputs, pushdata tables
        # the rest of the reorging block txs are already included for these tables
        ipc_sock_client = await self.loop.run_in_executor(self.general_executor, IPCSocketClient)
        response: ReorgDifferentialResponse = await self.loop.run_in_executor(self.general_executor,
            ipc_sock_client.reorg_differential, old_hashes, new_hashes)
        removals_from_mempool = response.removals_from_mempool
        additions_to_mempool = response.additions_to_mempool
        orphaned_tx_hashes = response.orphaned_tx_hashes
        self.logger.debug(f"removals_from_mempool (len={len(removals_from_mempool)}), "
                          f"additions_to_mempool (len={len(additions_to_mempool)}),"
                          f"orphaned_tx_hashes (len={len(orphaned_tx_hashes)}")
        return removals_from_mempool, additions_to_mempool, orphaned_tx_hashes

    async def sync_all_blocks_job(self):
        """Supervises synchronization to catch up to the block tip of ConduitRaw service"""

        async def wait_for_batched_blocks_completion(batch_id, all_pending_block_hashes) -> None:
            """all_pending_block_hashes is copied into these threads to prevent mutation"""
            try:
                self.logger.debug(f"Waiting for main batch to complete")
                await self.sync_state.done_blocks_tx_parser_event.wait()
                self.logger.debug(f"Main batch complete. "
                                  f"Connecting headers to lock in indexing progress...")
                self.sync_state.done_blocks_tx_parser_event.clear()
                await self.connect_done_block_headers(all_pending_block_hashes.copy())
            except Exception:
                self.logger.exception("unexpected exception in "
                    "'wait_for_batched_blocks_completion' ")

        async def maintain_chain_tip():
            # Now wait on the queue for notifications

            batch_id = 0
            while True:
                # ------------------------- Batch Start ------------------------- #
                # This queue is just a trigger to check the new tip and allocate another batch
                is_reorg, start_header, stop_header, old_hashes, new_hashes = \
                    await self.long_poll_conduit_raw_chain_tip()

                removals_from_mempool, additions_to_mempool = None, None
                if is_reorg:
                    self.mysql_db.queries.mysql_update_oprhaned_headers(old_hashes)
                    assert old_hashes is not None
                    assert new_hashes is not None
                    removals_from_mempool, additions_to_mempool, orphaned_tx_hashes = \
                        await self._get_differential_post_reorg(old_hashes, new_hashes)

                    self.mysql_db.queries.mysql_load_temp_mempool_additions(additions_to_mempool)
                    self.mysql_db.queries.mysql_load_temp_mempool_removals(removals_from_mempool)
                    self.mysql_db.queries.mysql_load_temp_orphaned_tx_hashes(orphaned_tx_hashes)

                deficit = stop_header.height - (start_header.height - 1)
                local_tip_height = self.sync_state.get_local_block_tip_height()
                self.logger.debug(f"Allocated {deficit} headers in main batch to from height: "
                    f"{start_header.height} to height: {stop_header.height}")
                self.logger.debug(f"ConduitRaw tip height: {local_tip_height}. "
                                  f"(remaining={deficit})")
                if stop_header.height <= self.sync_state.get_local_block_tip_height():
                    continue  # drain the queue until we hit relevant ones

                batch_id += 1
                self.logger.debug(f"Controller Batch {batch_id} Start")

                # Allocate the "MainBatch" and get the full set of "WorkUnits" (blocks broken up)
                main_batch = await self.loop.run_in_executor(
                    self.general_executor, self.sync_state.get_main_batch, start_header,
                    stop_header)
                all_pending_block_hashes = set()
                self.logger.debug(f"is_reorg={is_reorg} in controller")
                all_work_units = self.sync_state.get_work_units_all(is_reorg,
                    all_pending_block_hashes, main_batch)
                self.tx_parser_completion_queue.put_nowait(all_pending_block_hashes.copy())


                # Chip away at the 'MainBatch' without exceeding configured resource constraints
                remaining_work_units = all_work_units
                while len(remaining_work_units) != 0:
                    remaining_work_units = await self.chip_away(remaining_work_units)
                    self.logger.debug(f"Waiting for chip away batch to complete")
                    await self.sync_state.chip_away_batch_event.wait()
                    self.logger.debug(f"Chip away batch completed. Remaining work units: {len(remaining_work_units)}")
                    self.sync_state.chip_away_batch_event.clear()
                    self.sync_state.reset_pending_chip_away_work_items()

                await wait_for_batched_blocks_completion(batch_id, all_pending_block_hashes)

                #  Todo: if is_reorg=True then need to preserve this context to ensure that the final
                #   mempool invalidation step is done with extreme care.
                #   May also need to trigger an event in the aiohttp API around a critical final
                #   atomic commit (to update mempool + API tip)
                if is_reorg:
                    reorg_handling_complete = False
                    await self.reorg_event_socket.send(
                        cbor2.dumps((reorg_handling_complete, start_header.hash, stop_header.hash)))

                    api_block_tip_height = await self.sanity_checks_and_update_api_tip(is_reorg)
                    self.mysql_db.tables.mysql_drop_temp_orphaned_txs()

                else:
                    api_block_tip_height = await self.sanity_checks_and_update_api_tip(is_reorg)
                self.sync_state.reset_pending_blocks()

                if is_reorg:
                    reorg_handling_complete = True
                    await self.reorg_event_socket.send(
                        cbor2.dumps((reorg_handling_complete, start_header.hash, stop_header.hash)))
                # ------------------------- Batch complete ------------------------- #
                self.logger.debug(f"Controller Batch {batch_id} Complete. "
                    f"New tip height: {api_block_tip_height}")

        try:
            # up to 500 blocks per loop
            await maintain_chain_tip()

        except asyncio.CancelledError:
            self.logger.exception("asyncio event cancelled")
            raise
        except Exception as e:
            self.logger.exception("sync_blocks_job raised an exception")
            raise

    # -- Message Types -- #
    async def send_version(self, recv_host=None, recv_port=None, send_host=None, send_port=None):
        message = self.serializer.version(
            recv_host=recv_host, recv_port=recv_port, send_host=send_host, send_port=send_port,
        )
        await self.send_request(VERSION, message)

    async def wait_for_batch_completion(self, blocks_batch_set):
        while True:
            # Todo change this to a zmq socket with asyncio context so I can await in event loop
            item = await self.loop.run_in_executor(self.general_executor,
                self.worker_ack_queue_tx_parse_confirmed.get)
            worker_id, work_item_id, block_hash, txs_done_count = item
            try:
                self.sync_state._pending_work_item_progress_counter[work_item_id] += txs_done_count
                self.sync_state._pending_blocks_progress_counter[block_hash] += txs_done_count
            except KeyError:
                raise

            try:
                if self.sync_state.have_completed_work_item(work_item_id, block_hash):
                    self.sync_state.all_pending_chip_away_work_item_ids.remove(work_item_id)
                    if len(self.sync_state.all_pending_chip_away_work_item_ids) == 0:
                        self.sync_state.chip_away_batch_event.set()

                if self.sync_state.block_is_fully_processed(block_hash):
                    header = self.storage.get_header_for_hash(block_hash)
                    if not block_hash in blocks_batch_set:
                        self.logger.exception(f"also wrote unexpected block: "
                                              f"{hash_to_hex_str(header.hash)} {header.height} to disc")
                        continue

                    # self.logger.debug(f"block height={header.height} done!")
                    blocks_batch_set.remove(block_hash)
                    with self.sync_state.done_blocks_tx_parser_lock:
                        self.sync_state.done_blocks_tx_parser.add(block_hash)
            except KeyError:
                header = self.storage.get_header_for_hash(block_hash)
                self.logger.debug(f"also parsed block: {header.height}")

            # all blocks in batch processed
            if len(blocks_batch_set) == 0:
                self.sync_state.done_blocks_tx_parser_event.set()
                break

    async def batch_completion_job(self):
        """
        In a crash, the batch will be re-done on start-up because the block_headers.mmap
        file is not updated until all work in the batch is ack'd and flushed.
        """
        batch_id = 0
        while True:
            try:
                all_pending_block_hashes = await self.tx_parser_completion_queue.get()
                await self.wait_for_batch_completion(all_pending_block_hashes)
                self.logger.debug(f"ACKs for batch {batch_id} received")
                batch_id += 1
            except Exception as e:
                self.logger.exception(e)

    def _invalidate_mempool_rows(self, api_block_tip_height: int, api_block_tip_hash: bytes) \
            -> None:
        self.mysql_db.start_transaction()
        try:
            self.mysql_db.mysql_invalidate_mempool_rows()
            self.mysql_db.mysql_drop_temp_mined_tx_hashes()
            self.mysql_db.mysql_update_api_tip_height_and_hash(api_block_tip_height,
                api_block_tip_hash)
        finally:
            self.mysql_db.commit_transaction()

    def _apply_reorg_diff_to_mempool(self, api_block_tip_height: int, api_block_tip_hash: bytes) -> None:
        self.mysql_db.start_transaction()
        try:
            self.mysql_db.mysql_drop_temp_mined_tx_hashes()  # not required so discard it
            self.mysql_db.queries.mysql_remove_from_mempool()
            self.mysql_db.queries.mysql_add_to_mempool()
            self.mysql_db.mysql_update_api_tip_height_and_hash(api_block_tip_height,
                api_block_tip_hash)
        finally:
            self.mysql_db.commit_transaction()
