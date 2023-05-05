import asyncio
import bitcoinx
import cbor2
from bitcoinx import hash_to_hex_str, double_sha256, MissingHeader, Header
from concurrent.futures.thread import ThreadPoolExecutor
from io import BytesIO
import logging
import multiprocessing
from multiprocessing.process import BaseProcess
import os
from pathlib import Path
import socket
import struct
import time
import typing
from typing import Any, cast
import zmq
from zmq.asyncio import Context as AsyncZMQContext

from conduit_lib.algorithms import parse_txs
from conduit_lib.bitcoin_p2p_client import BitcoinP2PClient
from conduit_lib.controller_base import ControllerBase
from conduit_lib.database.mysql.types import MinedTxHashes
from conduit_lib.headers_api_threadsafe import HeadersAPIThreadsafe
from conduit_lib.ipc_sock_client import IPCSocketClient
from conduit_lib.database.mysql.mysql_database import (
    MySQLDatabase,
    load_mysql_database,
)
from conduit_lib.deserializer import Deserializer
from conduit_lib import Peer
from conduit_lib.handlers import Handlers
from conduit_lib.ipc_sock_msg_types import (
    HeadersBatchedResponse,
    ReorgDifferentialResponse,
)
from conduit_lib.serializer import Serializer
from conduit_lib.store import setup_storage, Storage
from conduit_lib.constants import (
    MsgType,
    NULL_HASH,
    MAIN_BATCH_HEADERS_COUNT_LIMIT,
    CONDUIT_INDEX_SERVICE_NAME,
    TARGET_BYTES_BLOCK_BATCH_REQUEST_SIZE_CONDUIT_INDEX,
)
from conduit_lib.types import (
    BlockHeaderRow,
    ChainHashes,
    BlockSliceRequestType,
    Slice,
)
from conduit_lib.utils import create_task, headers_to_p2p_struct
from conduit_lib.wait_for_dependencies import (
    wait_for_mysql,
    wait_for_ipc_socket_server,
)

from .sync_state import SyncState
from .types import WorkUnit
from .workers.common import (
    convert_pushdata_rows_for_flush,
    convert_input_rows_for_flush,
)
from .workers.transaction_parser import TxParser
from conduit_lib.zmq_sockets import (
    bind_async_zmq_socket,
    connect_async_zmq_socket,
)

MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))

if typing.TYPE_CHECKING:
    from conduit_lib.networks import NetworkConfig


def get_headers_dir_conduit_index() -> Path:
    return Path(os.getenv("DATADIR_SSD", str(MODULE_DIR.parent))) / "headers_conduit_index"


class ZMQSocketListeners:
    def __init__(self) -> None:
        ZMQ_CONNECT_HOST = os.getenv("ZMQ_CONNECT_HOST", "127.0.0.1")
        self.zmq_async_context = AsyncZMQContext.instance()
        self.zmq_sockets: list[zmq.asyncio.Socket] = []

        # Controller to TxParser Workers
        self.socket_mined_tx = bind_async_zmq_socket(
            self.zmq_async_context, "tcp://127.0.0.1:55555", zmq.SocketType.PUSH
        )
        self.socket_mempool_tx = bind_async_zmq_socket(
            self.zmq_async_context, "tcp://127.0.0.1:55556", zmq.SocketType.PUSH
        )

        self.socket_mined_tx_ack = bind_async_zmq_socket(
            self.zmq_async_context,
            "tcp://127.0.0.1:55889",
            zmq.SocketType.PULL,
            [(zmq.SocketOption.RCVHWM, 10000)],
        )
        self.socket_mined_tx_parsed_ack = bind_async_zmq_socket(
            self.zmq_async_context, "tcp://127.0.0.1:54214", zmq.SocketType.PULL
        )

        self.socket_kill_workers = bind_async_zmq_socket(
            self.zmq_async_context, "tcp://127.0.0.1:63241", zmq.SocketType.PUB
        )
        self.socket_is_post_ibd = bind_async_zmq_socket(
            self.zmq_async_context, "tcp://127.0.0.1:52841", zmq.SocketType.PUB
        )

        # Controller to Aiohttp API
        self.reorg_event_socket = connect_async_zmq_socket(
            self.zmq_async_context,
            f"tcp://{ZMQ_CONNECT_HOST}:51495",
            zmq.SocketType.PUSH,
        )

    def bind_async_zmq_socket(
        self,
        context: AsyncZMQContext,
        uri: str,
        zmq_socket_type: zmq.SocketType,
        options: list[tuple[zmq.SocketOption, int | bytes]] | None = None,
    ) -> zmq.asyncio.Socket:
        zmq_socket = bind_async_zmq_socket(context, uri, zmq_socket_type, options)
        self.zmq_sockets.append(zmq_socket)
        return zmq_socket

    def close(self) -> None:
        for zmq_socket in self.zmq_sockets:
            zmq_socket.close()


class Controller(ControllerBase):
    def __init__(self, net_config: "NetworkConfig", loop_type: None = None) -> None:
        self.service_name = CONDUIT_INDEX_SERVICE_NAME
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
        self.net_config = net_config
        self.peer = self.net_config.get_peer()

        self.general_executor = ThreadPoolExecutor(max_workers=4)

        wait_for_ipc_socket_server()
        wait_for_mysql()
        headers_dir = get_headers_dir_conduit_index()
        self.storage: Storage = setup_storage(self.net_config, headers_dir)
        self.headers_threadsafe = HeadersAPIThreadsafe(self.storage.headers, self.storage.headers_lock)
        self.headers_threadsafe_blocks = HeadersAPIThreadsafe(
            self.storage.block_headers, self.storage.block_headers_lock
        )
        self.handlers: Handlers = Handlers(self, self.net_config, self.storage)  # type: ignore
        self.serializer: Serializer = Serializer(self.net_config)
        self.deserializer: Deserializer = Deserializer(self.net_config)
        self.sync_state: SyncState = SyncState(self.storage, self)

        self.zmq_socket_listeners = ZMQSocketListeners()

        # Bitcoin Network Socket
        self.bitcoin_p2p_client: BitcoinP2PClient | None = None

        # Mempool and API state
        self.mempool_tx_hash_set: set[bytes] = set()

        self.ibd_signal_sent = False

        # Batch Completion
        self.tx_parser_completion_queue: asyncio.Queue[set[bytes]] = asyncio.Queue()

        self.global_tx_hashes_dict: dict[int, list[bytes]] = {}  # blk_num:tx_hashes
        self.mempool_tx_count: int = 0

        # Database Interfaces
        self.mysql_db: MySQLDatabase | None = None
        self.ipc_sock_client = IPCSocketClient()
        self.total_time_connecting_headers = 0.0
        self.estimated_moving_av_block_size_mb = (
            0.1 if self.sync_state.get_local_block_tip_height() < 2016 else 500
        )

    def setup(self) -> None:
        self.mysql_db = load_mysql_database()

        # Drop mempool table for now and re-fill - easiest brute force way to achieve consistency
        self.mysql_db.tables.mysql_drop_mempool_table()
        self.mysql_db.tables.mysql_create_permanent_tables()

    async def run(self) -> None:
        self.running = True
        self.setup()
        self.tasks.append(create_task(self.start_jobs()))
        while True:
            await asyncio.sleep(5)

    async def maintain_node_connection(self) -> None:
        assert self.bitcoin_p2p_client is not None
        while True:
            await self.bitcoin_p2p_client.handshake_complete_event.wait()
            await self.bitcoin_p2p_client.connection_lost_event.wait()
            self.bitcoin_p2p_client.connection_lost_event.clear()
            await self.connect_session(self.peer)
            self.logger.debug(
                f"The bitcoin node disconnected but is now reconnected. "
                f"Re-requesting mempool (as appropriate)..."
            )

    async def stop(self) -> None:
        self.running = False

        # Do this first while all the stuff below errors unexpectedly so that the manual
        # cleanup is much less painful.
        for p in self.processes:
            p.terminate()
            p.join()

        self.ipc_sock_client.close()
        self.general_executor.shutdown(wait=False, cancel_futures=True)

        if self.storage:
            await self.storage.close()

        if self.zmq_socket_listeners:
            with self.zmq_socket_listeners.socket_kill_workers as sock:
                try:
                    await sock.send(b"stop_signal")
                except zmq.error.ZMQError as zmq_error:
                    # We silence the error if this socket is already closed.
                    # But other cases we want to know about them and maybe fix
                    # them or add them to this list.
                    if str(zmq_error) != "not a socket":
                        raise
            await asyncio.sleep(1)
            self.zmq_socket_listeners.close()

        self.sync_state._batched_blocks_exec.shutdown(wait=False)

        for task in self.tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    async def connect_session(self, peer: Peer) -> None:
        self.logger.debug(
            "Connecting to (%s, %s) [%s]",
            peer.remote_host,
            peer.remote_port,
            self.net_config.NET,
        )
        try:
            self.bitcoin_p2p_client = BitcoinP2PClient(
                peer.remote_host,
                peer.remote_port,
                self.handlers,
                self.net_config,
            )
            await self.bitcoin_p2p_client.wait_for_connection()
        except ConnectionResetError:
            await self.stop()

    # Multiprocessing Workers
    def start_workers(self) -> None:
        WORKER_COUNT_TX_PARSERS = int(os.getenv("WORKER_COUNT_TX_PARSERS", "4"))
        for worker_id in range(WORKER_COUNT_TX_PARSERS):
            p: multiprocessing.Process = TxParser(worker_id + 1)
            p.start()
            self.processes.append(p)
        # Allow time for TxParsers to bind to the zmq socket to distribute load evenly
        time.sleep(2)

    async def maybe_do_db_repair(self) -> None:
        """If there were blocks that were only partially flushed that go beyond the check-pointed
        block hash, we need to purge those rows from the tables before we resume synchronization.
        """
        assert self.mysql_db is not None
        result = self.mysql_db.queries.mysql_get_checkpoint_state()
        if not result:
            self.mysql_db.tables.initialise_checkpoint_state()
            return None

        (
            best_flushed_block_height,
            best_flushed_block_hash,
            reorg_was_allocated,
            first_allocated_block_hash,
            last_allocated_block_hash,
            old_hashes_array,
            new_hashes_array,
        ) = result
        needs_repair = best_flushed_block_hash != last_allocated_block_hash
        if not needs_repair:
            return

        self.logger.debug(
            f"ConduitDB did not shut down cleanly last session. " f"Performing automated database repair..."
        )

        # Drop and re-create mempool table
        self.mysql_db.tables.mysql_drop_mempool_table()
        self.mysql_db.tables.mysql_create_mempool_table()

        # Delete / Clean up all db entries for blocks above the best_flushed_block_hash
        if reorg_was_allocated:
            old_hashes: list[bytes] | None = [
                old_hashes_array[i * 32 : (i + 1) * 32] for i in range(len(old_hashes_array) // 32)
            ]
            new_hashes: list[bytes] | None = [
                new_hashes_array[i * 32 : (i + 1) * 32] for i in range(len(new_hashes_array) // 32)
            ]
            assert new_hashes is not None
            await self.undo_specific_block_hashes(new_hashes)
        else:
            old_hashes = None
            new_hashes = None
            best_header = self.headers_threadsafe.get_header_for_hash(best_flushed_block_hash)
            await self.undo_blocks_above_height(best_header.height)

        # Re-attempt indexing of allocated work (that did not complete last time)
        self.logger.debug(f"Re-attempting previously allocated work")
        first_allocated_header = self.headers_threadsafe.get_header_for_hash(first_allocated_block_hash)
        last_allocated_header = self.headers_threadsafe.get_header_for_hash(last_allocated_block_hash)

        best_flushed_tip_height = await self.index_blocks(
            reorg_was_allocated,
            first_allocated_header,
            last_allocated_header,
            old_hashes,
            new_hashes,
        )
        self.logger.debug(f"Database repair complete. " f"New chain tip: {best_flushed_tip_height}")

    async def undo_blocks_above_height(self, height: int) -> None:
        tip_header = self.sync_state.get_local_block_tip()
        unsafe_blocks = [
            (self.headers_threadsafe.get_header_for_height(height).hash, height)
            for height in range(height + 1, tip_header.height + 1)
        ]
        await self._undo_blocks(unsafe_blocks)

    async def undo_specific_block_hashes(self, block_hashes: list[bytes]) -> None:
        unsafe_blocks = [
            (
                block_hash,
                self.headers_threadsafe.get_header_for_hash(block_hash).height,
            )
            for block_hash in block_hashes
        ]
        await self._undo_blocks(unsafe_blocks)

    async def _undo_blocks(self, blocks_to_undo: list[tuple[bytes, int]]) -> None:
        assert self.mysql_db is not None
        ipc_sock_client = await self.loop.run_in_executor(self.general_executor, IPCSocketClient)
        batched_tx_rows = []
        batched_in_rows = []
        batched_out_rows = []
        batched_pd_rows = []
        batched_header_hashes = []
        for block_hash, height in blocks_to_undo:
            self.logger.debug(f"Undoing block hash: {hash_to_hex_str(block_hash)}, " f"height: {height}")
            tx_offsets = next(ipc_sock_client.transaction_offsets_batched([block_hash]))
            block_num = ipc_sock_client.block_number_batched([block_hash]).block_numbers[0]
            slice = Slice(start_offset=0, end_offset=0)
            raw_blocks_array = ipc_sock_client.block_batched([BlockSliceRequestType(block_num, slice)])
            block_num, len_slice = struct.unpack_from(f"<IQ", raw_blocks_array, 0)
            _block_num, _len_slice, raw_block = struct.unpack_from(f"<IQ{len_slice}s", raw_blocks_array, 0)
            (
                tx_rows,
                _tx_rows_mempool,
                in_rows,
                out_rows,
                pd_rows,
                utxo_spends,
                pushdata_matches_tip_filter,
            ) = parse_txs(
                raw_block,
                tx_offsets,
                height,
                confirmed=True,
                first_tx_pos_batch=0,
            )
            pushdata_rows_for_flushing = convert_pushdata_rows_for_flush(pd_rows)
            input_rows_for_flushing = convert_input_rows_for_flush(in_rows)

            batched_tx_rows.extend(tx_rows)
            batched_in_rows.extend(input_rows_for_flushing)
            batched_out_rows.extend(out_rows)
            batched_pd_rows.extend(pushdata_rows_for_flushing)
            batched_header_hashes.append(block_hash)

        # Delete All
        tx_hashes = [row[0] for row in batched_tx_rows]
        self.mysql_db.queries.mysql_delete_transaction_rows(tx_hashes)

        # Commented out because way too slow... Need to rely on overwriting these records instead
        # ScyllaDB will be a different story because of merge operator under the hood
        # self.mysql_db.queries.mysql_delete_pushdata_rows(batched_pd_rows)
        # self.mysql_db.queries.mysql_delete_output_rows(batched_out_rows)
        # self.mysql_db.queries.mysql_delete_input_rows(batched_in_rows)
        # for block_hash in batched_header_hashes:
        #     self.mysql_db.queries.mysql_delete_header_row(block_hash)

    async def request_mempool(self) -> None:
        # NOTE: if the -rejectmempoolrequest=0 option is not set on the node, the node disconnects
        self.logger.debug(f"Requesting mempool...")
        assert self.bitcoin_p2p_client is not None
        await self.bitcoin_p2p_client.send_message(self.serializer.mempool())

    async def start_jobs(self) -> None:
        self.mysql_db = self.storage.mysql_database
        create_task(self.wait_for_mined_tx_acks_task())
        self.start_workers()
        await self.spawn_batch_completion_job()
        await self.maybe_do_db_repair()
        await self.spawn_lagging_batch_monitor()
        self.tasks.append(create_task(self.sync_all_blocks_job()))

    async def spawn_batch_completion_job(self) -> None:
        create_task(self.batch_completion_job())

    def all_blocks_processed(self, global_tx_hashes_dict: dict[int, list[bytes]]) -> bool:
        expected_block_hashes = list(self.sync_state.expected_blocks_tx_counts.keys())
        expected_block_nums = self.ipc_sock_client.block_number_batched(expected_block_hashes).block_numbers
        block_hash_to_num_map = dict(zip(expected_block_hashes, expected_block_nums))

        for block_hash in expected_block_hashes:
            block_num: int = block_hash_to_num_map[block_hash]
            processed_tx_count: int = len(global_tx_hashes_dict.get(block_num, []))
            expected_tx_count: int = self.sync_state.expected_blocks_tx_counts[block_hash]
            if expected_tx_count != processed_tx_count:
                return False
        return True

    async def wait_for_mined_tx_acks_task(self) -> None:
        while True:
            message = await self.zmq_socket_listeners.socket_mined_tx_ack.recv()
            new_mined_tx_hashes = cbor2.loads(message)

            for blk_num, new_hashes in new_mined_tx_hashes.items():
                if not self.global_tx_hashes_dict.get(blk_num):
                    self.global_tx_hashes_dict[blk_num] = []
                self.global_tx_hashes_dict[blk_num].extend(new_hashes)

    async def update_mempool_and_checkpoint_tip_atomic(
        self, best_flushed_block_tip: bitcoinx.Header, is_reorg: bool
    ) -> None:
        """
        Get all tx_hashes up to api_block_tip_height
        Must ATOMICALLY:
            1) invalidate mempool rows (that have been mined)
            2) update api chain tip
        """
        assert self.mysql_db is not None

        # 1) Check if we have received all ACKs for mined tx hashes
        while True:
            if len(self.global_tx_hashes_dict) >= len(
                self.sync_state.expected_blocks_tx_counts
            ) and self.all_blocks_processed(self.global_tx_hashes_dict):
                break
            else:
                await asyncio.sleep(0.2)

        # 2) Transfer from dict -> Temp Table for Table join with Mempool table
        mined_tx_hashes = []
        for blk_num, new_mined_tx_hashes in self.global_tx_hashes_dict.items():
            for tx_hash in new_mined_tx_hashes:
                mined_tx_hashes.append(MinedTxHashes(tx_hash.hex(), blk_num))

        self.mysql_db.mysql_drop_temp_mined_tx_hashes()  # For good measure. Should be unnecessary..
        self.logger.debug(f"Loading {len(mined_tx_hashes)} to 'temp_mined_tx_hashes' table")
        self.mysql_db.mysql_load_temp_mined_tx_hashes(mined_tx_hashes=mined_tx_hashes)
        self.global_tx_hashes_dict = {}

        # 3) Atomically invalidate all mempool rows that have been mined & update best flushed tip
        # If there is a reorg, it is not as simple as just deleting the mined txs
        # we must both add and remove txs based on the differential between the old and new chain
        if is_reorg:
            self._apply_reorg_diff_to_mempool(best_flushed_block_tip)
        else:
            self._invalidate_mempool_rows(best_flushed_block_tip)

    async def sanity_checks_and_update_best_flushed_tip(self, is_reorg: bool) -> int:
        assert self.mysql_db is not None
        t0 = time.time()
        checkpoint_tip: bitcoinx.Header = self.sync_state.get_local_block_tip()
        best_flushed_block_height: int = checkpoint_tip.height

        # Update API tip for filtering of queries in the internal aiohttp API
        conduit_best_tip = await self.sync_state.get_conduit_best_tip()
        if await self.sync_state.is_post_ibd_state(checkpoint_tip, conduit_best_tip):
            await self.update_mempool_and_checkpoint_tip_atomic(checkpoint_tip, is_reorg)

        else:
            # No txs in mempool until is_post_ibd == True
            self.mysql_db.mysql_update_checkpoint_tip(checkpoint_tip)
        t_diff = time.time() - t0
        self.logger.debug(f"Sanity checks took: {t_diff} seconds")
        return best_flushed_block_height

    async def connect_done_block_headers(self, blocks_batch_set: set[bytes]) -> None:
        assert self.mysql_db is not None
        ipc_socket_client = IPCSocketClient()
        t0 = time.perf_counter()
        unsorted_headers = [self.storage.get_header_for_hash(h) for h in blocks_batch_set]

        def get_height(header: Header) -> int:
            return cast(int, header.height)

        sorted_headers = sorted(unsorted_headers, key=get_height)
        sorted_heights = [h.height for h in sorted_headers]

        # Assert the resultant blocks are consecutive with no gaps
        max_height = max(sorted_heights)

        # Check for gaps
        expected_block_heights = [i + 1 for i in range(max_height - len(sorted_heights), max_height)]
        assert sorted_heights == expected_block_heights

        block_headers: bitcoinx.Headers = self.storage.block_headers
        for header in sorted_headers:
            block_headers.connect(header.raw)

        self.storage.block_headers.flush()

        # Get Tx Counts & Block Sizes for the headers table
        sorted_hashes = [h.hash for h in sorted_headers]
        block_numbers = ipc_socket_client.block_number_batched(sorted_hashes).block_numbers
        block_hash_to_num_map = dict(zip(sorted_hashes, block_numbers))
        sorted_block_metadata_batch = ipc_socket_client.block_metadata_batched(
            sorted_hashes
        ).block_metadata_batch

        header_rows: list[BlockHeaderRow] = []
        for header, block_metadata in zip(sorted_headers, sorted_block_metadata_batch):
            blk_num = block_hash_to_num_map[header.hash]
            row = BlockHeaderRow(
                blk_num,
                header.hash.hex(),
                header.height,
                header.raw.hex(),
                block_metadata.tx_count,
                block_metadata.block_size,
                is_orphaned=0,
            )
            header_rows.append(row)
        self.mysql_db.bulk_loads.mysql_bulk_load_headers(header_rows)

        tip = self.sync_state.get_local_block_tip()
        self.logger.debug(f"Connected up to header height {tip.height}, " f"hash {hash_to_hex_str(tip.hash)}")
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
            GENESIS_BLOCK_HASH = os.environ["GENESIS_BLOCK_HASH"]
            if str(e).find(GENESIS_BLOCK_HASH) != -1 or str(e).find(NULL_HASH) != -1:
                self.logger.debug("skipping - prev_out == genesis block")
                return True
            else:
                self.logger.exception("Caught exception")
                raise

    async def maybe_start_processing_mempool_txs(self) -> None:
        if not self.ibd_signal_sent:
            self.logger.debug(
                f"Initial block download mode completed. " f"Activating mempool tx processing..."
            )
            await self.connect_session(self.peer)
            await self.zmq_socket_listeners.socket_is_post_ibd.send(b"is_ibd_signal")
            self.ibd_signal_sent = True
            create_task(self.maintain_node_connection())
            assert self.bitcoin_p2p_client is not None
            await self.bitcoin_p2p_client.handshake("127.0.0.1", self.net_config.PORT)
            await self.bitcoin_p2p_client.handshake_complete_event.wait()
            await self.request_mempool()
            create_task(self.log_current_mempool_size_task_async())

    async def long_poll_conduit_raw_chain_tip(
        self,
    ) -> tuple[bool, Header, Header, ChainHashes | None, ChainHashes | None]:
        conduit_best_tip = await self.sync_state.get_conduit_best_tip()
        local_tip = self.headers_threadsafe.tip()
        if await self.sync_state.is_post_ibd_state(local_tip, conduit_best_tip):
            await self.maybe_start_processing_mempool_txs()

        OVERKILL_REORG_DEPTH = 500  # Virtually zero chance of a reorg more deep than this.
        old_hashes = None
        new_hashes = None
        ipc_sock_client = None
        while True:
            is_reorg = False
            try:
                tip_height = self.sync_state.get_local_block_tip_height()
                start_height = tip_height + 1

                if tip_height > 2016:
                    await self.update_moving_average(tip_height)

                estimated_ideal_block_count = self.get_ideal_block_batch_count(
                    TARGET_BYTES_BLOCK_BATCH_REQUEST_SIZE_CONDUIT_INDEX
                )

                # Long-polling
                ipc_sock_client = await self.loop.run_in_executor(self.general_executor, IPCSocketClient)
                result: HeadersBatchedResponse = await self.loop.run_in_executor(
                    self.general_executor,
                    ipc_sock_client.headers_batched,
                    start_height,
                    estimated_ideal_block_count,
                )

                headers_p2p_msg = headers_to_p2p_struct(result.headers_batch)
                (
                    first_header_of_batch,
                    success,
                ) = self.headers_threadsafe.connect_headers(BytesIO(headers_p2p_msg), lock=False)
                if success:
                    start_header = self.headers_threadsafe.get_header_for_hash(
                        double_sha256(first_header_of_batch)
                    )
                    stop_header = self.sync_state.get_local_tip()
                else:
                    self.logger.debug(f"Potential reorg detected")
                    # This should mean there has been a reorg. The tip should always connect
                    from_height = max(start_height - OVERKILL_REORG_DEPTH, 1)
                    count = MAIN_BATCH_HEADERS_COUNT_LIMIT + OVERKILL_REORG_DEPTH
                    result = await self.loop.run_in_executor(
                        self.general_executor,
                        ipc_sock_client.headers_batched,
                        from_height,
                        count,
                    )

                    # Try again
                    headers_p2p_msg = headers_to_p2p_struct(result.headers_batch)
                    (
                        is_reorg,
                        start_header,
                        stop_header,
                        old_hashes,
                        new_hashes,
                    ) = self.headers_threadsafe.connect_headers_reorg_safe(headers_p2p_msg)
                    self.logger.error("Reorg confirmed")

                if stop_header:
                    self.logger.debug(
                        f"Got new tip from ConduitRaw service for " f"parsing at height: {stop_header.height}"
                    )
                    return (
                        is_reorg,
                        start_header,
                        stop_header,
                        old_hashes,
                        new_hashes,
                    )
                else:
                    continue
            except socket.timeout:
                time.sleep(0.2)
            except Exception:
                self.logger.exception("unexpected exception in long_poll_conduit_raw_chain_tip")
                time.sleep(0.2)
            finally:
                if ipc_sock_client:
                    try:
                        ipc_sock_client.close()
                    except OSError:
                        # "OSError: [WinError 10057] A request to send or receive data was
                        #  disallowed because the socket is not connected and (when sending on a
                        #  datagram socket using a sendto call) no address was supplied"
                        pass

    async def push_chip_away_work(self, work_units: list[WorkUnit]) -> None:
        # Push to workers only a subset of the 'full_batch_for_deficit' to chip away
        for (
            is_reorg,
            part_size,
            work_item_id,
            block_hash,
            block_num,
            first_tx_pos_batch,
            part_end_offset,
            tx_offsets_array,
        ) in work_units:
            len_arr = len(tx_offsets_array) * 8  # 8 byte uint64_t
            packed_array = tx_offsets_array.tobytes()
            packed_msg = struct.pack(
                f"<IIII32sIIQ{len_arr}s",
                MsgType.MSG_BLOCK,
                len_arr,
                work_item_id,
                is_reorg,
                block_hash,
                block_num,
                first_tx_pos_batch,
                part_end_offset,
                packed_array,
            )
            await self.zmq_socket_listeners.socket_mined_tx.send(packed_msg)

    async def chip_away(self, remaining_work_units: list[WorkUnit]) -> list[WorkUnit]:
        """This breaks up blocks into smaller 'WorkUnits'. This allows tailoring workloads and
        max memory allocations to be safe with available system resources)"""
        (
            remaining_work,
            work_for_this_batch,
        ) = self.sync_state.get_work_units_chip_away(remaining_work_units)

        await self.push_chip_away_work(work_for_this_batch)
        return remaining_work

    async def _get_differential_post_reorg(
        self, old_hashes: list[bytes], new_hashes: list[bytes]
    ) -> tuple[set[bytes], set[bytes], set[bytes]]:
        # The transaction_parser.py needs the "removals_from_mempool"
        # to only include these in the inputs, outputs, pushdata tables
        # the rest of the reorging block txs are already included for these tables
        ipc_sock_client = await self.loop.run_in_executor(self.general_executor, IPCSocketClient)
        response: ReorgDifferentialResponse = await self.loop.run_in_executor(
            self.general_executor,
            ipc_sock_client.reorg_differential,
            old_hashes,
            new_hashes,
        )
        removals_from_mempool = response.removals_from_mempool
        additions_to_mempool = response.additions_to_mempool
        orphaned_tx_hashes = response.orphaned_tx_hashes
        self.logger.debug(
            f"removals_from_mempool (len={len(removals_from_mempool)}), "
            f"additions_to_mempool (len={len(additions_to_mempool)}),"
            f"orphaned_tx_hashes (len={len(orphaned_tx_hashes)})"
        )
        return removals_from_mempool, additions_to_mempool, orphaned_tx_hashes

    async def wait_for_batched_blocks_completion(self, all_pending_block_hashes: set[bytes]) -> None:
        """all_pending_block_hashes is copied into these threads to prevent mutation"""
        try:
            self.logger.debug(f"Waiting for main batch to complete")
            await self.sync_state.done_blocks_tx_parser_event.wait()
            self.logger.debug(f"Main batch complete. " f"Connecting headers to lock in indexing progress...")
            self.sync_state.done_blocks_tx_parser_event.clear()
            await self.connect_done_block_headers(all_pending_block_hashes.copy())
        except Exception:
            self.logger.exception("unexpected exception in " "'wait_for_batched_blocks_completion' ")

    async def load_temp_tables_for_reorg_handling(
        self, old_hashes: ChainHashes, new_hashes: ChainHashes
    ) -> None:
        assert self.mysql_db is not None
        self.mysql_db.queries.mysql_update_orphaned_headers(old_hashes)
        (
            removals_from_mempool,
            additions_to_mempool,
            orphaned_tx_hashes,
        ) = await self._get_differential_post_reorg(old_hashes, new_hashes)

        self.mysql_db.queries.mysql_load_temp_mempool_additions(additions_to_mempool)
        self.mysql_db.queries.mysql_load_temp_mempool_removals(removals_from_mempool)
        self.mysql_db.queries.mysql_load_temp_orphaned_tx_hashes(orphaned_tx_hashes)

    async def index_blocks(
        self,
        is_reorg: bool,
        start_header: bitcoinx.Header,
        stop_header: bitcoinx.Header,
        old_hashes: ChainHashes | None,
        new_hashes: ChainHashes | None,
    ) -> int:
        assert self.mysql_db is not None

        if is_reorg:
            assert old_hashes is not None
            assert new_hashes is not None
            await self.load_temp_tables_for_reorg_handling(old_hashes, new_hashes)

        conduit_tip = await self.sync_state.get_conduit_best_tip()
        remaining = conduit_tip.height - (start_header.height - 1)
        allocated_count = stop_header.height - start_header.height + 1
        self.logger.debug(
            f"Allocated {allocated_count} headers in main batch from height: "
            f"{start_header.height} to height: {stop_header.height}"
        )
        self.logger.debug(f"ConduitRaw tip height: {conduit_tip.height}. (remaining={remaining})")

        # Allocate the "MainBatch" and get the full set of "WorkUnits" (blocks broken up)
        main_batch = await self.loop.run_in_executor(
            self.general_executor,
            self.sync_state.get_main_batch,
            start_header,
            stop_header,
        )
        all_pending_block_hashes: set[bytes] = set()

        all_work_units = self.sync_state.get_work_units_all(is_reorg, all_pending_block_hashes, main_batch)

        self.tx_parser_completion_queue.put_nowait(all_pending_block_hashes.copy())

        # Mark the block hashes we have allocated work for so we can auto-db-repair if needed
        self.mysql_db.queries.update_allocated_state(
            reorg_was_allocated=is_reorg,
            first_allocated=start_header,
            last_allocated=stop_header,
            old_hashes=old_hashes,
            new_hashes=new_hashes,
        )

        # Chip away at the 'MainBatch' without exceeding configured resource constraints
        remaining_work_units = all_work_units
        while len(remaining_work_units) != 0:
            remaining_work_units = await self.chip_away(remaining_work_units)
            self.logger.debug(f"Waiting for chip away batch to complete")
            await self.sync_state.chip_away_batch_event.wait()
            self.logger.debug(f"Chip away batch completed. Remaining work units: {len(remaining_work_units)}")
            self.sync_state.chip_away_batch_event.clear()
            self.sync_state.reset_pending_chip_away_work_items()

        await self.wait_for_batched_blocks_completion(all_pending_block_hashes)
        if is_reorg:
            reorg_handling_complete = False
            await self.zmq_socket_listeners.reorg_event_socket.send(
                cbor2.dumps(
                    (
                        reorg_handling_complete,
                        start_header.hash,
                        stop_header.hash,
                    )
                )
            )

            best_flushed_tip_height = await self.sanity_checks_and_update_best_flushed_tip(is_reorg)
            self.mysql_db.tables.mysql_drop_temp_orphaned_txs()

        else:
            best_flushed_tip_height = await self.sanity_checks_and_update_best_flushed_tip(is_reorg)
        self.sync_state.reset_pending_blocks()

        if is_reorg:
            reorg_handling_complete = True
            await self.zmq_socket_listeners.reorg_event_socket.send(
                cbor2.dumps(
                    (
                        reorg_handling_complete,
                        start_header.hash,
                        stop_header.hash,
                    )
                )
            )
        return best_flushed_tip_height

    async def log_current_mempool_size_task_async(self) -> None:
        assert self.mysql_db is not None
        while True:
            self.logger.debug(f"Mempool size: {self.mysql_db.queries.get_mempool_size()} " f"transactions")
            await asyncio.sleep(60)

    async def sync_all_blocks_job(self) -> None:
        """Supervises synchronization to catch up to the block tip of ConduitRaw service"""

        # up to 500 blocks per loop
        # Now wait on the queue for notifications

        batch_id = 1
        while True:
            # ------------------------- Batch Start ------------------------- #
            # This queue is just a trigger to check the new tip and allocate another batch
            (
                is_reorg,
                start_header,
                stop_header,
                old_hashes,
                new_hashes,
            ) = await self.long_poll_conduit_raw_chain_tip()

            if stop_header.height <= self.sync_state.get_local_block_tip_height():
                continue  # drain the queue until we hit relevant ones

            self.logger.debug(f"Controller Batch {batch_id} Start")
            best_flushed_tip_height = await self.index_blocks(
                is_reorg, start_header, stop_header, old_hashes, new_hashes
            )
            self.logger.debug(
                f"Controller Batch {batch_id} Complete. " f"New tip height: {best_flushed_tip_height}"
            )
            batch_id += 1

    async def wait_for_batch_completion(self, blocks_batch_set: set[bytes]) -> None:
        """Sets chip_away_batch_event as well as done_blocks_tx_parser_event when the main batch
        is done"""
        while True:
            msg = await self.zmq_socket_listeners.socket_mined_tx_parsed_ack.recv()
            worker_id, work_item_id, block_hash, txs_done_count = cbor2.loads(msg)
            try:
                self.sync_state._work_item_progress_counter[work_item_id] += txs_done_count
                self.sync_state._blocks_progress_counter[block_hash] += txs_done_count
            except KeyError:
                raise

            try:
                if self.sync_state.have_completed_work_item(work_item_id):
                    self.sync_state.all_pending_chip_away_work_item_ids.remove(work_item_id)
                    if len(self.sync_state.all_pending_chip_away_work_item_ids) == 0:
                        self.sync_state.chip_away_batch_event.set()

                if self.sync_state.block_is_fully_processed(block_hash):
                    header = self.headers_threadsafe.get_header_for_hash(block_hash, lock=False)
                    if not block_hash in blocks_batch_set:
                        self.logger.exception(
                            f"also wrote unexpected block: "
                            f"{hash_to_hex_str(header.hash)} {header.height} to disc"
                        )
                        continue

                    blocks_batch_set.remove(block_hash)
            except KeyError:
                header = self.headers_threadsafe.get_header_for_hash(block_hash, lock=False)
                self.logger.debug(f"also parsed block: {header.height}")

            # all blocks in batch processed
            if len(blocks_batch_set) == 0:
                self.sync_state.done_blocks_tx_parser_event.set()
                break

    async def batch_completion_job(self) -> None:
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
            except Exception:
                self.logger.exception("Exception in batch_completion_job")

    def _invalidate_mempool_rows(self, best_flushed_tip: bitcoinx.Header) -> None:
        # Todo - this is actually not atomic - should be a single transaction
        assert self.mysql_db is not None
        self.mysql_db.start_transaction()
        try:
            self.mysql_db.mysql_invalidate_mempool_rows()
            self.mysql_db.mysql_update_checkpoint_tip(best_flushed_tip)
        finally:
            self.mysql_db.commit_transaction()
            self.mysql_db.mysql_drop_temp_mined_tx_hashes()

    def _apply_reorg_diff_to_mempool(self, best_flushed_block_tip: bitcoinx.Header) -> None:
        # Todo - this is actually not atomic - should be a single transaction
        assert self.mysql_db is not None
        self.mysql_db.mysql_drop_temp_mined_tx_hashes()  # not required so discard it
        self.mysql_db.start_transaction()
        try:
            self.mysql_db.queries.mysql_remove_from_mempool()
            self.mysql_db.queries.mysql_add_to_mempool()
            self.mysql_db.mysql_update_checkpoint_tip(best_flushed_block_tip)
        finally:
            self.mysql_db.commit_transaction()

    async def lagging_batch_monitor(self) -> None:
        """Spawned for each batch of blocks. If it takes more than 10 seconds to complete the
        batch of blocks, it will begin logging the state of progress"""
        timeout = 10
        last_check = time.time()
        while True:
            await asyncio.sleep(1)
            diff = time.time() - last_check
            if diff > timeout:
                if len(self.sync_state.all_pending_chip_away_work_item_ids) != 0:
                    self.sync_state.print_progress_info()
                    last_check = time.time()

    async def spawn_lagging_batch_monitor(self) -> None:
        self.tasks.append(create_task(self.lagging_batch_monitor()))
