import asyncio
import bitcoinx
from bitcoinx import hash_to_hex_str, double_sha256, MissingHeader
from confluent_kafka.cimpl import Consumer
import logging
import os
import queue
import struct
import time
from typing import Optional, Dict, List, Tuple
from concurrent.futures.thread import ThreadPoolExecutor
import multiprocessing
from pathlib import Path
import zmq

from conduit_lib.conduit_raw_api_client import ConduitRawAPIClient
from conduit_lib.database.mysql.mysql_database import MySQLDatabase, load_mysql_database
from conduit_lib.store import setup_storage
from conduit_lib.constants import WORKER_COUNT_TX_PARSERS, MsgType, NULL_HASH, \
    MAIN_BATCH_HEADERS_COUNT_LIMIT
from conduit_lib.networks import NetworkConfig
from conduit_lib.logging_server import TCPLoggingServer
from conduit_lib.wait_for_dependencies import wait_for_mysql, wait_for_kafka, \
    wait_for_conduit_raw_api
from contrib.scripts.export_blocks import GENESIS_HASH_HEX

from .batch_completion import BatchCompletionTxParser
from .sync_state import SyncState
from .types import WorkUnit
from .workers.transaction_parser import TxParser


MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
CONDUIT_RAW_API_HOST: str = os.environ.get('CONDUIT_RAW_API_HOST', 'localhost:50000')


class Controller:
    """Designed to sync the blockchain as fast as possible.

    Coordinates:
    - network via BitcoinNetIO
    - outsources parsing of block data to workers in workers.py (which work in parallel reading
    from a shared memory buffer)
    - synchronizes the refreshing of the shared memory buffer which holds multiple raw blocks)
    """

    def __init__(self, config: Dict, net_config: NetworkConfig, host="127.0.0.1", port=8000,
            logging_server_proc: TCPLoggingServer=None):
        self.running = False
        self.logging_server_proc = logging_server_proc
        self.processes = [self.logging_server_proc]
        # self.processes = []
        self.tasks = []
        self.logger = logging.getLogger("controller")
        self.loop = asyncio.get_event_loop()

        self.config = config  # cli args & env_vars

        # Defined in async method at startup (self.run)
        self.storage = None
        self.handlers = None  # Handlers(self, self.net_config, self.storage)
        self.serializer = None  # Serializer(self.net_config, self.storage)
        self.deserializer = None  # Deserializer(self.net_config, self.storage)

        self.net_config = net_config

        # IPC from Controller to TxParser
        context1 = zmq.Context()
        self.mined_tx_socket = context1.socket(zmq.PUSH)
        self.mined_tx_socket.bind("tcp://127.0.0.1:55555")

        # IPC from Controller to TxParser
        context2 = zmq.Context()
        self.mempool_tx_socket = context2.socket(zmq.PUSH)
        self.mempool_tx_socket.bind("tcp://127.0.0.1:55556")

        # PUB-SUB from Controller to worker to kill the worker
        context3 = zmq.Context()
        self.kill_worker_socket = context3.socket(zmq.PUB)
        self.kill_worker_socket.bind("tcp://127.0.0.1:63241")

        self.worker_ack_queue_tx_parse_confirmed = multiprocessing.Queue()  # blk_hash:tx_count
        self.worker_ack_queue_mined_tx_hashes = multiprocessing.Queue()  # blk_height:tx_hashes

        # Batch Completion
        self.tx_parser_completion_queue = queue.Queue()

        self.global_tx_hashes_dict = {}  # blk_height:tx_hashes
        self.mined_tx_hashes_queue_waiter_executor = ThreadPoolExecutor(max_workers=1)
        # self.worker_ack_queue_tx_parse_mempool = multiprocessing.Queue()  # tx_count

        # Database Interfaces
        self.mysql_db: Optional[MySQLDatabase] = None

        self.lmdb_grpc_client: Optional[ConduitRawAPIClient] = None

        self.total_time_connecting_headers = 0

        self.sync_state: Optional[SyncState] = None
        self.headers_state_consumer: Optional[Consumer] = None
        self.headers_state_consumer_executor = ThreadPoolExecutor(max_workers=1)
        self.batch_completion_raw: Optional[BatchCompletionTxParser]

    def setup_kafka_consumer(self):
        # Todo: the group id should actually only change on --reset to refill the local headers
        #  store. Otherwise it should not have to re-pull old headers again (and filter them)
        #  Probably should store this group id number in MySQL somewhere
        group = os.urandom(8)
        self.headers_state_consumer: Consumer = Consumer({
            'bootstrap.servers': os.environ.get('KAFKA_HOST', "127.0.0.1:26638"),
            'group.id': group,
            'auto.offset.reset': 'earliest'
        })
        self.headers_state_consumer.subscribe(['conduit-raw-headers-state'])

    async def setup(self):
        headers_dir = MODULE_DIR.parent
        self.storage = setup_storage(self.config, self.net_config, headers_dir)
        self.sync_state = SyncState(self.storage, self)
        self.mysql_db = load_mysql_database()
        self.lmdb_grpc_client = ConduitRawAPIClient()

        # Drop mempool table for now and re-fill - easiest brute force way to achieve consistency
        self.mysql_db.tables.mysql_drop_mempool_table()
        self.mysql_db.tables.mysql_create_permanent_tables()

        self.batch_completion_raw = BatchCompletionTxParser(
            self.storage,
            self.sync_state,
            self.worker_ack_queue_tx_parse_confirmed,
            self.tx_parser_completion_queue
        )
        self.batch_completion_raw.start()

    async def run(self):
        self.running = True
        try:
            await wait_for_conduit_raw_api(conduit_raw_api_host=CONDUIT_RAW_API_HOST)
            await wait_for_kafka(kafka_host=self.config['kafka_host'])
            # Must setup kafka consumer after conduit_raw_api is ready in case conduit_raw
            # does a full kafka reset
            self.setup_kafka_consumer()
            await wait_for_mysql(mysql_host=self.config['mysql_host'])
            await self.setup()
            await self.start_jobs()
            # Allow time for TxParsers to bind to the zmq socket to distribute load evenly
            time.sleep(2)
            while True:
                await asyncio.sleep(0.5)
        except Exception:
            self.logger.exception("unexpected exception in Controller.run")
        finally:
            await self.stop()

    async def stop(self):
        self.running = False
        try:
            if self.storage:
                await self.storage.close()

            with self.kill_worker_socket as sock:
                sock.send(b"stop_signal")

            await asyncio.sleep(1)

            for p in self.processes:
                p.terminate()
                p.join()

            self.sync_state._batched_blocks_exec.shutdown(wait=False)

            for task in self.tasks:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
        except Exception:
            self.logger.exception("suppressing raised exceptions on cleanup")

    def run_coro_threadsafe(self, coro, *args, **kwargs):
        asyncio.run_coroutine_threadsafe(coro(*args, **kwargs), self.loop)

    # Multiprocessing Workers
    def start_workers(self):
        for worker_id in range(WORKER_COUNT_TX_PARSERS):
            p = TxParser(
                worker_id+1,
                self.worker_ack_queue_tx_parse_confirmed,
                self.worker_ack_queue_mined_tx_hashes,
            )
            p.start()
            self.processes.append(p)

    async def spawn_sync_all_blocks_job(self):
        """runs once at startup and is re-spawned for new unsolicited block tips"""
        self.tasks.append(asyncio.create_task(self.sync_all_blocks_job()))

    def maybe_rollback(self):
        """This will have to do a full table scan to find all the transactions with height above
        what was safetly flushed to disc... It will not be fast at scan but is better than
        re-syncing the whole chain. It can be thought of as a last resort db repair process."""
        # Safe block tip height
        last_good_block_height = self.sync_state.get_local_block_tip_height()
        self.logger.debug(f"last_good_block_height={last_good_block_height}")

        # Get max flushed tx height
        max_flushed_tx_height = self.mysql_db.queries.mysql_get_max_tx_height()
        if not max_flushed_tx_height:
            return
        self.logger.debug(f"max_flushed_tx_height={max_flushed_tx_height}")
        if max_flushed_tx_height == last_good_block_height:
            return

        # Check for any "Unsafe" flushed txs (above the safe tip height)
        unsafe_tx_rows = self.mysql_db.queries.mysql_get_txids_above_last_good_height(
            last_good_block_height)

        # Do rollback
        self.mysql_db.queries.mysql_rollback_unsafe_txs(unsafe_tx_rows)

    async def start_jobs(self):
        try:
            self.mysql_db: MySQLDatabase = self.storage.mysql_database
            self.maybe_rollback()
            self.start_workers()
            # Allow time for TxParser workers to bind to ZMQ socket...
            await self.spawn_sync_all_blocks_job()
        except Exception as e:
            self.logger.exception("unexpected exception in start jobs")
            raise

    def get_header_for_height(self, height: int) -> bitcoinx.Header:
        headers = self.storage.headers
        chain = self.storage.headers.longest_chain()
        header = headers.header_at_height(chain, height)
        return header

    async def update_mempool_and_api_tip_atomic(self, api_block_tip_height, api_block_tip_hash):
        # Get all tx_hashes up to api_block_tip_height
        # Must ATOMICALLY:
        #   1) invalidate mempool rows (that have been mined)
        #   2) update api chain tip

        # 1) Drain queue
        new_mined_tx_hashes = {}
        # Todo - Relying on multiprocessing queue.empty() may be fragile...
        #  However, probably okay for now given step-wise design/pipeline
        while not self.worker_ack_queue_mined_tx_hashes.empty():
            new_mined_tx_hashes = await self.loop.run_in_executor(
                self.mined_tx_hashes_queue_waiter_executor,
                self.worker_ack_queue_mined_tx_hashes.get)
        # 2) Update dict
        for blk_height, new_hashes in new_mined_tx_hashes.items():
            if not self.global_tx_hashes_dict.get(blk_height):
                self.global_tx_hashes_dict[blk_height] = []
            self.global_tx_hashes_dict[blk_height].extend(new_hashes)

        # 3) Transfer from dict -> Temp Table for Table join with Mempool table
        for blk_height, new_mined_tx_hashes in new_mined_tx_hashes.items():
            if blk_height <= api_block_tip_height:
                mined_tx_hashes = []
                for tx_hash in self.global_tx_hashes_dict[blk_height]:
                    mined_tx_hashes.append((tx_hash.hex(), blk_height))

                self.mysql_db.mysql_load_temp_mined_tx_hashes(mined_tx_hashes=mined_tx_hashes)
                del self.global_tx_hashes_dict[blk_height]

        # 4) Invalidate mempool rows (that have been mined) ATOMICALLY
        self.logger.debug(f"Invalidating relevant mempool transactions...")
        self.mysql_db.start_transaction()
        try:
            self.mysql_db.mysql_invalidate_mempool_rows(api_block_tip_height)
            self.mysql_db.mysql_drop_temp_mined_tx_hashes()
            self.mysql_db.mysql_update_api_tip_height_and_hash(api_block_tip_height,
                api_block_tip_hash)
        finally:
            self.mysql_db.commit_transaction()

    async def sanity_checks_and_update_api_tip(self):
        api_block_tip_height = self.sync_state.get_local_block_tip_height()
        api_block_tip = self.get_header_for_height(api_block_tip_height)
        api_block_tip_hash = api_block_tip.hash

        await self.update_mempool_and_api_tip_atomic(api_block_tip_height, api_block_tip_hash)
        return api_block_tip_height

    def connect_done_block_headers(self, blocks_batch_set):
        t0 = time.perf_counter()
        sorted_headers = sorted([(self.storage.get_header_for_hash(h).height, h) for h in
            blocks_batch_set])
        sorted_heights = [height for height, h in sorted_headers]
        # Assert the resultant blocks are consecutive with no gaps
        max_height = max(sorted_heights)
        # self.logger.debug(f"block_heights={block_heights}")

        # Check for gaps
        expected_block_heights = [i+1 for i in range(max_height-len(sorted_heights), max_height)]
        # self.logger.debug(f"expected_block_heights={expected_block_heights}")
        assert sorted_heights == expected_block_heights

        block_headers: bitcoinx.Headers = self.storage.block_headers
        for height, hash in sorted_headers:
            # self.logger.debug(f"new block tip height: {height}")
            header = self.storage.get_header_for_hash(hash)
            block_headers.connect(header.raw)

        self.mysql_db.bulk_loads.set_rocks_db_bulk_load_off()
        self.storage.block_headers.flush()
        # ? Add reorg and other sanity checks later here...

        tip = self.sync_state.get_local_block_tip()
        self.logger.debug(f"Connected up to header.hash, header.height = "
                          f"{tip.height, hash_to_hex_str(tip.hash)}")
        t1 = time.perf_counter() - t0
        self.total_time_connecting_headers += t1
        self.logger.debug(f"Total time connecting headers: {self.total_time_connecting_headers}")

    def connect_conduit_headers(self, raw_header) -> bool:
        """Two mmap files - one for "headers-first download" and the other for the
        blocks we then download."""
        try:
            self.storage.headers.connect(raw_header)
            self.storage.headers.flush()
            return True
        except MissingHeader as e:
            if str(e).find(GENESIS_HASH_HEX) != -1 or str(e).find(NULL_HASH) != -1:
                # self.logger.debug("skipping - prev_out == genesis block")
                return True
            else:
                self.logger.exception(e)
                raise

    def long_poll_conduit_raw_chain_tip(self) -> Tuple[bitcoinx.Header, int]:
        tip, tip_height = self.lmdb_grpc_client.get_chain_tip()
        deserialized_header = None
        while True:
            try:
                start_height = self.sync_state.get_local_block_tip_height() + 1

                # Long-polling
                result = self.lmdb_grpc_client.get_block_headers_batched(start_height,
                    batch_size=MAIN_BATCH_HEADERS_COUNT_LIMIT, wait_for_ready=True)

                for new_tip in result:
                    self.connect_conduit_headers(new_tip)

                    # For debugging only
                    tip_hash = double_sha256(new_tip)
                    deserialized_header = self.storage.get_header_for_hash(tip_hash)

                if deserialized_header:
                    self.logger.debug(f"Got new tip from ConduitRaw service for "
                                      f"parsing at height: {deserialized_header.height}")
                    return deserialized_header, tip_height
                else:
                    continue
            except Exception:
                self.logger.exception("unexpected exception in long_poll_conduit_raw_chain_tip")
                time.sleep(0.2)


    def push_chip_away_work(self, work_units: List[WorkUnit]) -> None:
        # Push to workers only a subset of the 'full_batch_for_deficit' to chip away
        for part_size, block_hash, block_height, first_tx_pos_batch, part_end_offset, \
                tx_offsets_array in work_units:
            len_arr = len(tx_offsets_array) * 8  # 8 byte uint64_t
            packed_array = tx_offsets_array.tobytes()
            packed_msg = struct.pack(f"<II32sIIQ{len_arr}s", MsgType.MSG_BLOCK, len_arr, block_hash,
                block_height, first_tx_pos_batch, part_end_offset, packed_array)

            self.mined_tx_socket.send(packed_msg)

    async def chip_away(self, remaining_work_units: List[WorkUnit]):
        """This breaks up blocks into smaller 'WorkUnits'. This allows tailoring workloads and
        max memory allocations to be safe with available system resources)"""
        remaining_work, work_for_this_batch = \
            self.sync_state.get_work_units_chip_away(remaining_work_units)
        self.push_chip_away_work(work_for_this_batch)
        return remaining_work

    async def sync_all_blocks_job(self):
        """Supervises synchronization to catch up to the block tip of ConduitRaw service"""

        async def wait_for_batched_blocks_completion(batch_id, all_pending_block_hashes) -> None:
            """all_pending_block_hashes is copied into these threads to prevent mutation"""
            try:
                await self.sync_state.done_blocks_tx_parser_event.wait()
                self.sync_state.done_blocks_tx_parser_event.clear()
                self.connect_done_block_headers(all_pending_block_hashes.copy())
            except Exception:
                self.logger.exception("unexpected exception in 'wait_for_batched_blocks_completion' ")

        async def maintain_chain_tip():
            # Now wait on the queue for notifications

            batch_id = 0
            while True:
                # ------------------------- Batch Start ------------------------- #
                # This queue is just a trigger to check the new tip and allocate another batch
                main_batch_tip, conduit_raw_tip_height = await self.loop.run_in_executor(
                    self.headers_state_consumer_executor, self.long_poll_conduit_raw_chain_tip)

                deficit = main_batch_tip.height - self.sync_state.get_local_block_tip_height()
                local_tip_height = self.sync_state.get_local_block_tip_height()
                remaining = conduit_raw_tip_height - local_tip_height
                self.logger.debug(f"Allocated {deficit} headers in main batch to height: "
                    f"{main_batch_tip.height}")
                self.logger.debug(f"ConduitRaw tip height: {conduit_raw_tip_height}. "
                                  f"Local tip height: {local_tip_height} "
                                  f"(remaining={remaining})")
                if main_batch_tip.height <= self.sync_state.get_local_block_tip_height():
                    continue  # drain the queue until we hit relevant ones

                batch_id += 1
                self.logger.debug(f"Controller Batch {batch_id} Start")

                # Allocate the "MainBatch" and get the full set of "WorkUnits" (blocks broken up)
                all_pending_block_hashes, main_batch = \
                    self.sync_state.get_main_batch(main_batch_tip)
                all_work_units = self.sync_state.get_work_units_all(all_pending_block_hashes,
                    main_batch)
                self.tx_parser_completion_queue.put_nowait(all_pending_block_hashes.copy())


                # Chip away at the 'MainBatch' without exceeding configured resource constraints
                remaining_work_units = all_work_units
                while len(remaining_work_units) != 0:
                    remaining_work_units = await self.chip_away(remaining_work_units)
                    await self.sync_state.chip_away_batch_event.wait()
                    self.sync_state.reset_pending_chip_away_work_items()
                    self.sync_state.chip_away_batch_event.clear()



                await wait_for_batched_blocks_completion(batch_id, all_pending_block_hashes)
                self.sync_state.reset_pending_blocks()
                api_block_tip_height = await self.sanity_checks_and_update_api_tip()
                self.logger.debug(f"maintain_chain_tip - new block tip height: {api_block_tip_height}")
                # ------------------------- Batch complete ------------------------- #
                self.logger.debug(f"Controller Batch {batch_id} Complete. "
                    f"New tip height: {self.sync_state.get_local_block_tip_height()}")

        try:
            # up to 500 blocks per loop
            await maintain_chain_tip()

        except asyncio.CancelledError:
            raise
        except Exception as e:
            self.logger.exception("sync_blocks_job raised an exception")
            raise

