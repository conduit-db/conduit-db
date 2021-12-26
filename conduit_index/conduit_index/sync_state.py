import asyncio
import logging
import multiprocessing
import threading
import time
import typing
from datetime import datetime, timedelta
from typing import Set, List, Tuple, Dict
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Optional

import bitcoinx
from bitcoinx import hash_to_hex_str, unpack_header, double_sha256

from conduit_lib.ipc_sock_client import IPCSocketClient
from conduit_lib.constants import SMALL_BLOCK_SIZE, CHIP_AWAY_BYTE_SIZE_LIMIT
from conduit_lib.store import Storage
from .load_balance_algo import distribute_load
from .types import WorkUnit, MainBatch
from array import array
from bitcoinx.networks import Header

if typing.TYPE_CHECKING:
    from .controller import Controller


class SyncState:
    """
    Specific to ConduitRaw's sync state - Ensures that each batch is completed safely,
    sanity checks are completed (including reorg handling) and everything is flushed to disc
    before the network io shared memory buffer can be reset/mutated and before then next block
    can be requested from the node.

    NOTE: ConduitIndex actually tracks three sets of headers!
    - 1) All headers to node tip
    - 2) Fully processed headers (to know how far indexing of the full chain has progressed)
    - 3) The state of ConduitRaw - just wants to know the current chain tip so that ConduitIndexer
    will only allocate batches for which the raw block data has already been processed.

    These three types are dubbed "headers", "block_headers" and "conduit_raw_headers" respectively
    The idea is that 'headers' will always be ahead, followed by 'conduit_raw_headers' and then
    finally, 'block_headers' will be bringing up the rear as the most heavy process playing catch-up
    """

    def __init__(self, storage: Storage, controller: 'Controller') -> None:
        self.logger = logging.getLogger("sync-state")
        self.storage = storage
        self.controller = controller
        self.ipc_sock_client: Optional[IPCSocketClient] = None

        self.conduit_raw_header_tip: Optional[bitcoinx.Header] = None
        self.conduit_raw_header_tip_lock: threading.Lock = threading.Lock()

        # Not actually used by Indexer but cannot remove because it's in the shared lib
        self.target_header_height: Optional[int] = None
        self.target_block_header_height: Optional[int] = None
        self.local_block_tip_height: int = self.get_local_block_tip_height()
        self.initial_block_download_event = asyncio.Event()  # start requesting mempool txs

        # Accounting and ack'ing for non-block msgs
        self.incoming_msg_queue: asyncio.Queue[Tuple[bytes, memoryview]] = asyncio.Queue()
        self._msg_received_count = 0
        self._msg_handled_count = 0
        self._msg_received_count_lock = threading.Lock()
        self._msg_handled_count_lock = threading.Lock()

        # Accounting and ack'ing for block msgs
        self.blocks_batch_set: Set[bytes] = set()  # usually a set of 500 hashes
        self.expected_blocks_tx_counts: Dict[bytes, int] = {}  # blk_hash: total_tx_count
        self._blocks_progress_counter: Dict[bytes, int] = {}  # block_hash: txs_done_count

        # Accounting and ack'ing for chip away block msgs
        self.all_pending_chip_away_work_item_ids: Set[int] = set()  # set of work_item_ids
        self.expected_work_item_tx_counts: Dict[int, int] = {}  # work_item_id: total_tx_count
        self._work_item_progress_counter: Dict[int, int] = {}  # work_item_id: txs_done_count
        self.chip_away_batch_event = asyncio.Event()

        # Accounting and ack'ing for block msgs
        self.all_pending_block_hashes: Set[bytes] = set()  # usually a set of 500 hashes during IBD
        self.received_blocks: Set[bytes] = set()  # must process before network buffer reset
        self.done_blocks_tx_parser: Set[bytes] = set()
        self.done_blocks_tx_parser_lock = threading.Lock()
        self.done_blocks_tx_parser_event = asyncio.Event()

        self._batched_blocks_exec = ThreadPoolExecutor(1, "join-batched-blocks")
        self.initial_block_download_event_mp = multiprocessing.Event()

        self.total_time_allocating_work = 0.
        self.is_post_ibd = False
        self.conduit_best_tip: Optional[bitcoinx.Header] = None

    @property
    def message_received_count(self):
        return self._msg_received_count

    @property
    def message_handled_count(self):
        return self._msg_handled_count

    # Conduit Best Tip
    async def get_conduit_best_tip(self) -> bitcoinx.Header:
        ipc_sock_client = None
        try:
            ipc_sock_client = await self.controller.loop.run_in_executor(
                self.controller.general_executor, IPCSocketClient)
            tip, tip_height = await self.controller.loop.run_in_executor(
                self.controller.general_executor, ipc_sock_client.chain_tip)
            conduit_best_tip = bitcoinx.Header(*unpack_header(tip), double_sha256(tip), tip, tip_height)
            return conduit_best_tip
        finally:
            if ipc_sock_client:
                ipc_sock_client.close()

    def get_local_tip(self) -> Header:
        """In ConduitIndex, this set of headers is used differently. We only fetch and connect
        what is needed to assist us with processing the next set of raw blocks. This is in contrast
        to ConduitRaw which fetches ALL headers before doing anything else."""
        with self.storage.headers_lock:
            return self.storage.headers.longest_chain().tip

    # Block Headers
    def get_local_block_tip_height(self) -> int:
        with self.storage.block_headers_lock:
            return self.storage.block_headers.longest_chain().tip.height

    def get_local_block_tip(self) -> bitcoinx.Header:
        with self.storage.block_headers_lock:
            return self.storage.block_headers.longest_chain().tip

    async def is_ibd(self, tip: bitcoinx.Header, conduit_best_tip: bitcoinx.Header) -> bool:
        # Todo - really instead of conduit_best_tip it should come from the node...
        #  because it affects compact block protocol down the road...Other than that
        #  the logic seems correct... to fix this would need to add back headers synchronization
        #  like ConduitRaw does...
        if self.is_post_ibd:  # cache result
            return self.is_post_ibd
        conduit_best = datetime.utcfromtimestamp(conduit_best_tip.timestamp)
        our_tip = datetime.utcfromtimestamp(tip.timestamp)
        conduit_best_minus_24_hrs = conduit_best - timedelta(hours=24)
        if our_tip > conduit_best_minus_24_hrs:
            self.is_post_ibd = True
            return True
        return False

    def set_target_header_height(self, height: int) -> None:
        self.target_header_height = height

    def have_processed_non_block_msgs(self) -> bool:
        return self._msg_received_count == self._msg_handled_count

    def have_processed_block_msgs(self) -> bool:
        self.logger.debug(f"len(self.done_blocks_raw)={len(self.done_blocks_tx_parser)}")
        self.logger.debug(f"len(self.received_blocks)={len(self.received_blocks)}")

        with self.done_blocks_tx_parser_lock:
            for blk_hash in self.done_blocks_tx_parser:
                if not blk_hash in self.received_blocks:
                    return False
            if len(self.done_blocks_tx_parser) != len(self.received_blocks):
                return False

        return True

    def have_processed_all_msgs_in_buffer(self):
        return self.have_processed_non_block_msgs() and self.have_processed_block_msgs()

    def reset_msg_counts(self):
        with self._msg_handled_count_lock:
            self._msg_handled_count = 0
        with self._msg_received_count_lock:
            self._msg_received_count = 0

    def get_work_units_all(self, is_reorg: bool, all_pending_block_hashes: Set[bytes],
            all_work: MainBatch) -> List[WorkUnit]:
        """is_reorg means that this block hash was one of the ones affected by the reorg."""

        t0 = time.perf_counter()
        try:
            all_work_units = []
            work_item_id = 0
            for i, work in enumerate(all_work):
                block_size, tx_offsets, block_header, block_num = work
                needs_breaking_up = \
                    block_size > SMALL_BLOCK_SIZE or block_size > CHIP_AWAY_BYTE_SIZE_LIMIT

                first_tx_pos_batch = 0
                if needs_breaking_up:
                    tx_count = len(tx_offsets)
                    divided_work = distribute_load(block_header.hash, block_num, tx_count,
                        block_size, tx_offsets)

                    for work_part in divided_work:
                        size_of_part, blk_hash, block_num, first_tx_pos_batch, part_end_offset, \
                            work_part_tx_offsets = work_part

                        # Adding work_item_id to WorkPart -> WorkUnit
                        work_item: WorkUnit = is_reorg, size_of_part, work_item_id, blk_hash, \
                            block_num, first_tx_pos_batch, part_end_offset, work_part_tx_offsets
                        all_work_units.append(work_item)
                        work_item_id += 1
                        # self.logger.debug(f"divided_work={divided_work}")
                else:
                    part_end_offset = block_size
                    size_of_part = block_size
                    all_work_units.append((is_reorg, size_of_part, work_item_id, block_header.hash,
                        block_num, first_tx_pos_batch, part_end_offset, tx_offsets))
                    work_item_id += 1

                all_pending_block_hashes.add(block_header.hash)
                self.add_pending_block(block_header.hash, len(tx_offsets))

            return all_work_units
        finally:
            t1 = time.perf_counter() - t0
            self.total_time_allocating_work += t1
            self.logger.debug(f"total time allocating work: {self.total_time_allocating_work}")

    def get_work_units_chip_away(self, remaining_work_units: List[WorkUnit]) \
            -> Tuple[List[WorkUnit], List[WorkUnit]]:
        """When limits are reached this function can merely return back to caller and this
        function will be called repeatedly until 'all_pending_block_hashes' have been fully
        consumed.
        Todo:   Test for 1 x 4GB transaction - what happens if it exceeds CHIP_AWAY_BYTE_SIZE_LIMIT?
                Test for 1 x 4GB block with CHIP_AWAY_BYTE_SIZE_LIMIT == 40MB -> BATCH_COUNT = 100
        """
        total_allocation = 0
        remaining_work = []
        work_for_this_batch = []

        for idx, work_unit in enumerate(remaining_work_units):
            is_reorg, size_of_part, work_item_id, blk_hash, blk_num, first_tx_pos_batch, part_end_offset, \
                tx_offsets = work_unit

            # self.logger.debug(f"work_unit={work_unit}")
            reached_limit = \
                total_allocation + size_of_part > CHIP_AWAY_BYTE_SIZE_LIMIT

            if reached_limit:
                remaining_work.append(work_unit)
                continue

            total_allocation += size_of_part
            work_for_this_batch.append(work_unit)
            self.add_pending_chip_away_work_item(work_item_id, len(tx_offsets))

        return remaining_work, work_for_this_batch

    def get_main_batch(self, start_header: bitcoinx.Header, stop_header: bitcoinx.Header) \
            -> MainBatch:
        """Must run in thread due to ipc_sock_client not being async"""
        try:
            ipc_sock_client = IPCSocketClient()

            with self.storage.headers_lock:
                block_height_deficit = stop_header.height - (start_header.height - 1)

                # May need to use block_hash not height to be more correct
                block_headers: List[bitcoinx.Header] = []
                for i in range(1, block_height_deficit + 1):
                    block_header = self.controller.get_header_for_height(start_header.height - 1 + i)
                    block_headers.append(block_header)

                header_hashes = [block_header.hash for block_header in block_headers]

            # Todo: All four of these network calls could be done in parallel in a thread pool executor

            block_metadata_batch = ipc_sock_client.block_metadata_batched(header_hashes).block_metadata_batch
            block_sizes_batch = [block_metadata.block_size for block_metadata in block_metadata_batch]
            tx_offsets_results = ipc_sock_client.transaction_offsets_batched(header_hashes)
            block_numbers = ipc_sock_client.block_number_batched(header_hashes).block_numbers

            all_work_units = list(zip(block_sizes_batch, tx_offsets_results, block_headers,
                block_numbers))
            return all_work_units
        except Exception as e:
            self.logger.exception(f"Unexpected exception in get_main_batch thread")
            raise

    def incr_msg_received_count(self) -> None:
        with self._msg_received_count_lock:
            self._msg_received_count += 1

    def incr_msg_handled_count(self) -> None:
        with self._msg_handled_count_lock:
            self._msg_handled_count += 1

    def readout_sync_state(self):
        self.logger.error(f"A blockage in the pipeline is suspected and needs diagnosing.")
        self.logger.error(f"Controller State:")
        self.logger.error(f"-----------------")
        self.logger.error(f"msg_received_count={self._msg_handled_count}")
        self.logger.error(f"msg_handled_count={self._msg_handled_count}")

        self.logger.debug(f"len(self.received_blocks)={len(self.received_blocks)}")
        self.logger.debug(f"len(self.done_blocks_tx_parser)={len(self.done_blocks_tx_parser)}")

        for blk_hash in self.received_blocks:
            self.logger.error(f"received blk_hash={hash_to_hex_str(blk_hash)}")

        for blk_hash in self.done_blocks_tx_parser:
            self.logger.error(f"done_blocks blk_hash={hash_to_hex_str(blk_hash)}")

    # ----- SUPERVISE BATCH COMPLETION ----- #
    # Maximum number of blocks in this batch is 'MAIN_BATCH_HEADERS_COUNT_LIMIT'

    def block_is_fully_processed(self, block_hash: bytes) -> bool:
        return self._blocks_progress_counter[block_hash] == \
               self.expected_blocks_tx_counts[block_hash]

    def add_pending_block(self, blk_hash: bytes, total_tx_count: int) -> None:
        self.expected_blocks_tx_counts[blk_hash] = total_tx_count
        self._blocks_progress_counter[blk_hash] = 0

    def reset_pending_blocks(self):
        self._blocks_progress_counter = {}
        self.expected_work_item_tx_counts = {}
        self.expected_blocks_tx_counts = {}

    # ----- SUPERVISE 'CHIP AWAY' BATCH COMPLETIONS ----- #
    # This batch can be as small as system resources need it to be (even small parts of a block)

    def have_completed_work_item(self, work_item_id: int) -> bool:
        return self._work_item_progress_counter[work_item_id] == \
               self.expected_work_item_tx_counts[work_item_id]


    def add_pending_chip_away_work_item(self, work_item_id: int, work_item_tx_count: int) -> None:
        def init_counters_if_needed():
            if not self.expected_work_item_tx_counts.get(work_item_id):
                self.expected_work_item_tx_counts[work_item_id] = 0
            if not self._work_item_progress_counter.get(work_item_id):
                self._work_item_progress_counter[work_item_id] = 0

        init_counters_if_needed()
        self.expected_work_item_tx_counts[work_item_id] += work_item_tx_count
        self.all_pending_chip_away_work_item_ids.add(work_item_id)


    def reset_pending_chip_away_work_items(self) -> None:
        self._work_item_progress_counter = {}

    # ----- AVOID MEMPOOL-INDUCED THRASHING DURING IBD ----- #

    def is_post_IBD(self):
        """has initial block download been completed?
        (to syncronize all blocks with all initially fetched headers)"""
        return self.initial_block_download_event.is_set()

    def set_post_IBD_mode(self):
        self.initial_block_download_event.set()  # once set the first time will stay set
        self.initial_block_download_event_mp.set()
