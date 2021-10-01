import array
import asyncio
import logging
import multiprocessing
import os
import threading
import time
import typing
from typing import Set, List, Tuple
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Optional

import bitcoinx
from bitcoinx import Headers, hash_to_hex_str

# from conduit_lib.conduit_raw_api_client import ConduitRawAPIClient
from conduit_lib.conduit_raw_api_client import ConduitRawAPIClient
from conduit_lib.constants import SMALL_BLOCK_SIZE, CHIP_AWAY_BYTE_SIZE_LIMIT
from conduit_lib.store import Storage
from conduit_lib.utils import cast_to_valid_ipv4
from .load_balance_algo import distribute_load
from .types import WorkUnit, MainBatch

if typing.TYPE_CHECKING:
    from .controller import Controller


try:
    CONDUIT_RAW_API_HOST: str = os.environ.get('CONDUIT_RAW_API_HOST', 'localhost:50000')
    CONDUIT_RAW_HOST = cast_to_valid_ipv4(CONDUIT_RAW_API_HOST.split(":")[0])
    CONDUIT_RAW_PORT = int(CONDUIT_RAW_API_HOST.split(":")[1])
except Exception:
    logger = logging.getLogger('sync-state-env-vars')
    logger.exception("unexpected exception")



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

    def __init__(self, storage: Storage, controller: 'Controller'):
        self.logger = logging.getLogger("sync-state")
        self.storage = storage
        self.controller = controller
        self.lmdb_grpc_client: Optional[ConduitRawAPIClient] = None

        self.conduit_raw_header_tip: bitcoinx.Header = None
        self.conduit_raw_header_tip_lock: threading.Lock = threading.Lock()

        # Not actually used by Indexer but cannot remove because it's in the shared lib
        self.target_header_height: Optional[int] = None
        self.target_block_header_height: Optional[int] = None
        self.local_block_tip_height: int = self.get_local_block_tip_height()
        self.initial_block_download_event = asyncio.Event()  # start requesting mempool txs

        # Accounting and ack'ing for non-block msgs
        self.incoming_msg_queue = asyncio.Queue()
        self._msg_received_count = 0
        self._msg_handled_count = 0
        self._msg_received_count_lock = threading.Lock()
        self._msg_handled_count_lock = threading.Lock()

        # Accounting and ack'ing for block msgs
        self.blocks_batch_set = set()  # usually a set of 500 hashes
        self.expected_blocks_tx_counts = {}  # blk_hash: total_tx_count
        self._pending_blocks_progress_counter = {}

        # Accounting and ack'ing for chip away block msgs
        self.all_pending_chip_away_block_hashes = set()  # usually a set of 500 hashes
        self.expected_blocks_tx_counts_chip_away = {}  # blk_hash: total_tx_count
        self._pending_blocks_progress_counter_chip_away = {}
        self.chip_away_batch_event = asyncio.Event()

        # Accounting and ack'ing for block msgs
        self.all_pending_block_hashes = set()  # usually a set of 500 hashes during IBD
        self.received_blocks = set()  # received in network buffer - must process before buf reset
        self.done_blocks_tx_parser = set()
        self.done_blocks_tx_parser_lock = threading.Lock()
        self.done_blocks_tx_parser_event = asyncio.Event()

        self._batched_blocks_exec = ThreadPoolExecutor(1, "join-batched-blocks")
        self._grpc_exec = ThreadPoolExecutor(10, 'grpc-exec')
        self.initial_block_download_event_mp = multiprocessing.Event()

        self.total_time_allocating_work = 0

    @property
    def message_received_count(self):
        return self._msg_received_count

    @property
    def message_handled_count(self):
        return self._msg_handled_count

    # Block Headers
    def get_local_block_tip_height(self) -> int:
        return self.storage.block_headers.longest_chain().tip.height

    def get_local_block_tip(self) -> bitcoinx.Header:
        return self.storage.block_headers.longest_chain().tip

    def set_target_header_height(self, height) -> None:
        self.target_header_height = height

    def have_processed_non_block_msgs(self) -> bool:
        return self._msg_received_count == self._msg_handled_count

    def have_processed_block_msgs(self) -> bool:
        self.logger.debug(f"len(self.done_blocks_raw)={len(self.done_blocks_tx_parser)}")
        self.logger.debug(f"len(self.received_blocks)={len(self.received_blocks)}")
        self.logger.debug(f"self.controller.worker_ack_queue_preproc.qsize()="
                          f"{self.controller.worker_ack_queue_tx_parse_confirmed.qsize()}")


        with self.done_blocks_tx_parser_lock:
            for blk_hash in self.done_blocks_tx_parser:
                if not blk_hash in self.received_blocks:
                    return False
            if len(self.done_blocks_tx_parser) != len(self.received_blocks):
                return False

        return True

    def have_processed_all_msgs_in_buffer(self):
        return self.have_processed_non_block_msgs() and self.have_processed_block_msgs()

    def is_synchronized(self):
        """Synchronize against ConduitRaw"""
        return self.get_local_block_tip_height() >= self.get_conduit_raw_header_tip().height

    def reset_msg_counts(self):
        with self._msg_handled_count_lock:
            self._msg_handled_count = 0
        with self._msg_received_count_lock:
            self._msg_received_count = 0

    def get_work_units_all(self, all_pending_block_hashes: Set[bytes], all_work: MainBatch) \
            -> List[WorkUnit]:

        t0 = time.perf_counter()
        try:
            all_work_units = []
            for i, work in enumerate(all_work):
                block_size, tx_offsets, block_header = work
                needs_breaking_up = \
                    block_size > SMALL_BLOCK_SIZE or block_size > CHIP_AWAY_BYTE_SIZE_LIMIT

                first_tx_pos_batch = 0
                if needs_breaking_up:
                    tx_count = len(tx_offsets)
                    divided_work = distribute_load(block_header.hash, block_header.height, tx_count,
                        block_size, tx_offsets)

                    for work_part in divided_work:
                        size_of_part, blk_hash, blk_height, first_tx_pos_batch, part_end_offset, \
                            tx_offsets = work_part
                        all_work_units.append(work_part)
                        # self.logger.debug(f"divided_work={divided_work}")
                else:
                    part_end_offset = block_size
                    size_of_part = block_size
                    all_work_units.append((size_of_part, block_header.hash, block_header.height,
                        first_tx_pos_batch, part_end_offset, tx_offsets))

                all_pending_block_hashes.add(block_header.hash)
                self.add_pending_block(block_header.hash, len(tx_offsets))
            return all_work_units
        finally:
            t1 = time.perf_counter() - t0
            self.total_time_allocating_work += t1
            self.logger.debug(f"total time allocating work: {self.total_time_allocating_work}")

    def get_work_units_chip_away(self, all_work_units: List[WorkUnit]) \
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
        for idx, work_unit in enumerate(all_work_units):
            size_of_part, blk_hash, blk_height, first_tx_pos_batch, part_end_offset, \
                tx_offsets = work_unit

            # self.logger.debug(f"work_unit={work_unit}")
            reached_limit = \
                total_allocation + size_of_part > CHIP_AWAY_BYTE_SIZE_LIMIT

            if reached_limit:
                remaining_work.append(work_unit)
                continue

            total_allocation += size_of_part
            work_for_this_batch.append(work_unit)
            self.add_pending_chip_away_work_item(blk_hash, len(tx_offsets))

        return remaining_work, work_for_this_batch

    def get_main_batch(self, main_batch_tip: bitcoinx.Header) -> Tuple[Set[bytes], MainBatch]:
        # NOTE: This ConduitRawAPIClient is lazy loaded after the multiprocessing child processes
        #   are forked. This is ESSENTIAL - otherwise Segmentation Faults are the result!
        #   it is something to do with global socket state interfering with the child processes
        #   when they create their own ConduitRawAPIClient
        #   see: https://github.com/googleapis/synthtool/issues/902
        if not self.lmdb_grpc_client:
            self.lmdb_grpc_client = ConduitRawAPIClient()

        all_pending_block_hashes = set()
        headers: Headers = self.storage.headers
        chain = self.storage.headers.longest_chain()
        local_block_tip_height = self.get_local_block_tip_height()
        block_height_deficit = main_batch_tip.height - local_block_tip_height

        # May need to use block_hash not height to be more correct
        block_headers: List[bitcoinx.Header] = []
        for i in range(1, block_height_deficit + 1):
            block_header = headers.header_at_height(chain, local_block_tip_height + i)
            block_headers.append(block_header)

        header_hashes = [block_header.hash for block_header in block_headers]
        block_size_results = self.lmdb_grpc_client.get_block_metadata_batched(header_hashes)
        tx_offsets_results = self.lmdb_grpc_client.get_tx_offsets_batched(header_hashes)

        all_work_units = list(zip(block_size_results, tx_offsets_results, block_headers))
        return all_pending_block_hashes, all_work_units

    def incr_msg_received_count(self):
        with self._msg_received_count_lock:
            self._msg_received_count += 1

    def incr_msg_handled_count(self):
        with self._msg_handled_count_lock:
            self._msg_handled_count += 1

    def readout_sync_state(self):
        self.logger.error(f"A blockage in the pipeline is suspected and needs diagnosing.")
        self.logger.error(f"Controller State:")
        self.logger.error(f"-----------------")
        self.logger.error(f"msg_received_count={self._msg_handled_count_lock}")
        self.logger.error(f"msg_handled_count={self._msg_received_count}")

        self.logger.debug(f"len(self.received_blocks)={len(self.received_blocks)}")
        self.logger.debug(f"len(self.done_blocks_tx_parser)={len(self.done_blocks_tx_parser)}")

        for blk_hash in self.received_blocks:
            self.logger.error(f"received blk_hash={hash_to_hex_str(blk_hash)}")

        for blk_hash in self.done_blocks_tx_parser:
            self.logger.error(f"done_blocks blk_hash={hash_to_hex_str(blk_hash)}")

    # ----- SUPERVISE BATCH COMPLETION ----- #
    # Maximum number of blocks in this batch is 'MAIN_BATCH_HEADERS_COUNT_LIMIT'

    def block_is_fully_processed(self, block_hash: bytes) -> bool:
        return self._pending_blocks_progress_counter[block_hash] == \
            self.expected_blocks_tx_counts[block_hash]

    def add_pending_block(self, blk_hash, total_tx_count):
        self.expected_blocks_tx_counts[blk_hash] = total_tx_count
        self._pending_blocks_progress_counter[blk_hash] = 0

    def reset_pending_blocks(self):
        self._pending_blocks_received = {}
        self._pending_blocks_progress_counter = {}

    # ----- SUPERVISE 'CHIP AWAY' BATCH COMPLETIONS ----- #
    # This batch can be as small as system resources need it to be (even small parts of a block)

    def have_completed_chip_away_batch(self, block_hash: bytes) -> bool:
        return self._pending_blocks_progress_counter_chip_away[block_hash] == \
               self.expected_blocks_tx_counts_chip_away[block_hash]


    def add_pending_chip_away_work_item(self, blk_hash, work_item_tx_count):
        def init_counters_if_needed():
            if not self.expected_blocks_tx_counts_chip_away.get(blk_hash):
                self.expected_blocks_tx_counts_chip_away[blk_hash] = 0
            if not self._pending_blocks_progress_counter_chip_away.get(blk_hash):
                self._pending_blocks_progress_counter_chip_away[blk_hash] = 0

        init_counters_if_needed()
        self.expected_blocks_tx_counts_chip_away[blk_hash] += work_item_tx_count
        self.all_pending_chip_away_block_hashes.add(blk_hash)


    def reset_pending_chip_away_work_items(self):
        self._pending_blocks_received_chip_away = {}
        self._pending_blocks_progress_counter_chip_away = {}

    # ----- AVOID MEMPOOL-INDUCED THRASHING DURING IBD ----- #

    def is_post_IBD(self):
        """has initial block download been completed?
        (to syncronize all blocks with all initially fetched headers)"""
        return self.initial_block_download_event.is_set()

    def set_post_IBD_mode(self):
        self.initial_block_download_event.set()  # once set the first time will stay set
        self.initial_block_download_event_mp.set()
