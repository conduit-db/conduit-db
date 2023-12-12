# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.

import asyncio
from datetime import datetime, timedelta

import bitcoinx
from bitcoinx import unpack_header, double_sha256
from bitcoinx.networks import Header
from concurrent.futures.thread import ThreadPoolExecutor
import logging
import threading
import time
import typing
from typing import cast

from conduit_lib.ipc_sock_client import IPCSocketClient
from conduit_lib.constants import SMALL_BLOCK_SIZE, CHIP_AWAY_BYTE_SIZE_LIMIT, REGTEST
from conduit_lib.store import Storage
from .load_balance_algo import distribute_load, WorkPart
from .types import WorkUnit, MainBatch

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

    def __init__(self, storage: Storage, controller: "Controller") -> None:
        self.logger = logging.getLogger("sync-state")
        self.storage = storage
        self.controller = controller
        self.ipc_sock_client: IPCSocketClient | None = None
        self.headers_threadsafe = self.controller.headers_threadsafe
        self.headers_threadsafe_blocks = self.controller.headers_threadsafe_blocks

        self.conduit_raw_header_tip: bitcoinx.Header | None = None
        self.conduit_raw_header_tip_lock: threading.Lock = threading.Lock()

        # Not actually used by Indexer but cannot remove because it's in the shared lib
        self.target_header_height: int | None = None
        self.target_block_header_height: int | None = None
        self.local_block_tip_height: int = self.get_local_block_tip_height()

        # Accounting and ack'ing for non-block msgs
        self.incoming_msg_queue: asyncio.Queue[tuple[bytes, bytes]] = asyncio.Queue()

        # Accounting and ack'ing for block msgs
        self.blocks_batch_set = set[bytes]()  # usually a set of 500 hashes
        self.expected_blocks_tx_counts: dict[bytes, int] = {}  # blk_hash: total_tx_count
        self._blocks_progress_counter: dict[bytes, int] = {}  # block_hash: txs_done_count

        # Accounting and ack'ing for chip away block msgs
        self.all_pending_chip_away_work_item_ids = set[int]()  # set of work_item_ids
        self.expected_work_item_tx_counts: dict[int, int] = {}  # work_item_id: total_tx_count
        self._work_item_progress_counter: dict[int, int] = {}  # work_item_id: txs_done_count
        self.chip_away_batch_event = asyncio.Event()

        # Accounting and ack'ing for block msgs
        self.all_pending_block_hashes = set[bytes]()  # usually a set of 500 hashes during IBD

        self.done_blocks_tx_parser = set[bytes]()
        self.done_blocks_tx_parser_lock = threading.Lock()
        self.done_blocks_tx_parser_event = asyncio.Event()

        self._batched_blocks_exec = ThreadPoolExecutor(1, "join-batched-blocks")

        self.total_time_allocating_work = 0.0
        self.is_post_ibd = False
        self.conduit_best_tip: bitcoinx.Header | None = None

    # Conduit Best Tip
    async def get_conduit_best_tip(self) -> bitcoinx.Header:
        ipc_sock_client = None
        try:
            ipc_sock_client = await self.controller.loop.run_in_executor(
                self.controller.general_executor, IPCSocketClient
            )
            tip, tip_height = await self.controller.loop.run_in_executor(
                self.controller.general_executor, ipc_sock_client.chain_tip
            )
            conduit_best_tip = bitcoinx.Header(*unpack_header(tip), double_sha256(tip), tip, tip_height)
            return conduit_best_tip
        finally:
            if ipc_sock_client:
                ipc_sock_client.close()

    def get_local_tip(self) -> Header:
        return self.headers_threadsafe.tip()

    def get_local_tip_height(self) -> int:
        return cast(int, self.get_local_tip().height)

    def get_local_block_tip(self) -> bitcoinx.Header:
        return self.headers_threadsafe_blocks.tip()

    def get_local_block_tip_height(self) -> int:
        return cast(int, self.get_local_block_tip().height)

    async def is_post_ibd_state(self) -> bool:
        if self.is_post_ibd:  # cache result
            return self.is_post_ibd

        conduit_best_tip = await self.get_conduit_best_tip()
        local_tip = self.get_local_block_tip()

        # Usually the node sets IBD mode on once it is within 24 hours of the chain tip
        # which has the benefit of pre-filling the mempool to prepare for switching to the
        # compact block protocol by the time it reaches the chain tip.
        if self.controller.net_config.NET != REGTEST:
            conduit_best = datetime.utcfromtimestamp(conduit_best_tip.timestamp)
            our_tip = datetime.utcfromtimestamp(local_tip.timestamp)
            one_day_ago = datetime.utcnow() - timedelta(hours=24)
            if conduit_best > one_day_ago and our_tip > one_day_ago:
                return True
        else:
            # Regtest timestamps are not reliable when submitting old testing blocks
            # Just exit IBD mode as soon as ConduitIndex catches up to ConduitRaw
            if local_tip.hash == conduit_best_tip.hash:
                return True
        return False

    def set_target_header_height(self, height: int) -> None:
        self.target_header_height = height

    def get_work_units_all(
        self,
        is_reorg: bool,
        all_pending_block_hashes: set[bytes],
        all_work: MainBatch,
    ) -> list[WorkUnit]:
        """is_reorg means that this block hash was one of the ones affected by the reorg."""

        t0 = time.perf_counter()
        try:
            all_work_units = []
            work_item_id = 0
            for i, work in enumerate(all_work):
                block_size, tx_offsets, block_header, block_num = work
                needs_breaking_up = block_size > SMALL_BLOCK_SIZE or block_size > CHIP_AWAY_BYTE_SIZE_LIMIT

                first_tx_pos_batch = 0
                if needs_breaking_up:
                    tx_count = len(tx_offsets)
                    divided_work = distribute_load(
                        block_header.hash,
                        block_num,
                        tx_count,
                        block_size,
                        tx_offsets,
                    )

                    work_part: WorkPart
                    for work_part in divided_work:
                        (
                            size_of_part,
                            blk_hash,
                            block_num,
                            first_tx_pos_batch,
                            part_end_offset,
                            work_part_tx_offsets,
                        ) = work_part

                        # Adding work_item_id to WorkPart -> WorkUnit
                        work_item: WorkUnit = (
                            is_reorg,
                            size_of_part,
                            work_item_id,
                            blk_hash,
                            block_num,
                            first_tx_pos_batch,
                            part_end_offset,
                            work_part_tx_offsets,
                        )
                        all_work_units.append(work_item)
                        work_item_id += 1
                        # self.logger.debug(f"divided_work={divided_work}")
                else:
                    part_end_offset = block_size
                    size_of_part = block_size
                    all_work_units.append(
                        (
                            is_reorg,
                            size_of_part,
                            work_item_id,
                            block_header.hash,
                            block_num,
                            first_tx_pos_batch,
                            part_end_offset,
                            tx_offsets,
                        )
                    )
                    work_item_id += 1

                all_pending_block_hashes.add(block_header.hash)
                self.add_pending_block(block_header.hash, len(tx_offsets))

            return all_work_units
        finally:
            t1 = time.perf_counter() - t0
            self.total_time_allocating_work += t1
            self.logger.debug(f"total time allocating work: {self.total_time_allocating_work}")

    def get_work_units_chip_away(
        self, remaining_work_units: list[WorkUnit]
    ) -> tuple[list[WorkUnit], list[WorkUnit]]:
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
            (
                is_reorg,
                size_of_part,
                work_item_id,
                blk_hash,
                blk_num,
                first_tx_pos_batch,
                part_end_offset,
                tx_offsets,
            ) = work_unit

            # self.logger.debug(f"work_unit={work_unit}")
            reached_limit = total_allocation + size_of_part > CHIP_AWAY_BYTE_SIZE_LIMIT

            if reached_limit:
                remaining_work.append(work_unit)
                continue

            total_allocation += size_of_part
            work_for_this_batch.append(work_unit)
            self.add_pending_chip_away_work_item(work_item_id, len(tx_offsets))

        return remaining_work, work_for_this_batch

    async def get_main_batch(self, start_header: bitcoinx.Header, stop_header: bitcoinx.Header) -> MainBatch:
        """Must run in thread due to ipc_sock_client not being async"""
        ipc_sock_client_pool = None
        all_work_units: MainBatch = []
        try:
            # There should be a global connection pool but this will do for now
            ipc_sock_client_pool = []
            for i in range(3):
                ipc_sock_client = await asyncio.get_running_loop().run_in_executor(
                    self.controller.general_executor, IPCSocketClient
                )
                ipc_sock_client_pool.append(ipc_sock_client)

            with self.storage.headers_lock:
                block_height_deficit = stop_header.height - (start_header.height - 1)

                # May need to use block_hash not height to be more correct
                block_headers: list[bitcoinx.Header] = []
                for i in range(1, block_height_deficit + 1):
                    block_header = self.headers_threadsafe.get_header_for_height(
                        start_header.height - 1 + i, lock=False
                    )
                    block_headers.append(block_header)

                header_hashes = [block_header.hash for block_header in block_headers]

            task = asyncio.get_running_loop().run_in_executor(
                self.controller.general_executor,
                ipc_sock_client_pool[0].block_metadata_batched,
                header_hashes,
            )
            task2 = asyncio.get_running_loop().run_in_executor(
                self.controller.general_executor,
                ipc_sock_client_pool[1].transaction_offsets_batched,
                header_hashes,
            )
            task3 = asyncio.get_running_loop().run_in_executor(
                self.controller.general_executor, ipc_sock_client_pool[2].block_number_batched, header_hashes
            )

            self.logger.debug(f"get_main_batch called...")
            block_metadata_batch = (await task).block_metadata_batch
            tx_offsets_results = await task2
            block_numbers = (await task3).block_numbers
            block_sizes_batch = [block_metadata.block_size for block_metadata in block_metadata_batch]

            all_work_units = list(
                zip(
                    block_sizes_batch,
                    tx_offsets_results,
                    block_headers,
                    block_numbers,
                )
            )
        except Exception as e:
            self.logger.exception(f"Unexpected exception in get_main_batch thread")
            await self.controller.stop()
        finally:
            for ipc_sock_client in ipc_sock_client_pool:
                ipc_sock_client.close()
        return all_work_units

    # ----- SUPERVISE BATCH COMPLETION ----- #
    # Maximum number of blocks in this batch is 'MAIN_BATCH_HEADERS_COUNT_LIMIT'

    def block_is_fully_processed(self, block_hash: bytes) -> bool:
        return self._blocks_progress_counter[block_hash] == self.expected_blocks_tx_counts[block_hash]

    def add_pending_block(self, blk_hash: bytes, total_tx_count: int) -> None:
        self.expected_blocks_tx_counts[blk_hash] = total_tx_count
        self._blocks_progress_counter[blk_hash] = 0

    def reset_pending_blocks(self) -> None:
        self._blocks_progress_counter = {}
        self.expected_work_item_tx_counts = {}
        self.expected_blocks_tx_counts = {}

    # ----- SUPERVISE 'CHIP AWAY' BATCH COMPLETIONS ----- #
    # This batch can be as small as system resources need it to be (even small parts of a block)

    def have_completed_work_item(self, work_item_id: int) -> bool:
        return (
            self._work_item_progress_counter[work_item_id] == self.expected_work_item_tx_counts[work_item_id]
        )

    def add_pending_chip_away_work_item(self, work_item_id: int, work_item_tx_count: int) -> None:
        def init_counters_if_needed() -> None:
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

    def print_progress_info(self) -> None:
        self.logger.debug(
            f"Count of all_pending_chip_away_work_item_ids: "
            f"{len(self.all_pending_chip_away_work_item_ids)}"
        )
        if self.is_post_ibd:
            self.logger.debug(f"Mempool tx count: {self.controller.mempool_tx_count}")
