import asyncio
import logging
import multiprocessing
import threading
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Optional

import bitcoinx
from bitcoinx import Headers, hash_to_hex_str

from .store import Storage


class SyncState:

    def __init__(self, storage: Storage):
        self.done_block_heights = []
        self.logger = logging.getLogger("sync-state")
        self.storage = storage

        self.headers_msg_processed_event = asyncio.Event()
        self.headers_event_new_tip = asyncio.Event()
        self.headers_event_initial_sync = asyncio.Event()
        self.blocks_event_new_tip = asyncio.Event()
        self.target_header_height: Optional[int] = None
        self.target_block_header_height: Optional[int] = None
        self.local_tip_height: int = self.update_local_tip_height()
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
        self.outstanding_block_hashes_set = set()
        self.allocated_blocks_to_height = self.local_block_tip_height
        self.pending_blocks_inv_queue = asyncio.Queue()
        self._pending_blocks_progress_counter = {}

        self._batched_blocks_exec = ThreadPoolExecutor(1, "join-batched-blocks")
        self.initial_block_download_event_mp = multiprocessing.Event()

    @property
    def message_received_count(self):
        return self._msg_received_count

    @property
    def message_handled_count(self):
        return self._msg_handled_count

    def get_local_tip_height(self):
        return self.local_tip_height

    def get_local_block_tip_height(self) -> int:
        return self.storage.block_headers.longest_chain().tip.height

    def update_local_tip_height(self) -> int:
        self.local_tip_height = self.storage.headers.longest_chain().tip.height
        return self.local_tip_height

    def set_target_header_height(self, height) -> None:
        self.target_header_height = height

    def have_processed_non_block_msgs(self) -> bool:
        return self._msg_received_count == self._msg_handled_count

    def have_processed_block_msgs(self) -> bool:
        # Todo - cover MTree and BlockWriter workers too
        for blk_hash, count in self._pending_blocks_progress_counter.items():
            if not self.expected_blocks_tx_counts[blk_hash] == count:
                return False  # not all txs in block ack'd

        return True

    def have_processed_all_msgs_in_buffer(self):
        return self.have_processed_non_block_msgs() and self.have_processed_block_msgs()

    def is_synchronized(self):
        return self.get_local_block_tip_height() >= self.get_local_tip_height()

    def reset_done_block_heights(self):
        self.done_block_heights = []

    def reset_msg_counts(self):
        with self._msg_handled_count_lock:
            self._msg_handled_count = 0
        with self._msg_received_count_lock:
            self._msg_received_count = 0

    def get_next_batched_blocks(self):
        """Three key variables:
        - pending_blocks_batch_size
        - stop_header_height
        - blocks_batch_set

        Should really just have
        - outstanding_block_count (global and incremented each loop and reset on each buffer reset)
        - allocated_block_height

        - stop_header_height (local) - post-IDB can only be at allocated_block_height += 1
        - blocks_batch_set (local) - pre-IBD can be up to 500 | post-IDB can only contain 1 block

        """

        blocks_batch_set = set()
        headers: Headers = self.storage.headers
        chain = self.storage.headers.longest_chain()

        local_headers_tip_height = self.get_local_tip_height()
        local_block_tip_height = self.get_local_block_tip_height()
        block_height_deficit = local_headers_tip_height - local_block_tip_height

        batch_size = min(block_height_deficit, 500)
        stop_header_height = local_block_tip_height + batch_size

        for i in range(1, batch_size + 1):
            block_header = headers.header_at_height(chain, local_block_tip_height + i)
            blocks_batch_set.add(block_header.hash)

        return batch_size, blocks_batch_set, stop_header_height

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
        self.logger.error(f"msg_received_count={self.message_received_count}")
        self.logger.error(f"msg_handled_count={self.message_handled_count}")

        for blk_hash, count in self._pending_blocks_progress_counter.items():
            if not self.expected_blocks_tx_counts[blk_hash] == count:
                self.logger.error(f"self.expected_blocks_tx_counts[blk_hash] != count for "
                    f"{hash_to_hex_str(blk_hash)}")

        self.logger.error(f"self._pending_blocks_progress_counter.items():")
        for blk_hash, count in self._pending_blocks_progress_counter.items():
            self.logger.error(
                f"blk_hash={bitcoinx.hash_to_hex_str(blk_hash)}, count={count}, "
                f"expected_blocks_tx_counts[blk_hash]={self.expected_blocks_tx_counts[blk_hash]}"
            )
        self.logger.error(
            f"len(self.done_block_heights)={len(self.done_block_heights)}"
        )

    def add_pending_block(self, blk_hash, total_tx_count):
        self.expected_blocks_tx_counts[blk_hash] = total_tx_count
        self._pending_blocks_progress_counter[blk_hash] = 0

    def reset_pending_blocks(self):
        self._pending_blocks_received = {}
        self._pending_blocks_progress_counter = {}

    async def wait_for_new_headers_tip(self):
        self.headers_event_initial_sync.set()
        await self.headers_event_new_tip.wait()
        self.headers_event_new_tip.clear()

    def is_post_IBD(self):
        """has initial block download been completed?
        (to syncronize all blocks with all initially fetched headers)"""
        return self.initial_block_download_event.is_set()

    def set_post_IBD_mode(self):
        self.initial_block_download_event.set()  # once set the first time will stay set
        self.initial_block_download_event_mp.set()

    async def wait_for_new_block_tip(self):
        if self.is_post_IBD():
            await self.blocks_event_new_tip.wait()
            self.blocks_event_new_tip.clear()

        if not self.is_post_IBD():
            self.set_post_IBD_mode()
            self.blocks_event_new_tip.clear()
            await self.blocks_event_new_tip.wait()
            self.blocks_event_new_tip.clear()
