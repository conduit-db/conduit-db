import asyncio
import logging
import threading
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Optional

import bitcoinx
from bitcoinx import Headers

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
        self.pending_blocks_batch_set = set()  # usually a set of 500 hashes
        self.pending_blocks_received = {}  # blk_hash: total_tx_count
        self._pending_blocks_batch_size = 0
        self._pending_blocks_inv_queue = asyncio.Queue()
        self._pending_blocks_progress_counter = {}
        self._batched_blocks_exec = ThreadPoolExecutor(1, "join-batched-blocks")

    @property
    def message_received_count(self):
        return self._msg_received_count

    @property
    def message_handled_count(self):
        return self._msg_handled_count

    @property
    def pending_blocks_batch_size(self):
        return self._pending_blocks_batch_size

    @pending_blocks_batch_size.setter
    def pending_blocks_batch_size(self, value):
        self._pending_blocks_batch_size = value

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
        expected_blocks_processed_count = self.pending_blocks_batch_size - len(
            self.pending_blocks_batch_set
        )
        for blk_hash, count in self._pending_blocks_progress_counter.items():
            if not self.pending_blocks_received[blk_hash] == count:
                return False  # not all txs in block ack'd

        if expected_blocks_processed_count != len(self.done_block_heights):
            return False

        return True

    def have_processed_all_msgs_in_buffer(self):
        return self.have_processed_non_block_msgs() and self.have_processed_block_msgs()

    def is_synchronized(self):
        return self.get_local_block_tip_height() >= self.get_local_tip_height()

    def reset_msg_counts(self):
        with self._msg_handled_count_lock:
            self._msg_handled_count = 0
        with self._msg_received_count_lock:
            self._msg_received_count = 0

    def reset_pending_blocks_batch_set(self):
        self.pending_blocks_batch_set = set()
        headers: Headers = self.storage.headers
        chain = self.storage.headers.longest_chain()

        local_headers_tip_height = self.get_local_tip_height()
        local_block_tip_height = self.get_local_block_tip_height()
        block_height_deficit = min((local_headers_tip_height - local_block_tip_height), 500)
        self.stop_header_height = local_block_tip_height + block_height_deficit
        self.pending_blocks_batch_size = self.stop_header_height - local_block_tip_height

        for i in range(1, block_height_deficit + 1):
            block_header = headers.header_at_height(chain, local_block_tip_height + i)
            self.pending_blocks_batch_set.add(block_header.hash)
        self.logger.debug(self.pending_blocks_batch_set)

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

        self.logger.error(f"pending_blocks_batch_size={self.pending_blocks_batch_size}")
        self.logger.error(f"pending_blocks_batch_set={self.pending_blocks_batch_set}")
        self.logger.error(f"pending_blocks_received={self.pending_blocks_received}")

        self.logger.error(f"self._pending_blocks_progress_counter.items():")
        for blk_hash, count in self._pending_blocks_progress_counter.items():
            self.logger.error(
                f"blk_hash={bitcoinx.hash_to_hex_str(blk_hash)}, count={count}, "
                f"pending_blocks_received[blk_hash]={self.pending_blocks_received[blk_hash]}"
            )
        expected_blocks_processed_count = self.pending_blocks_batch_size - len(
            self.pending_blocks_batch_set
        )
        self.logger.error(
            f"expected_blocks_processed_count={expected_blocks_processed_count},"
            f"len(self.done_block_heights)={len(self.done_block_heights)}"
        )

    def add_pending_block(self, blk_hash, total_tx_count):
        self.pending_blocks_received[blk_hash] = total_tx_count
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

    async def wait_for_new_block_tip(self):
        if self.is_post_IBD():
            await self.blocks_event_new_tip.wait()
            self.blocks_event_new_tip.clear()

        if not self.is_post_IBD():
            self.set_post_IBD_mode()
            self.blocks_event_new_tip.clear()
            await self.blocks_event_new_tip.wait()
            self.blocks_event_new_tip.clear()
