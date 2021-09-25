import asyncio
import logging
import multiprocessing
import threading
import typing
from typing import Optional

import bitcoinx
from bitcoinx import Headers, hash_to_hex_str

from conduit_lib.store import Storage

if typing.TYPE_CHECKING:
    from .controller import Controller


class SyncState:
    """
    Specific to ConduitRaw's sync state - Ensures that each batch is completed safely,
    sanity checks are completed (including reorg handling) and everything is flushed to disc
    before the network io shared memory buffer can be reset/mutated and before then next block
    can be requested from the node.
    """

    def __init__(self, storage: Storage, controller: 'Controller'):
        self.logger = logging.getLogger("sync-state")
        self.storage = storage
        self.controller = controller

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
        self.all_pending_block_hashes = set()  # usually a set of 500 hashes during IBD
        self.received_blocks = set()  # received in network buffer - must process before buf reset

        # Done blocks Sets
        self.done_blocks_raw = set()
        self.done_blocks_mtree = set()
        self.done_blocks_preproc = set()

        # Done blocks Locks
        self.done_blocks_raw_lock = threading.Lock()
        self.done_blocks_mtree_lock = threading.Lock()
        self.done_blocks_preproc_lock = threading.Lock()

        # Done blocks Events - Note initialization of logging tcp connection is expensive so
        # a thread pool is unsuitable - as you pay the initialization overhead repeatedly
        self.done_blocks_raw_event = asyncio.Event()
        self.done_blocks_mtree_event = asyncio.Event()
        self.done_blocks_preproc_event = asyncio.Event()

        self.pending_blocks_inv_queue = asyncio.Queue()
        self.initial_block_download_event_mp = multiprocessing.Event()

    def get_local_tip_height(self):
        return self.local_tip_height

    def get_local_block_tip_height(self) -> int:
        return self.storage.block_headers.longest_chain().tip.height

    def get_local_block_tip(self) -> bitcoinx.Header:
        return self.storage.block_headers.longest_chain().tip

    def update_local_tip_height(self) -> int:
        self.local_tip_height = self.storage.headers.longest_chain().tip.height
        return self.local_tip_height

    def set_target_header_height(self, height) -> None:
        self.target_header_height = height

    def is_synchronized(self):
        return self.get_local_block_tip_height() >= self.get_local_tip_height()

    def reset_msg_counts(self):
        with self._msg_handled_count_lock:
            self._msg_handled_count = 0
        with self._msg_received_count_lock:
            self._msg_received_count = 0

    def get_next_batched_blocks(self):
        """Key Variables
        - stop_header_height
        - all_pending_block_hashes
        """
        self.all_pending_block_hashes = set()
        headers: Headers = self.storage.headers
        chain = self.storage.headers.longest_chain()

        local_headers_tip_height = self.get_local_tip_height()
        local_block_tip_height = self.get_local_block_tip_height()
        block_height_deficit = local_headers_tip_height - local_block_tip_height

        batch_count = min(block_height_deficit, 500)
        stop_header_height = local_block_tip_height + batch_count

        for i in range(1, batch_count + 1):
            block_header = headers.header_at_height(chain, local_block_tip_height + i)
            self.all_pending_block_hashes.add(block_header.hash)

        return batch_count, self.all_pending_block_hashes, stop_header_height

    def incr_msg_received_count(self):
        with self._msg_received_count_lock:
            self._msg_received_count += 1

    def incr_msg_handled_count(self):
        with self._msg_handled_count_lock:
            self._msg_handled_count += 1

    async def wait_for_new_headers_tip(self):
        self.headers_event_initial_sync.set()
        await self.headers_event_new_tip.wait()
        self.headers_event_new_tip.clear()

    def is_post_IBD(self):
        """has initial block download been completed?
        (to syncronize all blocks with all initially fetched headers)"""
        return self.initial_block_download_event.is_set()

    def set_post_IBD_mode(self):
        self.logger.debug(f"setting initial_block_download_event")
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

    def have_processed_non_block_msgs(self) -> bool:
        return self._msg_received_count == self._msg_handled_count

    def have_processed_block_msgs(self) -> bool:
        try:
            # self.logger.debug(f"len(self.done_blocks_raw)={len(self.done_blocks_raw)}")
            # self.logger.debug(f"len(self.done_blocks_mtree)={len(self.done_blocks_mtree)}")
            # self.logger.debug(f"len(self.done_blocks_preproc)={len(self.done_blocks_preproc)}")
            # self.logger.debug(f"len(self.received_blocks)={len(self.received_blocks)}")
            # self.logger.debug(f"self.controller.worker_ack_queue_preproc.qsize()={self.controller.worker_ack_queue_preproc.qsize()}")
            # self.logger.debug(f"self.controller.worker_ack_queue_blk_writer.qsize()={self.controller.worker_ack_queue_blk_writer.qsize()}")
            # self.logger.debug(f"self.controller.worker_ack_queue_mtree.qsize()={self.controller.worker_ack_queue_mtree.qsize()}")

            with self.done_blocks_raw_lock:
                for blk_hash in self.done_blocks_raw:
                    if not blk_hash in self.received_blocks:
                        return False
                if len(self.done_blocks_raw) != len(self.received_blocks):
                    return False

            with self.done_blocks_mtree_lock:
                for blk_hash in self.done_blocks_mtree:
                    if not blk_hash in self.received_blocks:
                        return False
                if len(self.done_blocks_mtree) != len(self.received_blocks):
                    return False

            with self.done_blocks_preproc_lock:
                for blk_hash in self.done_blocks_preproc:
                    if not blk_hash in self.received_blocks:
                        return False
                if len(self.done_blocks_preproc) != len(self.received_blocks):
                    return False

            return True
        except Exception:
            self.logger.exception("unexpected exception in have_processed_block_msgs")
            raise

    def have_processed_all_msgs_in_buffer(self):
        return self.have_processed_non_block_msgs() and self.have_processed_block_msgs()

    def readout_sync_state(self):
        self.logger.error(f"A blockage in the pipeline is suspected and needs diagnosing.")
        self.logger.error(f"Controller State:")
        self.logger.error(f"-----------------")
        self.logger.error(f"msg_received_count={self._msg_handled_count_lock}")
        self.logger.error(f"msg_handled_count={self._msg_received_count}")

        self.logger.debug(f"len(self.received_blocks)={len(self.received_blocks)}")
        self.logger.debug(f"len(self.done_blocks_raw)={len(self.done_blocks_raw)}")
        self.logger.debug(f"len(self.done_blocks_mtree)={len(self.done_blocks_mtree)}")
        self.logger.debug(f"len(self.done_blocks_preproc)={len(self.done_blocks_preproc)}")

        for blk_hash in self.received_blocks:
            self.logger.error(f"blk_hash={hash_to_hex_str(blk_hash)}")