import asyncio
import logging
import threading
import typing
from typing import Optional, Set, Tuple, cast, Union

import bitcoinx

from conduit_lib.bitcoin_net_io import BlockCallback
from conduit_lib.constants import TARGET_BLOCK_BATCH_REQUEST_SIZE_CONDUIT_RAW
from conduit_lib.deserializer_types import Inv
from conduit_lib.store import Storage
from bitcoinx.networks import Header

if typing.TYPE_CHECKING:
    from .controller import Controller


class SyncState:
    """
    Specific to ConduitRaw's sync state - Ensures that each batch is completed safely,
    sanity checks are completed (including reorg handling) and everything is flushed to disc
    before the network io shared memory buffer can be reset/mutated and before then next block
    can be requested from the node.
    """

    def __init__(self, storage: Storage, controller: 'Controller') -> None:
        self.logger = logging.getLogger("sync-state")
        self.storage = storage
        self.controller = controller

        self.headers_msg_processed_queue: asyncio.Queue[Tuple[bool, Header, Header]] = asyncio.Queue()
        self.headers_new_tip_queue: asyncio.Queue[Inv] = asyncio.Queue()
        self.headers_event_initial_sync: asyncio.Event = asyncio.Event()
        self.blocks_event_new_tip: asyncio.Event = asyncio.Event()
        self.target_header_height: Optional[int] = None
        self.target_block_header_height: Optional[int] = None
        self.local_tip_height: int = self.update_local_tip_height()
        self.local_block_tip_height: int = self.get_local_block_tip_height()

        # Accounting and ack'ing for non-block msgs
        self.incoming_msg_queue: asyncio.Queue[Tuple[bytes, Union[BlockCallback, memoryview]]] = asyncio.Queue()
        self._msg_received_count = 0
        self._msg_handled_count = 0
        self._msg_received_count_lock = threading.Lock()
        self._msg_handled_count_lock = threading.Lock()

        # Accounting and ack'ing for block msgs
        self.all_pending_block_hashes: Set[bytes] = set()  # usually a set of 500 hashes during IBD
        self.received_blocks: Set[bytes] = set()  # received in network buffer - must process before buf reset

        # Done blocks Sets
        self.done_blocks_raw: Set[bytes] = set()
        self.done_blocks_mtree: Set[bytes] = set()
        self.done_blocks_preproc: Set[bytes] = set()

        # Done blocks Locks
        self.done_blocks_raw_lock = threading.Lock()
        self.done_blocks_mtree_lock = threading.Lock()
        self.done_blocks_preproc_lock = threading.Lock()

        # Done blocks Events - Note initialization of logging tcp connection is expensive so
        # a thread pool is unsuitable - as you pay the initialization overhead repeatedly
        self.done_blocks_raw_event: asyncio.Event = asyncio.Event()
        self.done_blocks_mtree_event: asyncio.Event = asyncio.Event()
        self.done_blocks_preproc_event: asyncio.Event = asyncio.Event()
        self.pending_blocks_inv_queue: asyncio.Queue[Inv] = asyncio.Queue()

    def get_local_tip(self) -> bitcoinx.Header:
        with self.storage.headers_lock:
            return self.storage.headers.longest_chain().tip

    def get_local_tip_height(self) -> int:
        with self.storage.headers_lock:
            return self.local_tip_height

    def get_local_block_tip_height(self) -> int:
        with self.storage.block_headers_lock:
            return cast(int, self.storage.block_headers.longest_chain().tip.height)

    def get_local_block_tip(self) -> bitcoinx.Header:
        with self.storage.block_headers_lock:
            return self.storage.block_headers.longest_chain().tip

    def update_local_tip_height(self) -> int:
        with self.storage.headers_lock:
            self.local_tip_height = self.storage.headers.longest_chain().tip.height
            return self.local_tip_height

    def set_target_header_height(self, height: int) -> None:
        self.target_header_height = height

    def is_synchronized(self) -> bool:
        return self.get_local_block_tip_height() >= self.get_local_tip_height()

    def reset_msg_counts(self) -> None:
        with self._msg_handled_count_lock:
            self._msg_handled_count = 0
        with self._msg_received_count_lock:
            self._msg_received_count = 0

    def get_next_batched_blocks(self, from_height: int, to_height: int) \
            -> Tuple[int, Set[bytes], int]:
        """Key Variables
        - stop_header_height
        - all_pending_block_hashes
        """
        self.all_pending_block_hashes = set()
        block_height_deficit = to_height - from_height

        estimated_ideal_block_count = self.controller.get_ideal_block_batch_count(
            target_mb=TARGET_BLOCK_BATCH_REQUEST_SIZE_CONDUIT_RAW)

        batch_count = min(block_height_deficit, estimated_ideal_block_count)
        stop_header_height = from_height + batch_count + 1

        for i in range(1, batch_count + 1):
            block_header = self.controller.get_header_for_height(from_height + i)
            self.all_pending_block_hashes.add(block_header.hash)

        return batch_count, self.all_pending_block_hashes, stop_header_height

    def incr_msg_received_count(self) -> None:
        with self._msg_received_count_lock:
            self._msg_received_count += 1

    def incr_msg_handled_count(self) -> None:
        with self._msg_handled_count_lock:
            self._msg_handled_count += 1

    def have_processed_non_block_msgs(self) -> bool:
        return self._msg_received_count == self._msg_handled_count

    def print_progress_info(self) -> None:
        self.logger.debug(f"Count of received_blocks: {len(self.received_blocks)}")
        self.logger.debug(f"Count of done_blocks_raw: {len(self.done_blocks_raw)}")
        self.logger.debug(f"Count of done_blocks_mtree: {len(self.done_blocks_mtree)}")
        self.logger.debug(f"Count of done_blocks_preproc: {len(self.done_blocks_preproc)}")

    def have_processed_block_msgs(self) -> bool:
        try:
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

    def have_processed_all_msgs_in_buffer(self) -> bool:
        return self.have_processed_non_block_msgs() and self.have_processed_block_msgs()
