import asyncio
import bitcoinx
from bitcoinx.networks import Header
import logging
import typing
from typing import cast

from conduit_lib.constants import TARGET_BYTES_BLOCK_BATCH_REQUEST_SIZE_CONDUIT_RAW
from conduit_lib.deserializer_types import Inv
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

    def __init__(self, storage: Storage, controller: 'Controller') -> None:
        self.logger = logging.getLogger("sync-state")
        self.storage = storage
        self.controller = controller

        self.headers_msg_processed_queue: asyncio.Queue[tuple[bool, Header, Header] | None] \
            = asyncio.Queue()
        self.headers_new_tip_queue: asyncio.Queue[Inv] = asyncio.Queue()
        self.headers_event_initial_sync: asyncio.Event = asyncio.Event()
        self.blocks_event_new_tip: asyncio.Event = asyncio.Event()
        self.target_header_height: int | None = None
        self.target_block_header_height: int | None = None
        self.local_tip_height: int = self.update_local_tip_height()
        self.local_block_tip_height: int = self.get_local_block_tip_height()

        # Accounting and ack'ing for non-block msgs
        self.incoming_msg_queue: asyncio.Queue[tuple[bytes, bytes]] = \
            asyncio.Queue()

        # Accounting and ack'ing for block msgs
        self.all_pending_block_hashes = set[bytes]()  # usually a set of 500 hashes during IBD

        # Done blocks Sets
        self.done_blocks_raw_event: asyncio.Event = asyncio.Event()
        self.done_blocks_mtree_event: asyncio.Event = asyncio.Event()

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

    def get_next_batched_blocks(self, from_height: int, to_height: int) \
            -> tuple[int, set[bytes], int]:
        """Key Variables
        - stop_header_height
        - all_pending_block_hashes
        """
        self.all_pending_block_hashes = set()
        block_height_deficit = to_height - from_height

        local_tip_height = self.get_local_block_tip_height()
        estimated_ideal_block_count = self.controller.get_ideal_block_batch_count(
            TARGET_BYTES_BLOCK_BATCH_REQUEST_SIZE_CONDUIT_RAW, local_tip_height)

        batch_count = min(block_height_deficit, estimated_ideal_block_count)
        stop_header_height = from_height + batch_count + 1

        for i in range(1, batch_count + 1):
            block_header = self.controller.get_header_for_height(from_height + i)
            self.all_pending_block_hashes.add(block_header.hash)

        return batch_count, self.all_pending_block_hashes, stop_header_height