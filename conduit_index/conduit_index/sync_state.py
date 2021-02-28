import asyncio
import logging
import multiprocessing
import threading
import typing
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Optional, Tuple

import bitcoinx
from bitcoinx import Headers, hash_to_hex_str

from conduit_lib.headers_state_client import HeadersStateClient
from conduit_lib.store import Storage

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

    def __init__(self, storage: Storage, controller: 'Controller'):
        self.logger = logging.getLogger("sync-state")
        self.storage = storage
        self.controller = controller
        self.lmdb = self.storage.lmdb
        self.headers_state_client = HeadersStateClient()
        self.headers_state_client.connect_or_keep_trying()
        self.headers_queue = self.headers_state_client.get_remote_headers_queue()
        self.headers_queue_async = asyncio.Queue()

        self.headers_msg_processed_event = asyncio.Event()
        self.headers_event_new_tip = asyncio.Event()
        self.headers_event_initial_sync = asyncio.Event()
        self.blocks_event_new_tip = asyncio.Event()
        self.blocks_event_new_tip.set()
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
        self.pending_blocks_inv_queue = asyncio.Queue()
        self._pending_blocks_progress_counter = {}

        # Accounting and ack'ing for block msgs
        self.global_blocks_batch_set = set()  # usually a set of 500 hashes during IBD
        self.received_blocks = set()  # received in network buffer - must process before buf reset
        self.done_blocks_tx_parser = set()
        self.done_blocks_tx_parser_lock = threading.Lock()
        self.done_blocks_tx_parser_event = asyncio.Event()

        self._batched_blocks_exec = ThreadPoolExecutor(1, "join-batched-blocks")
        self.initial_block_download_event_mp = multiprocessing.Event()

    @property
    def message_received_count(self):
        return self._msg_received_count

    @property
    def message_handled_count(self):
        return self._msg_handled_count

    # Headers
    def get_local_tip_height(self):
        return self.local_tip_height

    def update_local_tip_height(self) -> int:
        self.local_tip_height = self.storage.headers.longest_chain().tip.height
        return self.local_tip_height

    # Block Headers
    def get_local_block_tip_height(self) -> int:
        return self.storage.block_headers.longest_chain().tip.height

    def get_local_block_tip(self) -> int:
        return self.storage.block_headers.longest_chain().tip

    # ConduitRaw Headers State
    def get_conduit_raw_header_tip(self):
        """Needs to first have a connected headers_state_client"""
        return self.headers_state_client.get_remote_tip()

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

    def get_next_batched_blocks(self):
        """Key Variables:
        - stop_header_height
        - blocks_batch_set
        """
        # Todo - must check how large these blocks are to allocate a sensible number
        #  of blocks
        MAX_BYTES = 1024**3
        PARALLEL_PROCESSING_LIMIT = 10 * 1024**2  # If less than this size send to one worker only
        total_batch_bytes = 0
        allocated_work = []

        def batch_is_over_limit() -> bool:
            return total_batch_bytes > MAX_BYTES

        blocks_batch_set = set()
        headers: Headers = self.storage.headers
        chain = self.storage.headers.longest_chain()
        conduit_raw_tip = self.get_conduit_raw_header_tip()
        local_block_tip_height = self.get_local_block_tip_height()
        block_height_deficit = conduit_raw_tip.height - local_block_tip_height

        batch_count = min(block_height_deficit, 500)
        stop_header_height = local_block_tip_height + batch_count

        # May need to use block_hash not height to be more correct
        for i in range(1, batch_count + 1):
            block_header = headers.header_at_height(chain, local_block_tip_height + i)
            block_bytes = self.lmdb.get_block_metadata(block_header.hash)
            # self.logger.debug(f"block_bytes={block_bytes}")
            total_batch_bytes += block_bytes
            tx_offsets = self.lmdb.get_tx_offsets(block_header.hash)
            first_tx_pos_batch = 0  # Todo - update this when work is partitioned
            if block_bytes > PARALLEL_PROCESSING_LIMIT:
                # Todo - split up workload
                allocated_work.append((block_header.hash, block_header.height,
                    first_tx_pos_batch, tx_offsets))
            else:
                allocated_work.append((block_header.hash, block_header.height,
                    first_tx_pos_batch, tx_offsets))
            if batch_is_over_limit():
                # Todo - adjust batch_size and stop_header_height
                self.logger.warning(f"Batch is over the MAX_BYTES limit ({MAX_BYTES/1024**2}MB)!")
                self.logger.warning(f"Exceeding MAX_BYTES is currently not handled!")
                blocks_batch_set.add(block_header.hash)
                self.add_pending_block(block_header.hash, len(tx_offsets))
            else:
                blocks_batch_set.add(block_header.hash)
                self.add_pending_block(block_header.hash, len(tx_offsets))

        return batch_count, blocks_batch_set, stop_header_height, allocated_work

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

    def block_is_fully_processed(self, block_hash: bytes) -> bool:
        return self._pending_blocks_progress_counter[block_hash] == \
            self.expected_blocks_tx_counts[block_hash]

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
        # TODO - change this over to being triggered by ConduitRaw headers...
        self.logger.debug(f"WAITING....")
        if self.is_post_IBD():
            await self.blocks_event_new_tip.wait()
            self.blocks_event_new_tip.clear()

        if not self.is_post_IBD():  # one-time thing to trap the loop here on first IBD completion
            self.set_post_IBD_mode()
            self.blocks_event_new_tip.clear()
            await self.wait_for_new_block_tip()

        self.logger.debug(f"STOPPED WAITING....")