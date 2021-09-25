import asyncio
import logging
import threading
import typing

from bitcoinx import hash_to_hex_str

if typing.TYPE_CHECKING:
    from conduit_lib.store import Storage
    from .sync_state import SyncState


"""
These threads wait on full completion of the batch of blocks submitted to their corresponding worker
processes. In a crash, the batch will be re-done on start-up because the block_headers.mmap
file is not updated until all work in the batch is ack'd and flushed.

The threads must be long-running because instantiation of the logging module is quite
expensive (given the creation of a socket connection).
"""


class BatchCompletionTxParser(threading.Thread):
    """Only Processes ACK messages from the BlockWriter worker"""

    def __init__(self, storage: 'Storage', sync_state: 'SyncState',
            worker_ack_queue_tx_parse_confirmed, tx_parser_completion_queue, daemon=True):
        threading.Thread.__init__(self, daemon=daemon)
        self.logger = logging.getLogger("batch-completion-tx-parser")
        self.storage: Storage = storage
        self.sync_state = sync_state
        self.get_header_for_hash = self.storage.get_header_for_hash
        self.worker_ack_queue_tx_parse_confirmed = worker_ack_queue_tx_parse_confirmed
        self.tx_parser_completion_queue = tx_parser_completion_queue
        self.loop = asyncio.get_running_loop()

    def wait_for_batch_completion(self, blocks_batch_set):
        while True:
            worker_id, block_hash, txs_done_count = self.worker_ack_queue_tx_parse_confirmed.get()
            try:
                self.sync_state._pending_blocks_progress_counter_chip_away[block_hash] += txs_done_count
                self.sync_state._pending_blocks_progress_counter[block_hash] += txs_done_count
            except KeyError:
                raise

            try:
                if self.sync_state.have_completed_chip_away_batch(block_hash):
                    self.sync_state.all_pending_chip_away_block_hashes.remove(block_hash)
                    if len(self.sync_state.all_pending_chip_away_block_hashes) == 0:
                        self.loop.call_soon_threadsafe(self.sync_state.chip_away_batch_event.set)

                if self.sync_state.block_is_fully_processed(block_hash):
                    header = self.get_header_for_hash(block_hash)
                    if not block_hash in blocks_batch_set:
                        self.logger.exception(f"also wrote unexpected block: "
                            f"{hash_to_hex_str(header.hash)} {header.height} to disc")
                        continue

                    # self.logger.debug(f"block height={header.height} done!")
                    blocks_batch_set.remove(block_hash)
                    with self.sync_state.done_blocks_tx_parser_lock:
                        self.sync_state.done_blocks_tx_parser.add(block_hash)
            except KeyError:
                header = self.get_header_for_hash(block_hash)
                self.logger.debug(f"also parsed block: {header.height}")

            # all blocks in batch processed
            if len(blocks_batch_set) == 0:
                self.loop.call_soon_threadsafe(self.sync_state.done_blocks_tx_parser_event.set)
                break

    def run(self):
        batch_id = 0
        while True:
            try:
                all_pending_block_hashes = self.tx_parser_completion_queue.get()
                self.wait_for_batch_completion(all_pending_block_hashes)
                self.logger.debug(f"ACKs for batch {batch_id} received")
                batch_id += 1
            except Exception as e:
                self.logger.exception(e)
