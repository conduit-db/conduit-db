import asyncio
import logging
import multiprocessing
import queue
import threading
import typing

from bitcoinx import hash_to_hex_str

if typing.TYPE_CHECKING:
    from .controller import Controller
    from .sync_state import SyncState


"""
These threads wait on full completion of the batch of blocks submitted to their corresponding worker
processes. In a crash, the batch will be re-done on start-up because the block_headers.mmap
file is not updated until all work in the batch is ack'd and flushed.

The threads must be long-running because instantiation of the logging module is quite
expensive (given the creation of a socket connection).
"""


class BatchCompletionRaw(threading.Thread):
    """Only Processes ACK messages from the BlockWriter worker"""

    def __init__(self, controller: 'Controller', sync_state: 'SyncState',
            worker_ack_queue_blk_writer, blocks_batch_set_queue_raw, daemon=True):
        threading.Thread.__init__(self, daemon=daemon)
        self.logger = logging.getLogger("batch-completion-raw")
        self.controller: Controller = controller
        self.sync_state = sync_state
        self.get_header_for_hash = self.controller.get_header_for_hash
        self.worker_ack_queue_blk_writer: multiprocessing.Queue = worker_ack_queue_blk_writer
        self.blocks_batch_set_queue_raw = blocks_batch_set_queue_raw
        self.loop = asyncio.get_running_loop()

    def wait_for_batch_completion(self, blocks_batch_set):
        while True:
            block_hash = self.worker_ack_queue_blk_writer.get()
            if block_hash in blocks_batch_set:
                blocks_batch_set.remove(block_hash)
                with self.sync_state.done_blocks_raw_lock:
                    self.sync_state.done_blocks_raw.add(block_hash)
            else:
                header = self.get_header_for_hash(block_hash)
                self.logger.error(f"also wrote unexpected block: {hash_to_hex_str(header.hash)}"
                                  f" {header.height} to disc")

            # all blocks in batch processed
            if len(blocks_batch_set) == 0:
                self.loop.call_soon_threadsafe(self.sync_state.done_blocks_raw_event.set)
                break

    def run(self):
        batch_id = 0
        while True:
            try:
                blocks_batch_set = self.blocks_batch_set_queue_raw.get()
                self.wait_for_batch_completion(blocks_batch_set)
                if batch_id == 0:
                    self.logger.debug(f"ACKs for initial block download received")
                else:
                    self.logger.debug(f"ACKs for batch {batch_id} received")
                batch_id += 1
            except Exception as e:
                self.logger.exception(e)


class BatchCompletionMtree(threading.Thread):
    """Only Processes ACK messages from the MTree worker"""

    def __init__(self, controller: 'Controller', sync_state: 'SyncState',
            worker_ack_queue_mtree, blocks_batch_set_queue_mtree, daemon=True):
        threading.Thread.__init__(self, daemon=daemon)
        self.logger = logging.getLogger("batch-completion-mtree")
        self.controller: Controller = controller
        self.sync_state = sync_state
        self.get_header_for_hash = self.controller.get_header_for_hash
        self.worker_ack_queue_mtree: multiprocessing.Queue = worker_ack_queue_mtree
        self.blocks_batch_set_queue_mtree = blocks_batch_set_queue_mtree
        self.loop = asyncio.get_running_loop()

    def wait_for_batch_completion(self, blocks_batch_set):
        while True:
            block_hash = self.worker_ack_queue_mtree.get()
            if block_hash in blocks_batch_set:
                blocks_batch_set.remove(block_hash)
                with self.sync_state.done_blocks_mtree_lock:
                    self.sync_state.done_blocks_mtree.add(block_hash)
            else:
                header = self.get_header_for_hash(block_hash)
                self.logger.error(f"also wrote unexpected block: {hash_to_hex_str(header.hash)}"
                                  f" {header.height} to disc")

            # all blocks in batch processed
            if len(blocks_batch_set) == 0:
                self.loop.call_soon_threadsafe(self.sync_state.done_blocks_mtree_event.set)
                break

    def run(self):
        batch_id = 0
        while True:
            try:
                blocks_batch_set = self.blocks_batch_set_queue_mtree.get()
                self.wait_for_batch_completion(blocks_batch_set)
                if batch_id == 0:
                    self.logger.debug(f"ACKs for initial block download received")
                else:
                    self.logger.debug(f"ACKs for batch {batch_id} received")
                batch_id += 1
            except Exception as e:
                self.logger.exception(e)


class BatchCompletionPreprocessor(threading.Thread):
    """Only Processes ACK messages from the Preprocessor worker"""

    def __init__(self, controller: 'Controller', sync_state: 'SyncState',
            worker_ack_queue_preproc, blocks_batch_set_queue_preproc, daemon=True):
        threading.Thread.__init__(self, daemon=daemon)
        self.logger = logging.getLogger("batch-completion-preproc")
        self.controller: Controller = controller
        self.sync_state = sync_state
        self.get_header_for_hash = self.controller.get_header_for_hash
        self.worker_ack_queue_preproc: queue.Queue = worker_ack_queue_preproc
        self.blocks_batch_set_queue_preproc = blocks_batch_set_queue_preproc
        self.loop = asyncio.get_running_loop()

    def wait_for_batch_completion(self, blocks_batch_set):
        while True:
            block_hash = self.worker_ack_queue_preproc.get()
            if block_hash in blocks_batch_set:
                blocks_batch_set.remove(block_hash)
                with self.sync_state.done_blocks_preproc_lock:
                    self.sync_state.done_blocks_preproc.add(block_hash)
            else:
                header = self.get_header_for_hash(block_hash)
                self.logger.error(f"also wrote unexpected block: {hash_to_hex_str(header.hash)}"
                                  f" {header.height} to disc")

            # all blocks in batch processed
            if len(blocks_batch_set) == 0:
                self.loop.call_soon_threadsafe(self.sync_state.done_blocks_preproc_event.set)
                break

    def run(self):
        batch_id = 0
        while True:
            try:
                blocks_batch_set = self.blocks_batch_set_queue_preproc.get()
                self.wait_for_batch_completion(blocks_batch_set)
                if batch_id == 0:
                    self.logger.debug(f"ACKs for initial block download received")
                else:
                    self.logger.debug(f"ACKs for batch {batch_id} received")
                batch_id += 1
            except Exception as e:
                self.logger.exception(e)
