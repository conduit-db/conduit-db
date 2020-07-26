import asyncio
import logging
import multiprocessing
import threading
import time
from functools import partial
from multiprocessing import shared_memory
from typing import List, Tuple

from conduit.database.lmdb_database import LMDB_Database
from conduit.logging_client import setup_tcp_logging

class BlockWriter(multiprocessing.Process):
    """
    Single writer to blocks LMDB database in append only mode (will only ever need one of these to
    max out the underlying storage media throughput capacity I imagine).

    in: blk_hash, blk_start_pos, blk_end_pos
    out: dump to LMDB database
    ack: block_hash done

    """

    def __init__(
        self, shm_name, worker_in_queue_blk_writer, worker_ack_queue_blk_writer,
    ):
        super(BlockWriter, self).__init__()
        self.shm = shared_memory.SharedMemory(shm_name, create=False)
        self.worker_in_queue_blk_writer = worker_in_queue_blk_writer
        self.worker_ack_queue_blk_writer = worker_ack_queue_blk_writer
        self.worker_asyncio_block_writer_in_queue = None
        self.logger = logging.getLogger("raw-block-writer")

        self.batched_blocks = None
        self.lmdb = None

        # accumulate x seconds worth of blocks or MAX_BLOCK_BATCH_SIZE (whichever comes first)
        self.MIN_BLOCK_BATCH_SIZE = 500
        self.MAX_QUEUE_WAIT_TIME = 0.3
        self.time_prev = time.time()

    def run(self):
        setup_tcp_logging()
        self.logger = logging.getLogger("raw-block-writer")
        self.logger.setLevel(logging.DEBUG)
        self.logger.debug(f"starting {self.__class__.__name__}...")

        self.lmdb = LMDB_Database()

        self.loop = asyncio.get_event_loop()
        self.worker_asyncio_block_writer_in_queue = asyncio.Queue()

        try:
            main_thread = threading.Thread(target=self.main_thread)
            main_thread.start()
            asyncio.get_event_loop().run_until_complete(self.lmdb_inserts_task())
            self.logger("Coro done...")
            while True:
                time.sleep(0.05)
        except Exception as e:
            self.logger.exception(e)
            raise

    async def lmdb_inserts_task(self):
        self.batched_blocks = []
        while True:
            try:
                # no timeout functionality on asyncio queues...
                # todo - perhaps a special message from session.py is better to signal
                #  a full buffer and just flush a full buffer each time. Would stop the
                #  wasteful polling.
                item = await asyncio.wait_for(
                    self.worker_asyncio_block_writer_in_queue.get(),
                    timeout=self.MAX_QUEUE_WAIT_TIME,
                )
                blk_hash, blk_start_pos, blk_end_pos = item
                self.batched_blocks.append((blk_hash, blk_start_pos, blk_end_pos))

                # print(f"got block from queue unpacked: {blk_hash} {blk_start_pos} {blk_end_pos}")

                if len(self.batched_blocks) >= self.MIN_BLOCK_BATCH_SIZE:
                    # logger.debug("block batching hit max batch size - loading batched blocks...")
                    self.lmdb.put_blocks(self.batched_blocks, self.shm.buf)
                    self.ack_for_loaded_blocks(self.batched_blocks)
                    self.batched_blocks = []

            except asyncio.TimeoutError:
                # logger.debug("block batching timed out - loading batched blocks...")
                self.lmdb.put_blocks(self.batched_blocks, self.shm.buf)
                self.ack_for_loaded_blocks(self.batched_blocks)
                self.batched_blocks = []
                continue

            except Exception as e:
                self.logger.exception(e)
                raise

    def main_thread(self):
        while True:
            try:
                item = self.worker_in_queue_blk_writer.get()
                if not item:
                    return  # poison pill stop command
                assert isinstance(item, tuple)

                blk_hash, blk_start_pos, blk_end_pos = item
                coro = partial(
                    self.worker_asyncio_block_writer_in_queue.put,
                    (blk_hash, blk_start_pos, blk_end_pos),
                )
                asyncio.run_coroutine_threadsafe(coro(), self.loop)

            except Exception as e:
                self.logger.exception(e)

    def ack_for_loaded_blocks(self, batched_blocks: List[Tuple[bytes, int, int]]):
        if len(batched_blocks) == 0:
            return
        # Ack for all flushed blocks
        total_batch_size = 0
        for blk_hash, start_position, stop_position in self.batched_blocks:
            total_batch_size += stop_position - start_position
            # logger.debug(f"flushed raw block data for block hash={hash_to_hex_str(blk_hash)}, "
            #       f"size={stop_position-start_position} bytes")
            self.worker_ack_queue_blk_writer.put(blk_hash)
            # Fixme - need to get from this queue in the session manager side
            _discard_result = self.worker_ack_queue_blk_writer.get()
        if total_batch_size > 0:
            self.logger.debug(
                f"total batch size for raw blocks={round(total_batch_size/1024/1024, 3)} MB"
            )
