import asyncio
import logging
import multiprocessing
import os
import sys
import threading
import time
from functools import partial
from multiprocessing import shared_memory
from typing import List, Tuple

import zmq
from bitcoinx import hash_to_hex_str

from conduit_lib.database.lmdb.lmdb_database import LMDB_Database
from conduit_lib.logging_client import setup_tcp_logging


class BlockWriter(multiprocessing.Process):
    """
    Single writer to blocks LMDB database in append only mode (will only ever need one of these to
    max out the underlying storage media throughput capacity I imagine).

    in: blk_hash, blk_start_pos, blk_end_pos
    out: dump to LMDB database
    ack: block_hash done

    """

    def __init__(
        self, shm_name, worker_in_queue_blk_writer, worker_ack_queue_blk_writer
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
        # Todo - when batch sizes are smaller than 500 could cause lost data if not flushed
        #  may need controller to send a boolean value to indicate it is the last block in
        #  the batch and to therefore flush immediately
        self.MIN_BLOCK_BATCH_SIZE = 500
        self.MAX_QUEUE_WAIT_TIME = 0.3
        self.time_prev = time.time()

    def run(self):
        if sys.platform == "win32":
            setup_tcp_logging(port=54545)
        self.logger = logging.getLogger("raw-block-writer")
        self.logger.setLevel(logging.DEBUG)
        self.logger.info(f"Starting {self.__class__.__name__}...")

        self.lmdb = LMDB_Database()

        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        self.worker_asyncio_block_writer_in_queue = asyncio.Queue()

        # PUB-SUB from Controller to worker to kill the worker
        context3 = zmq.Context()
        self.kill_worker_socket = context3.socket(zmq.SUB)
        self.kill_worker_socket.connect("tcp://127.0.0.1:46464")
        self.kill_worker_socket.setsockopt(zmq.SUBSCRIBE, b"stop_signal")

        try:
            t1 = threading.Thread(target=self.main_thread, daemon=True)
            t1.start()
            t2 = threading.Thread(target=self.kill_thread, daemon=True)
            t2.start()
            self.loop.run_until_complete(self.lmdb_inserts_task())
            self.logger.debug("Coro done...")
        except Exception as e:
            self.logger.exception(e)
            raise
        except KeyboardInterrupt:
            self.logger.debug("BlockWriter stopping...")
        finally:
            self.lmdb.close()
            self.logger.info(f"Process Stopped")

    async def lmdb_inserts_task(self):
        self.lmdb: LMDB_Database
        self.batched_blocks = []
        while True:
            try:
                # no timeout functionality on asyncio queues...
                # todo - perhaps a special message from controller.py is better to signal
                #  a full buffer and just flush a full buffer each time. Would stop the
                #  wasteful polling.
                item = await asyncio.wait_for(
                    self.worker_asyncio_block_writer_in_queue.get(),
                    timeout=self.MAX_QUEUE_WAIT_TIME,
                )
                blk_hash, blk_start_pos, blk_end_pos = item
                self.batched_blocks.append((blk_hash, blk_start_pos, blk_end_pos))
                block_bytes = blk_end_pos - blk_start_pos

                # self.logger.debug(f"got block from queue unpacked: {blk_hash} {blk_start_pos}"
                #     f" {blk_end_pos} block_bytes={block_bytes}")

                if len(self.batched_blocks) >= self.MIN_BLOCK_BATCH_SIZE:
                    self.logger.debug("block batching hit max batch size - loading batched blocks...")
                    self.lmdb.put_blocks(self.batched_blocks, self.shm.buf)
                    self.ack_for_loaded_blocks(self.batched_blocks)
                    self.batched_blocks = []

            except asyncio.TimeoutError:
                # self.logger.debug("block batching timed out - loading batched blocks...")
                if self.batched_blocks:
                    self.lmdb.put_blocks(self.batched_blocks, self.shm.buf)
                    self.ack_for_loaded_blocks(self.batched_blocks)
                    self.batched_blocks = []
                continue

            except KeyboardInterrupt:
                self.logger.debug("lmdb_inserts_task stopping...")

            except Exception as e:
                self.logger.exception(e)
                raise

    def main_thread(self):
        try:
            while True:
                item = self.worker_in_queue_blk_writer.get()
                if not item:
                    return  # poison pill stop command
                assert isinstance(item, tuple)

                blk_hash, blk_start_pos, blk_end_pos = item
                # self.logger.debug(f"got item blk_hash={hash_to_hex_str(blk_hash)}")
                coro = partial(
                    self.worker_asyncio_block_writer_in_queue.put,
                    (blk_hash, blk_start_pos, blk_end_pos),
                )
                asyncio.run_coroutine_threadsafe(coro(), self.loop)
        except Exception as e:
            self.logger.exception(e)

    def kill_thread(self):
        try:
            while True:
                message = self.kill_worker_socket.recv()
                if message == b"stop_signal":
                    self.shm.close()
                    self.logger.info(f"Process Stopped")
                    break
                time.sleep(0.2)
        except Exception as e:
            self.logger.exception(e)
        finally:
            self.logger.info(f"Process Stopped")
            sys.exit(0)

    def ack_for_loaded_blocks(self, batched_blocks: List[Tuple[bytes, int, int]]):
        if len(batched_blocks) == 0:
            return
        # Ack for all flushed blocks
        total_batch_size = 0
        for blk_hash, start_position, stop_position in self.batched_blocks:
            total_batch_size += stop_position - start_position
            # self.logger.debug(f"flushed raw block data for block hash={hash_to_hex_str(blk_hash)}, "
            #       f"size={stop_position-start_position} bytes")
            self.worker_ack_queue_blk_writer.put(blk_hash)
        if total_batch_size > 0:
            self.logger.debug(f"total batch size for raw blocks="
                f"{round(total_batch_size/1024/1024, 3)} MB")
