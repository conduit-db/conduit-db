import asyncio
import logging
import multiprocessing
import os
import sys
import threading
import time
from multiprocessing import shared_memory
from pathlib import Path
from typing import List, Tuple, Optional

import zmq
from bitcoinx import hash_to_hex_str

from conduit_lib.database.lmdb.lmdb_database import LMDB_Database
from conduit_lib.logging_client import setup_tcp_logging
from conduit_lib.stack_tracer import trace_start, trace_stop
from conduit_lib.types import MultiprocessingQueue


MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))


class BlockWriter(multiprocessing.Process):
    """
    Single writer to blocks LMDB database in append only mode (will only ever need one of these to
    max out the underlying storage media throughput capacity I imagine).

    in: blk_hash, blk_start_pos, blk_end_pos
    out: dump to LMDB database
    ack: block_hash done

    """

    def __init__(
            self,
            shm_name: str,
            worker_in_queue_blk_writer: MultiprocessingQueue[Tuple[bytes, int, int]],
            worker_ack_queue_blk_writer: MultiprocessingQueue[bytes]
    ) -> None:
        super(BlockWriter, self).__init__()
        self.shm = shared_memory.SharedMemory(shm_name, create=False)
        self.worker_in_queue_blk_writer = worker_in_queue_blk_writer
        self.worker_ack_queue_blk_writer = worker_ack_queue_blk_writer

        self.batched_blocks: Optional[List[Tuple[bytes, int, int]]] = None
        self.batched_blocks_lock: Optional[threading.Lock] = None
        self.lmdb = None

        self.BLOCK_BATCHING_RATE = 0.3

    def run(self) -> None:
        if sys.platform == "win32":
            setup_tcp_logging(port=54545)
        self.logger = logging.getLogger("raw-block-writer")
        self.logger.setLevel(logging.DEBUG)
        self.logger.info(f"Starting {self.__class__.__name__}...")

        self.batched_blocks = []
        self.batched_blocks_lock = threading.Lock()

        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        # PUB-SUB from Controller to worker to kill the worker
        context3 = zmq.Context()  # type: ignore
        self.kill_worker_socket = context3.socket(zmq.SUB)  # type: ignore
        self.kill_worker_socket.connect("tcp://127.0.0.1:46464")
        self.kill_worker_socket.setsockopt(zmq.SUBSCRIBE, b"stop_signal")

        try:
            if int(os.environ.get('RUN_STACK_TRACER', '0')):
                trace_start(str(MODULE_DIR / f"stack_tracing_{os.urandom(4).hex()}.html"))
            threads = [
                threading.Thread(target=self.main_thread, daemon=True),
                threading.Thread(target=self.flush_thread, daemon=True),
                threading.Thread(target=self.kill_thread, daemon=True)
            ]
            for thread in threads:
                thread.start()

            for thread in threads:
                thread.join()
        except Exception as e:
            self.logger.exception(e)
            raise
        except KeyboardInterrupt:
            self.logger.debug("BlockWriter stopping...")
        finally:
            if int(os.environ.get('RUN_STACK_TRACER', '0')):
                trace_stop()
            self.logger.info(f"Process Stopped")

    def flush_thread(self) -> None:
        assert self.batched_blocks is not None
        assert self.batched_blocks_lock is not None
        try:
            lmdb = LMDB_Database()
            while True:
                if self.batched_blocks:
                    with self.batched_blocks_lock:
                        lmdb.put_blocks(self.batched_blocks, self.shm.buf)
                        self.ack_for_loaded_blocks(self.batched_blocks)
                        self.batched_blocks = []
                time.sleep(0.3)
        except Exception as e:
            self.logger.exception(e)
            raise

    def main_thread(self) -> None:
        assert self.batched_blocks is not None
        assert self.batched_blocks_lock is not None
        try:
            while True:
                item = self.worker_in_queue_blk_writer.get()
                if not item:
                    return  # poison pill stop command
                assert isinstance(item, tuple)

                with self.batched_blocks_lock:
                    blk_hash, blk_start_pos, blk_end_pos = item
                    self.batched_blocks.append((blk_hash, blk_start_pos, blk_end_pos))
        except Exception as e:
            self.logger.exception(e)

    def kill_thread(self) -> None:
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

    def ack_for_loaded_blocks(self, batched_blocks: List[Tuple[bytes, int, int]]) -> None:
        if len(batched_blocks) == 0:
            return None
        # Ack for all flushed blocks
        total_batch_size = 0
        for blk_hash, start_position, stop_position in batched_blocks:
            total_batch_size += stop_position - start_position
            self.worker_ack_queue_blk_writer.put(blk_hash)
        if total_batch_size > 0:
            self.logger.debug(f"total batch size for raw blocks="
                f"{round(total_batch_size/1024/1024, 3)} MB")
