import array
import logging
import multiprocessing
import sys
import threading
import time
from multiprocessing import shared_memory

import cbor2
import zmq
from typing import List

from conduit_lib.database.lmdb.lmdb_database import LMDB_Database
from conduit_lib.database.lmdb.types import MerkleTreeRow
from conduit_lib.logging_client import setup_tcp_logging

from conduit_lib.algorithms import calc_mtree
from conduit_lib.types import MultiprocessingQueue


class MTreeCalculator(multiprocessing.Process):
    """
    Single writer to mtree LMDB database (although LMDB handles concurrent writes internally so
    we could spin up more than one of these)

    in: blk_hash, blk_start_pos, blk_end_pos, tx_positions
    out: N/A - directly dumps to LMDB
    ack: block_hash done

    """

    def __init__(
        self, worker_id: int, shm_name: str, worker_ack_queue_mtree: MultiprocessingQueue[bytes]
    ) -> None:
        super(MTreeCalculator, self).__init__()
        self.BATCHING_RATE = 0.3
        self.worker_id = worker_id
        self.shm = shared_memory.SharedMemory(shm_name, create=False)
        self.worker_ack_queue_mtree = worker_ack_queue_mtree
        self.logger = logging.getLogger(f"merkle-tree={self.worker_id}")

    def process_merkle_tree_batch(self, batch: list[bytes], lmdb: LMDB_Database) -> None:
        batched_merkle_trees: List[MerkleTreeRow] = []
        batched_tx_offsets = []
        batched_acks = []

        t0 = time.perf_counter()
        for packed_msg in batch:
            blk_hash, blk_start_pos, blk_end_pos, tx_offsets_bytes = cbor2.loads(packed_msg)
            tx_offsets = array.array("Q", tx_offsets_bytes)

            mtree = calc_mtree(self.shm.buf[blk_start_pos:blk_end_pos], tx_offsets)
            batched_merkle_trees.append(MerkleTreeRow(blk_hash, mtree, len(tx_offsets)))
            batched_tx_offsets.append((blk_hash, tx_offsets))
            batched_acks.append(blk_hash)

        lmdb.put_merkle_trees(batched_merkle_trees)
        lmdb.put_tx_offsets(batched_tx_offsets)
        t1 = time.perf_counter() - t0
        self.logger.debug(f"mtree & transaction offsets batch flush took {t1} seconds")

        for blk_hash in batched_acks:
            self.worker_ack_queue_mtree.put(blk_hash)

    def run(self) -> None:
        if sys.platform == "win32":
            setup_tcp_logging(port=54545)
        self.logger.setLevel(logging.DEBUG)
        self.logger.info(f"Starting {self.__class__.__name__}...")

        # PUB-SUB from Controller to worker to kill the worker
        context1 = zmq.Context[zmq.Socket[bytes]]()
        self.kill_worker_socket = context1.socket(zmq.SUB)
        self.kill_worker_socket.connect("tcp://127.0.0.1:46464")
        self.kill_worker_socket.setsockopt(zmq.SUBSCRIBE, b"stop_signal")

        t1 = threading.Thread(target=self.kill_thread, daemon=True)
        t1.start()

        lmdb = LMDB_Database()

        batch = []
        prev_time_check = time.time()

        context2 = zmq.Context[zmq.Socket[bytes]]()
        merkle_tree_socket = context2.socket(zmq.PULL)
        merkle_tree_socket.connect("tcp://127.0.0.1:41835")

        try:
            while True:
                try:
                    if merkle_tree_socket.poll(1000, zmq.POLLIN):
                        packed_msg = merkle_tree_socket.recv(zmq.NOBLOCK)
                        if not packed_msg:
                            return  # poison pill stop command
                        batch.append(bytes(packed_msg))
                    else:
                        time_diff = time.time() - prev_time_check
                        # Todo: This might be redundant with poll(1000)
                        #  1 second is longer than self.BLOCK_BATCHING_RATE
                        if time_diff > self.BATCHING_RATE:
                            prev_time_check = time.time()
                            if batch:
                                self.process_merkle_tree_batch(batch, lmdb)
                            batch = []
                except zmq.error.Again:
                    self.logger.debug(f"zmq.error.Again")
                    continue
        except KeyboardInterrupt:
            # This will get logged by the multi-processing if we raise it. The expected behaviour
            # is therefore that we catch this and recognise it as an exit condition.
            return
        except Exception as e:
            self.logger.exception(e)
        finally:
            self.logger.info("Closing mined_blocks_thread")
            merkle_tree_socket.close()
            # merkle_tree_socket.term()

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
