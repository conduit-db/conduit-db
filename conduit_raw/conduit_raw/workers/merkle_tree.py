import logging
import multiprocessing
import threading
import time
from multiprocessing import shared_memory

import zmq

from conduit_lib.database.lmdb.lmdb_database import LMDB_Database
from conduit_lib.logging_client import setup_tcp_logging

from conduit_lib.algorithms import calc_mtree


class MTreeCalculator(multiprocessing.Process):
    """
    Single writer to mtree LMDB database (although LMDB handles concurrent writes internally so
    we could spin up more than one of these)

    in: blk_hash, blk_start_pos, blk_end_pos, tx_positions
    out: N/A - directly dumps to LMDB
    ack: block_hash done

    """

    def __init__(
        self, worker_id, shm_name, worker_in_queue_mtree, worker_ack_queue_mtree,
    ):
        super(MTreeCalculator, self).__init__()
        self.worker_id = worker_id
        self.shm = shared_memory.SharedMemory(shm_name, create=False)
        self.worker_in_queue_mtree = worker_in_queue_mtree
        self.worker_ack_queue_mtree = worker_ack_queue_mtree
        self.logger = logging.getLogger(f"merkle-tree={self.worker_id}")

    def run(self):
        setup_tcp_logging(port=54545)
        self.logger.setLevel(logging.DEBUG)
        self.logger.debug(f"starting {self.__class__.__name__}...")

        # PUB-SUB from Controller to worker to kill the worker
        context3 = zmq.Context()
        self.kill_worker_socket = context3.socket(zmq.SUB)
        self.kill_worker_socket.connect("tcp://127.0.0.1:46464")
        self.kill_worker_socket.setsockopt(zmq.SUBSCRIBE, b"stop_signal")

        t1 = threading.Thread(target=self.kill_thread, daemon=True)
        t1.start()

        lmdb_db = LMDB_Database()
        while True:
            try:
                item = self.worker_in_queue_mtree.get()
                if not item:
                    return

                blk_hash, blk_start_pos, blk_end_pos, tx_offsets = item
                # Todo - add batching to this like the other workers.
                # t0 = time.perf_counter()
                mtree = calc_mtree(self.shm.buf[blk_start_pos:blk_end_pos], tx_offsets)
                lmdb_db.put_merkle_tree(blk_hash, mtree)
                lmdb_db.put_tx_offsets(blk_hash, tx_offsets)
                # t1 = time.perf_counter() - t0
                # self.logger.debug(f"mtree and tx_offsets flush took {t1} seconds")

                self.worker_ack_queue_mtree.put(blk_hash)
            except KeyboardInterrupt:
                self.logger.debug("MTreeCalculator stopping...")
            except Exception as e:
                self.logger.exception(e)
            finally:
                self.logger.info(f"Process Stopped")

    def kill_thread(self):
        while True:
            try:
                message = self.kill_worker_socket.recv()
                if message == b"stop_signal":
                    self.shm.close()
                    self.logger.info(f"Process Stopped")
                    break
                time.sleep(0.2)

            except Exception as e:
                self.logger.exception(e)