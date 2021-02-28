import logging
import multiprocessing
import sys
import threading
import time
from multiprocessing import shared_memory

import zmq

from conduit.database.lmdb_database import LMDB_Database
from conduit.logging_client import setup_tcp_logging

from .algorithms import calc_mtree


class MTreeCalculator(multiprocessing.Process):
    """
    Single writer to mtree LMDB database (although LMDB handles concurrent writes internally so
    we could spin up more than one of these)

    in: blk_hash, blk_start_pos, blk_end_pos, tx_positions
    out: N/A - directly dumps to LMDB
    ack: block_hash done

    """

    def __init__(
        self, shm_name, worker_in_queue_mtree, worker_ack_queue_mtree,
    ):
        super(MTreeCalculator, self).__init__()
        self.shm = shared_memory.SharedMemory(shm_name, create=False)
        self.worker_in_queue_mtree = worker_in_queue_mtree
        self.worker_ack_queue_mtree = worker_ack_queue_mtree
        self.logger = logging.getLogger("merkle-tree")

    def run(self):
        setup_tcp_logging()
        self.logger = logging.getLogger("merkle-tree")
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

                t0 = time.time()
                mtree = calc_mtree(self.shm.buf[blk_start_pos:blk_end_pos], tx_offsets)
                lmdb_db.put_merkle_tree(mtree, blk_hash)
                t1 = time.time() - t0
                # logger.debug(f"full mtree calculation took {t1} seconds")
                # Todo - add batching to this like the other workers.

                # Todo - NOTHING PULLS FROM THIS YET!
                self.worker_ack_queue_mtree.put(blk_hash)
            except Exception as e:
                self.logger.exception(e)

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