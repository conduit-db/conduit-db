import logging
import multiprocessing
import time
from multiprocessing import shared_memory

from conduit.database.lmdb_database import LMDB_Database
from conduit.logging_client import setup_tcp_logging

from .algorithms import calc_mtree

# setup_tcp_logging()

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

                self.worker_ack_queue_mtree.put(blk_hash)
            except Exception as e:
                self.logger.exception(e)
