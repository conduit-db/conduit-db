import array
import logging
from multiprocessing import shared_memory
import queue
import threading
import time

import cbor2
import zmq

from conduit_lib.algorithms import preprocessor


class BlockPreProcessor(threading.Thread):
    """
    in (from block handler):    blk_hash, blk_height, blk_start_pos, blk_end_pos
    out1 (tx parsers):          tx_positions_div, blk_hash, blk_height, blk_start_pos
    out2 (mtree calculators):   tx_positions, blk_hash
    ack: N/A - when other workers are done, preprocessor is done too.
    """

    def __init__(
            self,
            shm_name: str,
            worker_in_queue_preproc: queue.Queue[tuple[bytes, int, int, int]],
            worker_ack_queue_preproc: queue.Queue[bytes],
            daemon: bool=True
    ) -> None:
        super(BlockPreProcessor, self).__init__(daemon=daemon)

        self.shm = shared_memory.SharedMemory(shm_name, create=False)
        self.worker_in_queue_preproc = worker_in_queue_preproc
        self.worker_ack_queue_preproc = worker_ack_queue_preproc
        self.tx_offsets_array = array.array("Q", [i for i in range(4_000_000)])
        self.logger = logging.getLogger("pre-processor")

    def run(self) -> None:
        self.logger.info(f"Starting {self.__class__.__name__}...")

        context1 = zmq.Context[zmq.Socket[bytes]]()
        merkle_tree_socket = context1.socket(zmq.PUSH)
        merkle_tree_socket.bind("tcp://127.0.0.1:41835")

        try:
            while True:
                item = self.worker_in_queue_preproc.get()
                if not item:
                    return  # poison pill stop command

                blk_hash, blk_height, blk_start_pos, blk_end_pos = item

                # tx_offsets_array is a preallocated array.array for better cffi with Cython
                t0 = time.perf_counter()
                count_added, _tx_offsets_array = preprocessor(
                    self.shm.buf[blk_start_pos:blk_end_pos],
                    self.tx_offsets_array,
                    block_offset=0
                )
                t1 = time.perf_counter() - t0
                # self.logger.debug(f"Preprocessing block of size: {blk_end_pos-blk_start_pos} took: {t1} seconds")
                tx_offsets = self.tx_offsets_array[0:count_added]
                packed_msg = cbor2.dumps((blk_hash, blk_start_pos, blk_end_pos, tx_offsets.tobytes()))
                merkle_tree_socket.send(packed_msg)
                self.worker_ack_queue_preproc.put(blk_hash)
        except Exception as e:
            self.logger.exception("Caught exception")
            raise
