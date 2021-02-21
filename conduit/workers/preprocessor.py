import array
import io
import logging
import math
import multiprocessing
import struct
from multiprocessing import shared_memory
from multiprocessing import Array
from typing import List, Tuple

import zmq

from conduit.constants import MsgType
from conduit.logging_client import setup_tcp_logging

from .algorithms import preprocessor


class BlockPreProcessor(multiprocessing.Process):
    """
    in (from block handler):    blk_hash, blk_height, blk_start_pos, blk_end_pos
    out1 (tx parsers):          tx_positions_div, blk_hash, blk_height, blk_start_pos
    out2 (mtree calculators):   tx_positions, blk_hash
    ack: N/A - when other workers are done, preprocessor is done too.
    """

    def __init__(
        self, worker_count_tx_parsers, shm_name, worker_in_queue_preproc,
            worker_in_queue_mtree,
    ):
        super(BlockPreProcessor, self).__init__()

        self.worker_count_tx_parsers = worker_count_tx_parsers
        self.shm = shared_memory.SharedMemory(shm_name, create=False)
        self.worker_in_queue_preproc = worker_in_queue_preproc
        self.worker_in_queue_mtree = worker_in_queue_mtree
        # Todo - Will only handle blocks with up to 1 million txs - for debugging index errors
        self.tx_offsets_array = array.array("Q", [i for i in range(1_000_000)])
        self.logger = logging.getLogger("pre-processor")

    def distribute_load_parsing(
        self, blk_hash, blk_height, blk_start_pos, blk_end_pos, count_added
    ) -> List[Tuple[bytes, int, int, int, int, int, int]]:
        """
        returns item with:
            blk_hash, blk_height, blk_start_pos, blk_end_pos, start_idx_batch, end_idx_batch
        """
        BATCH_COUNT = self.worker_count_tx_parsers
        BATCH_SIZE = math.floor(count_added/BATCH_COUNT)

        first_tx_pos_batch = 0
        if BATCH_COUNT == 1 or BATCH_SIZE <= 100:
            start_idx_batch = 0
            end_idx_batch = start_idx_batch + count_added
            return [(blk_hash, blk_height, blk_start_pos, blk_end_pos, start_idx_batch,
                end_idx_batch, first_tx_pos_batch)]

        divided_tx_positions = []
        # if there is only 1 tx in the block the other batches are empty
        for i in range(BATCH_COUNT):
            if i == BATCH_COUNT - 1:  # last batch
                start_idx_batch = i*BATCH_SIZE
                end_idx_batch = count_added
                divided_tx_positions.extend(
                    [(blk_hash, blk_height, blk_start_pos, blk_end_pos, start_idx_batch,
                        end_idx_batch, first_tx_pos_batch)])
            else:
                start_idx_batch = i*BATCH_SIZE
                end_idx_batch = (i+1)*BATCH_SIZE
                num_txs = end_idx_batch - start_idx_batch
                divided_tx_positions.extend(
                    [(blk_hash, blk_height, blk_start_pos, blk_end_pos, start_idx_batch,
                        end_idx_batch, first_tx_pos_batch)])
                first_tx_pos_batch += num_txs

        # for index, batch in enumerate(divided_tx_positions):
        #     self.logger.debug(f"worker={index+1} batch={batch}")

        return divided_tx_positions

    def run(self):
        # IPC for preprocessor to TxParser
        context = zmq.Context()
        self.mined_tx_socket = context.socket(zmq.PUSH)
        self.mined_tx_socket.bind("tcp://127.0.0.1:55555")

        setup_tcp_logging()
        self.logger = logging.getLogger("pre-processor")
        self.logger.setLevel(logging.DEBUG)
        self.logger.debug(f"starting {self.__class__.__name__}...")

        try:
            while True:
                item = self.worker_in_queue_preproc.get()
                if not item:
                    return  # poison pill stop command

                blk_hash, blk_height, blk_start_pos, blk_end_pos = item

                # write offsets into shared memory integer array and track current idx of array
                # block_offset=0 because preprocessing is so fast it doesn't need to be run in
                # parallel. It's also kind of complicated to actually do correctly
                count_added, _tx_offsets_array = preprocessor(
                    bytearray(self.shm.buf[blk_start_pos:blk_end_pos]),
                    self.tx_offsets_array,
                    block_offset=0
                )

                divided_parsing_work = self.distribute_load_parsing(
                    blk_hash, blk_height, blk_start_pos, blk_end_pos, count_added
                )
                # returns list of:
                # (blk_hash, blk_height, blk_start_pos, blk_end_pos, start_idx_batch,
                #   end_idx_batch, first_tx_pos_batch)

                for item in divided_parsing_work:
                    blk_hash, blk_height, blk_start_pos, blk_end_pos, start_idx_batch, \
                        end_idx_batch, first_tx_pos_batch = item

                    size_array = (end_idx_batch - start_idx_batch) * 8  # unsigned long long 8 bytes
                    packed_array = self.tx_offsets_array[start_idx_batch:end_idx_batch].tobytes()
                    packed_message = struct.pack(
                        f"<II32sQQI{size_array}sI",
                        MsgType.MSG_BLOCK,
                        size_array,
                        blk_hash,
                        blk_height,
                        blk_start_pos,
                        blk_end_pos,
                        packed_array,
                        first_tx_pos_batch
                    )
                    self.mined_tx_socket.send(packed_message)

                # Todo - use shared memory - faster
                tx_offsets = self.tx_offsets_array[0:count_added]
                self.worker_in_queue_mtree.put((blk_hash, blk_start_pos, blk_end_pos, tx_offsets))
        except Exception as e:
            self.logger.exception(e)
            raise
        finally:
            self.mined_tx_socket.close()
