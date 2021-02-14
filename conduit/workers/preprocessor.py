import io
import logging
import math
import multiprocessing
from multiprocessing import shared_memory
from typing import List, Tuple
from bitcoinx import read_varint

from conduit.constants import WORKER_COUNT_TX_PARSERS, MsgType
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
            worker_in_queue_tx_parse, worker_in_queue_mtree,
    ):
        super(BlockPreProcessor, self).__init__()
        self.worker_count_tx_parsers = worker_count_tx_parsers
        self.shm = shared_memory.SharedMemory(shm_name, create=False)
        self.worker_in_queue_preproc = worker_in_queue_preproc
        self.worker_in_queue_tx_parse = worker_in_queue_tx_parse
        self.worker_in_queue_mtree = worker_in_queue_mtree
        self.logger = logging.getLogger("pre-processor")

    def seek_to_next_tx(self, stream):
        # version
        stream.seek(4, io.SEEK_CUR)

        # tx_in block
        count_tx_in = read_varint(stream.read)
        for i in range(count_tx_in):
            stream.seek(36, io.SEEK_CUR)  # prev_hash + prev_idx
            script_sig_len = read_varint(stream.read)
            stream.seek(script_sig_len, io.SEEK_CUR)  # script_sig
            stream.seek(4, io.SEEK_CUR)  # sequence

        # tx_out block
        count_tx_out = read_varint(stream.read)
        for i in range(count_tx_out):
            stream.seek(8, io.SEEK_CUR)  # value
            script_pubkey_len = read_varint(stream.read)  # script_pubkey
            stream.seek(script_pubkey_len, io.SEEK_CUR)  # script_sig

        # lock_time
        stream.seek(4, io.SEEK_CUR)
        return stream.tell()

    def distribute_load_parsing(
        self, blk_hash, blk_height, blk_start_pos, blk_end_pos, tx_offsets
    ) -> List[Tuple[bytes, int, int, List[int]]]:

        BATCH_COUNT = self.worker_count_tx_parsers
        BATCH_SIZE = math.floor(len(tx_offsets)/BATCH_COUNT)

        first_tx_pos_batch = 0
        if BATCH_COUNT == 1 or BATCH_SIZE == 1:
            return [(blk_hash, blk_height, blk_start_pos, blk_end_pos, tx_offsets,
                first_tx_pos_batch)]

        divided_tx_positions = []
        # if there is only 1 tx in the block the other batches are empty
        for i in range(BATCH_COUNT):
            if i == BATCH_COUNT - 1:  # last batch
                batch_tx_offsets = tx_offsets[i*BATCH_SIZE:]
                num_txs = len(batch_tx_offsets)
                if num_txs:
                    divided_tx_positions.extend(
                        [(blk_hash, blk_height, blk_start_pos, blk_end_pos, batch_tx_offsets,
                            first_tx_pos_batch)])
            else:
                batch_tx_offsets = tx_offsets[i*BATCH_SIZE:(i+1)*BATCH_SIZE]
                num_txs = len(batch_tx_offsets)
                if num_txs:
                    divided_tx_positions.extend(
                        [(blk_hash, blk_height, blk_start_pos, blk_end_pos, batch_tx_offsets,
                            first_tx_pos_batch)])
                    first_tx_pos_batch += num_txs

        # for index, batch in enumerate(divided_tx_positions):
        #     self.logger.debug(f"index={index} batch={batch}")

        return divided_tx_positions

    def run(self):
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

                tx_offsets = preprocessor(bytearray(self.shm.buf[blk_start_pos:blk_end_pos]))

                divided_parsing_work = self.distribute_load_parsing(
                    blk_hash, blk_height, blk_start_pos, blk_end_pos, tx_offsets
                )

                for item in divided_parsing_work:
                    self.worker_in_queue_tx_parse.put((MsgType.MSG_BLOCK, item))

                self.worker_in_queue_mtree.put((blk_hash, blk_start_pos, blk_end_pos, tx_offsets))
        except Exception as e:
            self.logger.exception(e)
            raise