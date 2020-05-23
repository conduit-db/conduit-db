import io
import multiprocessing
from multiprocessing import shared_memory
from typing import List, Tuple

import bitcoinx
from bitcoinx import read_varint

from .logs import logs
from .constants import (
    WORKER_COUNT_TX_PARSERS,
)

logger = logs.get_logger("handlers")


class BlockPreProcessor(multiprocessing.Process):
    """
    in (from block handler):    blk_hash, blk_height, blk_start_pos, blk_end_pos
    out1 (tx parsers):          tx_positions_div, blk_hash, blk_height, blk_start_pos
    out2 (mtree calculators):   tx_positions, blk_hash
    ack: N/A - when other workers are done, preprocessor is done too.
    """

    def __init__(
        self,
        shm_name,
        worker_in_queue_preproc,
        worker_in_queue_tx_parse,
        worker_in_queue_mtree,
    ):
        super(BlockPreProcessor, self).__init__()
        self.shm = shared_memory.SharedMemory(shm_name, create=False)
        self.worker_in_queue_preproc = worker_in_queue_preproc
        self.worker_in_queue_tx_parse = worker_in_queue_tx_parse
        self.worker_in_queue_mtree = worker_in_queue_mtree

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

    def pre_process(self, block_view) -> List[int]:
        stream: io.BytesIO = io.BytesIO(block_view)
        stream.seek(80)  # header
        count = bitcoinx.read_varint(stream.read)
        tx_positions = [stream.tell()]  # start byte pos of each tx in the block
        for i in range(count - 1):
            tx_positions.append(self.seek_to_next_tx(stream))
        return tx_positions

    def distribute_load_parsing(
        self, blk_hash, blk_height, blk_start_pos, tx_positions
    ) -> List[Tuple[bytes, int, int, List[int]]]:
        divided_tx_positions = []
        count_per_div_parsing = round(len(tx_positions) / WORKER_COUNT_TX_PARSERS)
        # Todo - divide up the block into x number of worker segments for parallel
        #  processing.
        divided_tx_positions = [(blk_hash, blk_height, blk_start_pos, tx_positions)]
        return divided_tx_positions

    def run(self):
        try:
            while True:
                item = self.worker_in_queue_preproc.get()
                if not item:
                    return  # poison pill stop command

                blk_hash, blk_height, blk_start_pos, blk_end_pos = item

                tx_positions = self.pre_process(self.shm.buf[blk_start_pos:blk_end_pos])

                divided_parsing_work = self.distribute_load_parsing(
                    blk_hash, blk_height, blk_start_pos, tx_positions
                )

                for item in divided_parsing_work:
                    self.worker_in_queue_tx_parse.put(item)

                self.worker_in_queue_mtree.put((blk_hash, blk_start_pos, tx_positions))
        except Exception as e:
            logger.exception(e)
            raise


class TxParser(multiprocessing.Process):
    """
    in: blk_hash, blk_height, blk_start_pos, tx_positions
    out: N/A - directly updates posgres database
    ack: blk_hash, count_txs_done

    """

    def __init__(self, shm_name, worker_in_queue_tx_parse, worker_ack_queue_tx_parse):
        super(TxParser, self).__init__()
        self.shm = shared_memory.SharedMemory(shm_name, create=False)
        self.worker_in_queue_tx_parse = worker_in_queue_tx_parse
        self.worker_ack_queue_tx_parse = worker_ack_queue_tx_parse

    def run(self):
        while True:
            try:
                item = self.worker_in_queue_tx_parse.get()
                if not item:
                    return  # poison pill stop command

                blk_hash, blk_height, blk_start_pos, tx_positions_div = item
                # logger.debug(f"TxParser got blk_start_pos={blk_start_pos}; tx_positions_div="
                #              f"{tx_positions_div}; len(tx_positions_div)={len(tx_positions_div)}; "
                #              f"blk_height={blk_height}")

                results = []
                # maybe could renew this buffer in-sync with buffer reset if faster
                stream = io.BytesIO(self.shm.buf)
                for pos in tx_positions_div:
                    stream.seek(blk_start_pos + pos)
                    tx = bitcoinx.Tx.read(stream.read)

                    tx_hash: bytes = tx.hash()
                    tx_offset = pos - blk_start_pos  # from beginning of block
                    results.append(
                        (
                            tx.hash(),
                            [
                                f"<pubkey1 for {tx_hash.hex()}>",
                                f"<pubkey2 for" f" {tx_hash.hex()}>",
                            ],
                            blk_height,
                            tx_offset,
                        )
                    )

                # Todo - directly update postgres with metadata via asyncpg connection
                # logger.debug(
                #     f"TxParser: completed {len(results)} txs for block hash: "
                #     f"{blk_hash.hex()}, block height: {blk_height}."
                # )

                self.worker_ack_queue_tx_parse.put((blk_hash, len(tx_positions_div)))
            except Exception as e:
                logger.exception(e)


class MTreeCalculator(multiprocessing.Process):
    """
    Single writer to mtree LMDB database (although LMDB handles concurrent writes internally so
    we could spin up more than one of these)

    in: a list of tx start positions for *all the txs in the block* and the block_hash
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

    def run(self):
        stream = io.BytesIO(self.shm.buf)
        while True:
            try:
                item = self.worker_in_queue_mtree.get()
                if not item:
                    return

                blk_hash, blk_start_pos, tx_positions = item
                results = []

                # hash every tx and calculate merkle tree one layer at a time...
                # if divided into sub-divisions of full tree... will require a
                # coordinating higher level of abstraction

                # push result straight to LMDB (ideally in append only mode)
                # will require a block_id number for each row which will match the block_id for
                # raw_blocks in a different LMDB database.
                self.worker_ack_queue_mtree.put(blk_hash)
            except Exception as e:
                logger.exception(e)


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

    def run(self):
        stream = io.BytesIO(self.shm.buf)
        while True:
            try:
                item = self.worker_in_queue_blk_writer.get()
                if not item:
                    return  # poison pill stop command

                blk_hash, blk_start_pos, blk_end_pos = item

            except Exception as e:
                logger.exception(e)
