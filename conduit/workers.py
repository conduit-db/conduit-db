import asyncio
import io
import multiprocessing
import threading
import time
from functools import partial
from multiprocessing import shared_memory
from typing import List, Tuple

import asyncpg
import bitcoinx
from bitcoinx import read_varint

from .database import pg_connect, load_pg_database, PG_Database
from .logs import logs
from .constants import WORKER_COUNT_TX_PARSERS

try:
    from ._algorithms import preprocessor, parse_block  # cython
except ModuleNotFoundError:
    from .algorithms import preprocessor, parse_block  # pure python

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

    def distribute_load_parsing(
        self, blk_hash, blk_height, blk_start_pos, blk_end_pos, tx_positions
    ) -> List[Tuple[bytes, int, int, List[int]]]:
        divided_tx_positions = []
        count_per_div_parsing = round(len(tx_positions) / WORKER_COUNT_TX_PARSERS)
        # Todo - divide up the block into x number of worker segments for parallel
        #  processing.
        divided_tx_positions = [
            (blk_hash, blk_height, blk_start_pos, blk_end_pos, tx_positions)
        ]
        return divided_tx_positions

    def run(self):
        try:
            while True:
                item = self.worker_in_queue_preproc.get()
                if not item:
                    return  # poison pill stop command

                blk_hash, blk_height, blk_start_pos, blk_end_pos = item

                tx_positions = preprocessor(
                    bytearray(self.shm.buf[blk_start_pos:blk_end_pos])
                )

                divided_parsing_work = self.distribute_load_parsing(
                    blk_hash, blk_height, blk_start_pos, blk_end_pos, tx_positions
                )

                for item in divided_parsing_work:
                    self.worker_in_queue_tx_parse.put(item)

                self.worker_in_queue_mtree.put(
                    (blk_hash, blk_start_pos, blk_end_pos, tx_positions)
                )
        except Exception as e:
            logger.exception(e)
            raise


class TxParser(multiprocessing.Process):
    """
    in: blk_hash, blk_height, blk_start_pos, blk_end_pos, tx_positions_div
    out: N/A - directly updates posgres database
    ack: blk_hash, count_txs_done

    """

    def __init__(
        self,
        shm_name,
        worker_in_queue_tx_parse,
        worker_ack_queue_tx_parse,
        tx_num_value,
    ):
        super(TxParser, self).__init__()
        self.shm = shared_memory.SharedMemory(shm_name, create=False)
        self.worker_in_queue_tx_parse = worker_in_queue_tx_parse
        self.worker_ack_queue_tx_parse = worker_ack_queue_tx_parse
        self.tx_num_value: multiprocessing.Value = tx_num_value

        self.pg_db = None
        self.pg_parsed_rows_queue = None
        self.worker_ack_queue_asyncio = None
        self.loop = None

    def run(self):
        self.loop = asyncio.get_event_loop()
        self.pg_parsed_rows_queue = asyncio.Queue()
        self.worker_ack_queue_asyncio = asyncio.Queue()
        try:
            main_thread = threading.Thread(target=self.main_thread)
            main_thread.start()
            asyncio.get_event_loop().run_until_complete(self.pg_inserts_task())
            print("Coro done...")
            while True:
                time.sleep(0.05)
        except Exception as e:
            logger.exception(e)
            raise

    async def pg_inserts_task(self):
        pg_db: PG_Database
        pg_db = await load_pg_database()
        try:
            while True:
                tx_rows, in_rows, out_rows, set_pd_rows = await self.pg_parsed_rows_queue.get()
                # print(tx_rows)
                # print(in_rows)
                # print(out_rows)
                # print(f"got: {len(tx_rows)} tx_rows; {len(in_rows)} in_rows; "
                #     f"{len(out_rows)} out_rows")
                # await pg_db.pg_create_temp_tables()
                # await pg_db.pg_insert_tx_copy_method(tx_rows)
                # await pg_db.pg_insert_output_copy_method(out_rows)
                # await pg_db.pg_insert_input_copy_method(in_rows)
                # await pg_db.pg_upsert_from_temp_txs()
                # await pg_db.pg_upsert_from_temp_outputs()
                # await pg_db.pg_upsert_from_temp_inputs()
                # await pg_db.pg_drop_temp_tables()

                # result = await pg_db.pg_conn.fetchval("""select COUNT(*) from transactions""")
                # print(f"count transactions table rows={result}")
                # result = await pg_db.pg_conn.fetchval("""select COUNT(*) from inputs""")
                # print(f"count inputs table rows={result}")
                # result = await pg_db.pg_conn.fetchval("""select COUNT(*) from outputs""")
                # print(f"count outputs table rows={result}")
                blk_hash, tx_count = await self.worker_ack_queue_asyncio.get()
                # print(f"item={(blk_hash, tx_count)}")
                self.worker_ack_queue_tx_parse.put((blk_hash, tx_count))
                # ack is only sent when db is updated
        except Exception as e:
            logger.exception(e)
            raise e

    def main_thread(self):
        while True:
            try:
                item = self.worker_in_queue_tx_parse.get()
                if not item:
                    return  # poison pill stop command

                (
                    blk_hash,
                    blk_height,
                    blk_start_pos,
                    blk_end_pos,
                    tx_positions_div,
                ) = item

                txs_count = len(tx_positions_div)

                # increment global tx_num (for use in postgres db tables)
                # if something goes wrong it leaves a gap in the tx_num index (it's ok)

                with self.tx_num_value.get_lock():
                    first_tx_num = self.tx_num_value.value
                    self.tx_num_value.value += txs_count
                    last_tx_num = self.tx_num_value.value - 1  # 0-based index

                tx_rows, in_rows, out_rows, set_pd_rows = parse_block(
                    bytearray(self.shm.buf[blk_start_pos:blk_end_pos]),
                    tx_positions_div,
                    blk_height,
                    first_tx_num,
                    last_tx_num,
                )

                # print(f"parsed rows: len(tx_rows)={len(tx_rows)}, len(in_rows)={len(in_rows)}, "
                #       f"len(out_rows)={len(out_rows)}")
                coro = partial(
                    self.pg_parsed_rows_queue.put, (tx_rows, in_rows, out_rows, set_pd_rows)
                )
                asyncio.run_coroutine_threadsafe(coro(), self.loop)

                item = (blk_hash, len(tx_positions_div))
                coro2 = partial(self.worker_ack_queue_asyncio.put, item)
                asyncio.run_coroutine_threadsafe(coro2(), self.loop)
            except Exception as e:
                logger.exception(e)
                raise


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

    def run(self):
        stream = io.BytesIO(self.shm.buf)
        while True:
            try:
                item = self.worker_in_queue_mtree.get()
                if not item:
                    return

                blk_hash, blk_start_pos, blk_end_pos, tx_positions = item
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
