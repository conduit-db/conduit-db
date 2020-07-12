import asyncio
import async_timeout
import io
import multiprocessing
import threading
import time
from functools import partial
from multiprocessing import shared_memory
from typing import List, Tuple, Dict

import asyncpg
from bitcoinx import read_varint, hash_to_hex_str

from .database.postgres_database import load_pg_database, PG_Database
from .database.lmdb_database import LMDB_Database
from .logs import logs
from .constants import WORKER_COUNT_TX_PARSERS

try:
    from ._algorithms import preprocessor, parse_block  # cython
except ModuleNotFoundError:
    from .algorithms import preprocessor, parse_block, calc_mtree  # pure python

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


def handle_collision(tx_rows, in_rows, out_rows, set_pd_rows, blk_acks,
                colliding_tx_shash):
    """only expect to find 1 collision and then reattempt the batch
    (based on probabilities)"""
    try:
        colliding_index = None
        for index, tx_row in enumerate(tx_rows):
            if tx_row[0] == int(colliding_tx_shash):
                logger.error(f"detected colliding tx_row: {tx_row}")
                colliding_index = index
                break
            else:
                continue

        if colliding_index is not None and tx_row[3] == 0:  # position == 0 (i.e. coinbase)
            logger.error("coinbase colliding tx - ignore")
            _colliding_tx_row = tx_rows.pop(colliding_index)  # do nothing
            # todo... need to chase after the in_rows, out_rows and pd_rows
            return tx_rows
        elif colliding_index is not None and tx_row[3] != 0:  # this is a 'real' collision
            _colliding_tx_row = tx_rows.pop(colliding_index)
            logger.error("non-coinbase colliding tx - this needs to be handled...")
            return tx_rows
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
    ):
        super(TxParser, self).__init__()
        self.shm = shared_memory.SharedMemory(shm_name, create=False)
        self.worker_in_queue_tx_parse = worker_in_queue_tx_parse
        self.worker_ack_queue_tx_parse = worker_ack_queue_tx_parse
        self.worker_asyncio_tx_parser_in_queue = None
        self.worker_asyncio_tx_parser_ack_queue = None

        self.pg_db = None
        self.loop = None

        # batched rows
        self.batched_tx_rows = []
        self.batched_in_rows = []
        self.batched_out_rows = []
        self.batched_set_pd_rows = []
        self.batched_block_acks = []
        self.batched_prev_flush_time = time.time()

        # accumulate x seconds worth of txs or MAX_TX_BATCH_SIZE (whichever comes first)
        self.MAX_TX_BATCH_SIZE = 50000
        self.BATCHED_MAX_WAIT_TIME = 0.3

    def run(self):
        self.loop = asyncio.get_event_loop()
        self.worker_asyncio_tx_parser_in_queue = asyncio.Queue()
        self.worker_asyncio_tx_parser_ack_queue = asyncio.Queue()
        try:
            main_thread = threading.Thread(target=self.main_thread)
            main_thread.start()
            asyncio.get_event_loop().run_until_complete(self.pg_inserts_task())
            logger.debug("TxParser event loop completed...")
            while True:
                time.sleep(0.05)
        except Exception as e:
            logger.exception(e)
            raise

    def reset_batched_rows(self):
        self.batched_tx_rows = []
        self.batched_in_rows = []
        self.batched_out_rows = []
        self.batched_set_pd_rows = []
        self.batched_block_acks = []

    async def extend_batched_rows(self, blk_rows):
        tx_rows, in_rows, out_rows, set_pd_rows = blk_rows
        self.batched_tx_rows.extend(tx_rows)
        self.batched_in_rows.extend(in_rows)
        self.batched_out_rows.extend(out_rows)
        self.batched_set_pd_rows.extend(set_pd_rows)

    async def append_block_acks(self, blk_acks):
        self.batched_block_acks.append(blk_acks)

    async def flush_to_postgres(
        self, tx_rows, in_rows, out_rows, set_pd_rows, blk_acks
    ):
        # Todo - ideally wrap these all in a single transaction for consistency
        try:
            t0 = time.time()
            await self.pg_db.pg_create_temp_tables()
            await self.pg_db.pg_bulk_load_tx_rows(tx_rows)
            t1 = time.time() - t0
            logger.info(f"elapsed time for pg_bulk_load_tx_rows = {t1} seconds for {len(tx_rows)}")

            t0 = time.time()
            await self.pg_db.pg_bulk_load_output_rows(out_rows)
            t1 = time.time() - t0
            logger.info(f"elapsed time for pg_bulk_load_output_rows = {t1} seconds for {len(out_rows)}")

            t0 = time.time()
            await self.pg_db.pg_bulk_load_input_rows(in_rows)
            t1 = time.time() - t0
            logger.info(f"elapsed time for pg_bulk_load_input_rows = {t1} seconds for {len(in_rows)}")

            t0 = time.time()
            await self.pg_db.pg_bulk_load_pushdata_rows(set_pd_rows)
            t1 = time.time() - t0
            logger.info(f"elapsed time for pg_bulk_load_pushdata_rows = {t1} seconds for {len(set_pd_rows)}")
            await self.pg_db.pg_drop_temp_tables()

            # Ack for all flushed blocks
            for blk_hash, tx_count in blk_acks:
                # print(f"updated block hash={blk_hash}, with tx_count={tx_count}")
                self.worker_ack_queue_tx_parse.put((blk_hash, tx_count))

            self.reset_batched_rows()
        except asyncpg.exceptions.UniqueViolationError as e:
            error_text = e.as_dict()['detail']
            # extract tx_shash from brackets - not best practice.. but most efficient
            colliding_tx_shash = error_text.split('(')[2].split(')')[0]
            logger.error(f"colliding tx_shash = {colliding_tx_shash}")
            tx_rows = handle_collision(tx_rows, in_rows, out_rows, set_pd_rows, blk_acks,
                colliding_tx_shash)
            await self.pg_db.pg_drop_temp_tables()

            # retry without the colliding tx
            await self.flush_to_postgres(tx_rows, in_rows, out_rows, set_pd_rows, blk_acks)
            logger.error(e)

    async def pg_inserts_task(self):
        """bulk inserts to postgres.

        NOTE: this cannot be done via multiple coroutines per worker process. Varying
        times for the insert to complete, would cause each worker coroutine to pull from
        worker_ack_queue_asyncio out of sequence. This is not a bottleneck anyway
        - especially with larger blocks."""
        self.pg_db: PG_Database = await load_pg_database()
        await self.pg_db.pg_update_settings()
        try:
            while True:
                try:
                    # see 'footnote_1'
                    blk_rows = await asyncio.wait_for(
                        self.worker_asyncio_tx_parser_in_queue.get(), timeout=self.BATCHED_MAX_WAIT_TIME)
                    blk_acks = await self.worker_asyncio_tx_parser_ack_queue.get()
                    await self.extend_batched_rows(blk_rows)
                    await self.append_block_acks(blk_acks)

                except asyncio.TimeoutError:
                    # print("timed out...")
                    if len(self.batched_tx_rows) != 0:
                        await self.flush_to_postgres(
                            self.batched_tx_rows,
                            self.batched_in_rows,
                            self.batched_out_rows,
                            self.batched_set_pd_rows,
                            self.batched_block_acks,
                        )
                    continue

                # This is not in the "try block" because I wasn't sure if it would be atomic
                # i.e. what if it times out half-way through committing rows to the db?
                if len(self.batched_tx_rows) > self.MAX_TX_BATCH_SIZE:
                    # print("hit max tx batch size...")
                    await self.flush_to_postgres(
                        self.batched_tx_rows,
                        self.batched_in_rows,
                        self.batched_out_rows,
                        self.batched_set_pd_rows,
                        self.batched_block_acks,
                    )
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

                tx_rows, in_rows, out_rows, set_pd_rows = parse_block(
                    bytes(self.shm.buf[blk_start_pos:blk_end_pos]),
                    tx_positions_div,
                    blk_height,
                )

                # print(f"parsed rows: len(tx_rows)={len(tx_rows)}, len(in_rows)={len(in_rows)}, "
                #       f"len(out_rows)={len(out_rows)}")
                coro = partial(
                    self.worker_asyncio_tx_parser_in_queue.put,
                    (tx_rows, in_rows, out_rows, set_pd_rows),
                )
                asyncio.run_coroutine_threadsafe(coro(), self.loop)

                # need to ack from asyncio event loop because that's where the context
                # is for when the data has indeed been flushed to db
                item = (blk_hash, len(tx_positions_div))
                coro2 = partial(self.worker_asyncio_tx_parser_ack_queue.put, item)
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
        lmdb_db = LMDB_Database()
        while True:
            try:
                item = self.worker_in_queue_mtree.get()
                if not item:
                    return

                blk_hash, blk_start_pos, blk_end_pos, tx_offsets = item

                t0 = time.time()
                mtree = calc_mtree(self.shm.buf[blk_start_pos: blk_end_pos], tx_offsets)
                lmdb_db.put_merkle_tree(mtree, blk_hash)
                t1 = time.time() - t0
                # logger.debug(f"full mtree calculation took {t1} seconds")
                # Todo - add batching to this like the other workers.

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
        self.worker_asyncio_block_writer_in_queue = None

        self.batched_blocks = None
        self.lmdb = None

        # accumulate x seconds worth of blocks or MAX_BLOCK_BATCH_SIZE (whichever comes first)
        self.MIN_BLOCK_BATCH_SIZE = 500
        self.MAX_QUEUE_WAIT_TIME = 0.3
        self.time_prev = time.time()

    def run(self):
        self.loop = asyncio.get_event_loop()
        self.worker_asyncio_block_writer_in_queue = asyncio.Queue()

        try:
            main_thread = threading.Thread(target=self.main_thread)
            main_thread.start()
            asyncio.get_event_loop().run_until_complete(self.lmdb_inserts_task())
            print("Coro done...")
            while True:
                time.sleep(0.05)
        except Exception as e:
            logger.exception(e)
            raise

    async def lmdb_inserts_task(self):
        self.lmdb = LMDB_Database()
        self.batched_blocks = []
        while True:
            try:
                # no timeout functionality on asyncio queues...
                # todo - perhaps a special message from session.py is better to signal
                #  a full buffer and just flush a full buffer each time. Would stop the
                #  wasteful polling.
                item = await asyncio.wait_for(self.worker_asyncio_block_writer_in_queue.get(),
                    timeout=self.MAX_QUEUE_WAIT_TIME)
                blk_hash, blk_start_pos, blk_end_pos = item
                self.batched_blocks.append((blk_hash, blk_start_pos, blk_end_pos))

                # print(f"got block from queue unpacked: {blk_hash} {blk_start_pos} {blk_end_pos}")

                if len(self.batched_blocks) >= self.MIN_BLOCK_BATCH_SIZE:
                    # logger.debug("block batching hit max batch size - loading batched blocks...")
                    self.lmdb.put_blocks(self.batched_blocks, self.shm.buf)
                    self.ack_for_loaded_blocks(self.batched_blocks)
                    self.batched_blocks = []

            except asyncio.TimeoutError:
                # logger.debug("block batching timed out - loading batched blocks...")
                self.lmdb.put_blocks(self.batched_blocks, self.shm.buf)
                self.ack_for_loaded_blocks(self.batched_blocks)
                self.batched_blocks = []
                continue

            except Exception as e:
                logger.exception(e)
                raise

    def main_thread(self):
        while True:
            try:
                item = self.worker_in_queue_blk_writer.get()
                if not item:
                    return  # poison pill stop command
                assert isinstance(item, tuple)

                blk_hash, blk_start_pos, blk_end_pos = item
                coro = partial(self.worker_asyncio_block_writer_in_queue.put, (blk_hash,
                    blk_start_pos, blk_end_pos))
                asyncio.run_coroutine_threadsafe(coro(), self.loop)

            except Exception as e:
                logger.exception(e)

    def ack_for_loaded_blocks(self, batched_blocks: List[Tuple[bytes, int, int]]):
        if len(batched_blocks) == 0:
            return
        # Ack for all flushed blocks
        total_batch_size = 0
        for blk_hash, start_position, stop_position in self.batched_blocks:
            total_batch_size += stop_position-start_position
            # logger.debug(f"flushed raw block data for block hash={hash_to_hex_str(blk_hash)}, "
            #       f"size={stop_position-start_position} bytes")
            self.worker_ack_queue_blk_writer.put(blk_hash)
            # Fixme - need to get from this queue in the session manager side
            _discard_result = self.worker_ack_queue_blk_writer.get()
        if total_batch_size > 0:
            logger.debug(f"total batch size for raw blocks={round(total_batch_size/1024/1024, 3)} MB")
"""
Footnotes:
----------
footnote_1: this relies on the fact that if pg_parsed_rows_queue is blocked then 
worker_ack_queue_asyncio queue should also be blocked and so there should never be a 
situation where the async_timeout -> timeout after getting from one queue 
*but not the other*.
"""
