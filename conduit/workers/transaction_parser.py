import asyncio
import logging.handlers
import logging
import multiprocessing
import queue
import threading
import time
from datetime import datetime
from functools import partial
from multiprocessing import shared_memory
from typing import Sequence, Optional

import asyncpg

from conduit.constants import MsgType
from conduit.database.postgres_database import PG_Database, pg_connect
from conduit.logging_client import setup_tcp_logging

from .algorithms import calc_mtree_base_level, struct_le_q, parse_txs


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
        initial_block_download_event_mp,
    ):
        super(TxParser, self).__init__()
        self.shm = shared_memory.SharedMemory(shm_name, create=False)
        self.worker_in_queue_tx_parse = worker_in_queue_tx_parse
        self.worker_ack_queue_tx_parse = worker_ack_queue_tx_parse
        self.initial_block_download_event_mp = initial_block_download_event_mp

        self.worker_asyncio_tx_parser_confirmed_tx_queue = None
        self.worker_asyncio_tx_parser_mempool_tx_queue = None
        self.worker_asyncio_tx_parser_ack_queue = None
        self.processed_vs_unprocessed_queue = None

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
        setup_tcp_logging()
        rootLogger = logging.getLogger('')
        # formatting is done server side after unpickling
        socketHandler = logging.handlers.SocketHandler('127.0.0.1',
            logging.handlers.DEFAULT_TCP_LOGGING_PORT)
        rootLogger.addHandler(socketHandler)
        self.logger = logging.getLogger("transaction-parser")
        self.logger.setLevel(logging.DEBUG)

        self.loop = asyncio.get_event_loop()
        self.worker_asyncio_tx_parser_confirmed_tx_queue = asyncio.Queue()
        self.worker_asyncio_tx_parser_mempool_tx_queue = asyncio.Queue()
        self.worker_asyncio_tx_parser_ack_queue = asyncio.Queue()
        self.processed_vs_unprocessed_queue = queue.Queue()
        try:
            main_thread = threading.Thread(target=self.main_thread)
            main_thread.start()

            self.loop.run_until_complete(self.start_asyncio_jobs())
            self.logger.debug("TxParser event loop completed...")
        except Exception as e:
            self.logger.exception(e)
            raise

    async def start_asyncio_jobs(self):
        tasks = [
            asyncio.create_task(self.pg_insert_confirmed_tx_rows_task()),
            asyncio.create_task(self.pg_insert_mempool_tx_rows_task()),
        ]
        await asyncio.gather(*tasks)

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

    def handle_collision(self, tx_rows, in_rows, out_rows, set_pd_rows, blk_acks,
        colliding_tx_shash):
        """only expect to find 1 collision and then reattempt the batch
        (based on probabilities)"""
        try:
            colliding_index = None
            for index, tx_row in enumerate(tx_rows):
                if tx_row[0] == int(colliding_tx_shash):
                    self.logger.error(f"detected colliding tx_row: {tx_row}")
                    colliding_index = index
                    break
                else:
                    continue

            if colliding_index is not None and tx_row[3] == 0:  # position == 0 (i.e. coinbase)
                self.logger.error("coinbase colliding tx - ignore")
                _colliding_tx_row = tx_rows.pop(colliding_index)  # do nothing
                # todo... need to chase after the in_rows, out_rows and pd_rows
                return tx_rows
            elif colliding_index is not None and tx_row[3] != 0:  # this is a 'real' collision
                _colliding_tx_row = tx_rows.pop(colliding_index)
                self.logger.error("non-coinbase colliding tx - this needs to be handled...")
                return tx_rows
        except Exception as e:
            self.logger.exception(e)
            raise

    async def pg_flush_ins_outs_and_pushdata_rows(self, in_rows, out_rows, set_pd_rows):
        await self.pg_db.pg_create_temp_tables()
        t0 = time.time()
        await self.pg_db.pg_bulk_load_output_rows(out_rows)
        t1 = time.time() - t0
        self.logger.info(f"elapsed time for pg_bulk_load_output_rows = {t1} seconds for {len(out_rows)}")

        t0 = time.time()
        await self.pg_db.pg_bulk_load_input_rows(in_rows)
        t1 = time.time() - t0
        self.logger.info(f"elapsed time for pg_bulk_load_input_rows = {t1} seconds for {len(in_rows)}")

        t0 = time.time()
        await self.pg_db.pg_bulk_load_pushdata_rows(set_pd_rows)
        t1 = time.time() - t0
        self.logger.info(
            f"elapsed time for pg_bulk_load_pushdata_rows = {t1} seconds for {len(set_pd_rows)}"
        )
        await self.pg_db.pg_drop_temp_tables()

    async def pg_flush_rows(
        self,
        tx_rows: Sequence,
        in_rows: Sequence,
        out_rows: Sequence,
        set_pd_rows: Sequence,
        blk_acks: Optional[Sequence],
        confirmed: bool,
    ):
        try:
            if confirmed:
                await self.pg_db.pg_bulk_load_confirmed_tx_rows(tx_rows)
                await self.pg_flush_ins_outs_and_pushdata_rows(in_rows, out_rows, set_pd_rows)
                # Ack for all flushed blocks
                for blk_hash, tx_count in blk_acks:
                    # print(f"updated block hash={blk_hash}, with tx_count={tx_count}")
                    self.worker_ack_queue_tx_parse.put((blk_hash, tx_count))

                self.reset_batched_rows()
            else:
                self.logger.debug(f"confirmed = {confirmed}")
                await self.pg_db.pg_bulk_load_mempool_tx_rows(tx_rows)
                await self.pg_flush_ins_outs_and_pushdata_rows(in_rows, out_rows, set_pd_rows)

            await self.pg_flush_ins_outs_and_pushdata_rows(in_rows, out_rows, set_pd_rows)

        except asyncpg.exceptions.UniqueViolationError as e:
            error_text = e.as_dict()["detail"]
            # extract tx_shash from brackets - not best practice.. but most efficient
            colliding_tx_shash = error_text.split("(")[2].split(")")[0]
            self.logger.error(f"colliding tx_shash = {colliding_tx_shash}")
            tx_rows = self.handle_collision(
                tx_rows, in_rows, out_rows, set_pd_rows, blk_acks, colliding_tx_shash
            )
            # retry without the colliding tx
            await self.pg_flush_rows(tx_rows, in_rows, out_rows, set_pd_rows, blk_acks, confirmed)
            self.logger.error(e)

    async def pg_insert_confirmed_tx_rows_task(self):
        """bulk inserts to postgres. NOTE: Can only have ONE asyncio task pulling from
        worker_ack_queue_asyncio ."""

        self.pg_db: PG_Database = await pg_connect()
        await self.pg_db.pg_update_settings()
        try:
            while True:
                try:
                    # see 'footnote_1'
                    blk_rows = await asyncio.wait_for(
                        self.worker_asyncio_tx_parser_confirmed_tx_queue.get(),
                        timeout=self.BATCHED_MAX_WAIT_TIME,
                    )
                    blk_acks = await self.worker_asyncio_tx_parser_ack_queue.get()
                    await self.extend_batched_rows(blk_rows)
                    await self.append_block_acks(blk_acks)

                except asyncio.TimeoutError:
                    # print("timed out...")
                    if len(self.batched_tx_rows) != 0:
                        await self.pg_flush_rows(
                            self.batched_tx_rows,
                            self.batched_in_rows,
                            self.batched_out_rows,
                            self.batched_set_pd_rows,
                            self.batched_block_acks,
                            confirmed=True,
                        )
                    continue

                # This is not in the "try block" because I wasn't sure if it would be atomic
                # i.e. what if it times out half-way through committing rows to the db?
                if len(self.batched_tx_rows) > self.MAX_TX_BATCH_SIZE:
                    # print("hit max tx batch size...")
                    await self.pg_flush_rows(
                        self.batched_tx_rows,
                        self.batched_in_rows,
                        self.batched_out_rows,
                        self.batched_set_pd_rows,
                        self.batched_block_acks,
                        confirmed=True,
                    )
        except Exception as e:
            self.logger.exception(e)
            raise e

    async def pg_insert_mempool_tx_rows_task(self):
        """bulk inserts to postgres. NOTE: Can only have ONE asyncio task pulling from
        worker_ack_queue_asyncio ."""

        self.pg_db: PG_Database = await pg_connect()
        await self.pg_db.pg_update_settings()
        try:
            while True:
                mempool_rows = await self.worker_asyncio_tx_parser_mempool_tx_queue.get()
                await self.pg_flush_rows(
                    *mempool_rows, blk_acks=None, confirmed=False,
                )
        except Exception as e:
            self.logger.exception(e)
            raise e

    async def get_processed_vs_unprocessed_tx_offsets(self, raw_block, tx_offsets):
        """input rows, output rows and pushdata rows must not be inserted again if this has
        already occurred for the mempool transaction"""

        all_block_tx_hashes = calc_mtree_base_level(0, len(tx_offsets), {}, raw_block, tx_offsets)[
            0
        ]

        shashes = []
        for full_tx_hash in all_block_tx_hashes:
            shashes.append(struct_le_q.unpack(full_tx_hash[0:8])[0])

        offsets_map = dict(zip(shashes, tx_offsets))
        unprocessed_tx_shashes = await self.pg_db.pg_get_unprocessed_txs(shashes)

        relevant_offsets = []
        for tx_shash in unprocessed_tx_shashes:
            relevant_offsets.append(offsets_map.pop(tx_shash))
        self.processed_vs_unprocessed_queue.put_nowait((relevant_offsets, offsets_map))

    def run_coroutine_threadsafe(self, coro) -> asyncio.Future:
        return asyncio.run_coroutine_threadsafe(coro, self.loop)

    def main_thread(self):
        while True:
            try:
                msg_type, item = self.worker_in_queue_tx_parse.get()
                if not item:
                    return  # poison pill stop command

                if msg_type == MsgType.MSG_TX:
                    (tx_start_pos, tx_end_pos) = item
                    rawtx = bytes(self.shm.buf[tx_start_pos:tx_end_pos])

                    tx_rows, in_rows, out_rows, set_pd_rows, _tx_shashes = parse_txs(
                        buffer=rawtx,
                        tx_offsets=[0],
                        height_or_timestamp=datetime.now(),
                        confirmed=False,
                    )
                    coro = partial(
                        self.worker_asyncio_tx_parser_mempool_tx_queue.put,
                        (tx_rows, in_rows, out_rows, set_pd_rows),
                    )
                    self.run_coroutine_threadsafe(coro())

                if msg_type == MsgType.MSG_BLOCK:
                    (blk_hash, blk_height, blk_start_pos, blk_end_pos, tx_offsets_div,) = item
                    raw_block = bytes(self.shm.buf[blk_start_pos:blk_end_pos])

                    if not self.initial_block_download_event_mp.is_set:
                        self.run_coroutine_threadsafe(
                            self.get_processed_vs_unprocessed_tx_offsets(raw_block, tx_offsets_div)
                        )
                        unprocessed_tx_offsets, processed_tx_offsets = \
                            self.processed_vs_unprocessed_queue.get()
                    else:
                        unprocessed_tx_offsets = tx_offsets_div

                    tx_rows, in_rows, out_rows, set_pd_rows, tx_shashes = parse_txs(
                        buffer=raw_block,
                        tx_offsets=unprocessed_tx_offsets,
                        height_or_timestamp=blk_height,
                        confirmed=True,
                    )
                    coro = partial(
                        self.worker_asyncio_tx_parser_confirmed_tx_queue.put,
                        (tx_rows, in_rows, out_rows, set_pd_rows),
                    )
                    self.run_coroutine_threadsafe(coro())

                    # need to ack from asyncio event loop because that's where the context
                    # is for when the data has indeed been flushed to db
                    item = (blk_hash, len(tx_offsets_div))
                    coro2 = partial(self.worker_asyncio_tx_parser_ack_queue.put, item)
                    self.run_coroutine_threadsafe(coro2())

                # print(f"parsed rows: len(tx_rows)={len(tx_rows)}, len(in_rows)={len(in_rows)}, "
                #       f"len(out_rows)={len(out_rows)}")
            except Exception as e:
                self.logger.exception(e)
                raise

"""
Footnotes:
----------
footnote_1: this relies on the fact that if pg_parsed_rows_queue is blocked then 
worker_ack_queue_asyncio queue should also be blocked and so there should never be a 
situation where the async_timeout -> timeout after getting from one queue 
*but not the other*.
"""