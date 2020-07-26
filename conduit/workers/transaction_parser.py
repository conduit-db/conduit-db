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
    ack_confirmed: blk_hash, count_txs_done
    ack_mempool: tx_counts
    """

    def __init__(
        self,
        shm_name,
        worker_in_queue_tx_parse,
        worker_ack_queue_tx_parse_confirmed,
        initial_block_download_event_mp,
    ):
        super(TxParser, self).__init__()
        self.shm = shared_memory.SharedMemory(shm_name, create=False)
        self.worker_in_queue_tx_parse = worker_in_queue_tx_parse
        self.worker_ack_queue_tx_parse_confirmed = worker_ack_queue_tx_parse_confirmed
        # self.worker_ack_queue_tx_parse_mempool = worker_ack_queue_tx_parse_mempool
        self.initial_block_download_event_mp = initial_block_download_event_mp

        self.worker_asyncio_tx_parser_confirmed_tx_queue = None
        self.worker_asyncio_tx_parser_mempool_tx_queue = None
        self.worker_asyncio_tx_parser_ack_queue = None
        self.processed_vs_unprocessed_queue = None

        self.pg_db = None
        self.loop = None

        # batched confirmed rows
        self.confirmed_batched_tx_rows = []
        self.confirmed_batched_in_rows = []
        self.confirmed_batched_out_rows = []
        self.confirmed_batched_set_pd_rows = []
        self.confirmed_batched_block_acks = []

        # batched rows
        self.mempool_batched_tx_rows = []
        self.mempool_batched_in_rows = []
        self.mempool_batched_out_rows = []
        self.mempool_batched_set_pd_rows = []
        self.mempool_batched_mempool_tx_acks = []

        # accumulate x seconds worth of txs or MAX_TX_BATCH_SIZE (whichever comes first)
        self.CONFIRMED_MAX_TX_BATCH_SIZE = 50000
        self.CONFIRMED_BATCHED_MAX_WAIT_TIME = 0.3
        self.MEMPOOL_MAX_TX_BATCH_SIZE = 2000
        self.MEMPOOL_BATCHED_MAX_WAIT_TIME = 0.1

    def run(self):
        setup_tcp_logging()
        self.logger = logging.getLogger("transaction-parser")
        self.logger.setLevel(logging.DEBUG)

        self.loop = asyncio.get_event_loop()
        self.worker_asyncio_tx_parser_confirmed_tx_queue = asyncio.Queue()
        self.worker_asyncio_tx_parser_mempool_tx_queue = asyncio.Queue()
        self.worker_asyncio_tx_parser_ack_queue_confirmed = asyncio.Queue()
        self.worker_asyncio_tx_parser_ack_queue_mempool = asyncio.Queue()
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

    # ----- CONFIRMED TXS ----- #

    def reset_batched_rows_confirmed(self):
        self.confirmed_batched_tx_rows = []
        self.confirmed_batched_in_rows = []
        self.confirmed_batched_out_rows = []
        self.confirmed_batched_set_pd_rows = []
        self.confirmed_batched_block_acks = []

    async def extend_batched_rows_confirmed(self, blk_rows):
        tx_rows, in_rows, out_rows, set_pd_rows = blk_rows
        self.confirmed_batched_tx_rows.extend(tx_rows)
        self.confirmed_batched_in_rows.extend(in_rows)
        self.confirmed_batched_out_rows.extend(out_rows)
        self.confirmed_batched_set_pd_rows.extend(set_pd_rows)

    async def append_acks_confirmed_txs(self, blk_acks):
        self.confirmed_batched_block_acks.append(blk_acks)

    # ----- MEMPOOL TXS ----- #

    def reset_batched_rows_mempool(self):
        self.mempool_batched_tx_rows = []
        self.mempool_batched_in_rows = []
        self.mempool_batched_out_rows = []
        self.mempool_batched_set_pd_rows = []
        self.confirmed_batched_block_acks = []

    async def extend_batched_rows_mempool(self, mempool_rows):
        tx_rows, in_rows, out_rows, set_pd_rows = mempool_rows
        self.mempool_batched_tx_rows.extend(tx_rows)
        self.mempool_batched_in_rows.extend(in_rows)
        self.mempool_batched_out_rows.extend(out_rows)
        self.mempool_batched_set_pd_rows.extend(set_pd_rows)

    async def append_acks_mempool_txs(self, mempool_tx_acks):
        self.mempool_batched_mempool_tx_acks.append(mempool_tx_acks)

    # ------------------------------------------------- #

    def handle_collision(
        self, tx_rows, in_rows, out_rows, set_pd_rows, blk_acks, colliding_tx_shash
    ):
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
        self.logger.info(
            f"elapsed time for pg_bulk_load_output_rows = {t1} seconds for {len(out_rows)}"
        )

        t0 = time.time()
        await self.pg_db.pg_bulk_load_input_rows(in_rows)
        t1 = time.time() - t0
        self.logger.info(
            f"elapsed time for pg_bulk_load_input_rows = {t1} seconds for {len(in_rows)}"
        )

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
        acks: Optional[Sequence],
        confirmed: bool,
    ):
        try:
            if confirmed:
                await self.pg_db.pg_bulk_load_confirmed_tx_rows(tx_rows)
                await self.pg_flush_ins_outs_and_pushdata_rows(in_rows, out_rows, set_pd_rows)
                # Ack for all flushed blocks
                for blk_hash, tx_count in acks:
                    # print(f"updated block hash={blk_hash}, with tx_count={tx_count}")
                    self.worker_ack_queue_tx_parse_confirmed.put((blk_hash, tx_count))

                self.reset_batched_rows_confirmed()

            else:
                await self.pg_db.pg_bulk_load_mempool_tx_rows(tx_rows)
                await self.pg_flush_ins_outs_and_pushdata_rows(in_rows, out_rows, set_pd_rows)
                # for tx_count in acks:
                # self.worker_ack_queue_tx_parse_mempool.put((tx_count))

                self.reset_batched_rows_mempool()

        except asyncpg.exceptions.UniqueViolationError as e:
            error_text = e.as_dict()["detail"]
            # extract tx_shash from brackets - not best practice.. but most efficient
            colliding_tx_shash = error_text.split("(")[2].split(")")[0]
            self.logger.error(f"colliding tx_shash = {colliding_tx_shash}")
            tx_rows = self.handle_collision(
                tx_rows, in_rows, out_rows, set_pd_rows, acks, colliding_tx_shash
            )
            # retry without the colliding tx
            await self.pg_flush_rows(tx_rows, in_rows, out_rows, set_pd_rows, acks, confirmed)
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
                    confirmed_rows = await asyncio.wait_for(
                        self.worker_asyncio_tx_parser_confirmed_tx_queue.get(),
                        timeout=self.CONFIRMED_BATCHED_MAX_WAIT_TIME,
                    )
                    block_acks = await self.worker_asyncio_tx_parser_ack_queue_confirmed.get()
                    await self.extend_batched_rows_confirmed(confirmed_rows)
                    await self.append_acks_confirmed_txs(block_acks)

                except asyncio.TimeoutError:
                    # print("timed out...")
                    if len(self.confirmed_batched_tx_rows) != 0:
                        await self.pg_flush_rows(
                            self.confirmed_batched_tx_rows,
                            self.confirmed_batched_in_rows,
                            self.confirmed_batched_out_rows,
                            self.confirmed_batched_set_pd_rows,
                            self.confirmed_batched_block_acks,
                            confirmed=True,
                        )
                    continue

                # This is not in the "try block" because I wasn't sure if it would be atomic
                # i.e. what if it times out half-way through committing rows to the db?
                if len(self.confirmed_batched_tx_rows) > self.CONFIRMED_MAX_TX_BATCH_SIZE:
                    # print("hit max tx batch size...")
                    await self.pg_flush_rows(
                        self.confirmed_batched_tx_rows,
                        self.confirmed_batched_in_rows,
                        self.confirmed_batched_out_rows,
                        self.confirmed_batched_set_pd_rows,
                        self.confirmed_batched_block_acks,
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
                try:
                    mempool_rows = await asyncio.wait_for(
                        self.worker_asyncio_tx_parser_mempool_tx_queue.get(),
                        timeout=self.MEMPOOL_BATCHED_MAX_WAIT_TIME,
                    )
                    mempool_tx_acks = await self.worker_asyncio_tx_parser_ack_queue_mempool.get()
                    await self.extend_batched_rows_mempool(mempool_rows)
                    await self.append_acks_mempool_txs(mempool_tx_acks)
                except asyncio.TimeoutError:
                    # self.logger.debug("mempool batch timer triggered")
                    if len(self.mempool_batched_tx_rows) != 0:
                        await self.pg_flush_rows(
                            self.mempool_batched_tx_rows,
                            self.mempool_batched_in_rows,
                            self.mempool_batched_out_rows,
                            self.mempool_batched_set_pd_rows,
                            self.mempool_batched_mempool_tx_acks,
                            confirmed=False,
                        )
                    continue

                # This is not in the "try block" because I wasn't sure if it would be atomic
                # i.e. what if it times out half-way through committing rows to the db?
                if len(self.mempool_batched_tx_rows) > self.MEMPOOL_MAX_TX_BATCH_SIZE - 1:
                    self.logger.debug(
                        f"hit max mempool batch size ({len(self.mempool_batched_tx_rows)})"
                    )
                    await self.pg_flush_rows(
                        self.mempool_batched_tx_rows,
                        self.mempool_batched_in_rows,
                        self.mempool_batched_out_rows,
                        self.mempool_batched_set_pd_rows,
                        self.mempool_batched_mempool_tx_acks,
                        confirmed=False,
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
                    tx_offsets = item

                    # Todo only does 1 mempool tx at a time at present
                    for tx_start_pos, tx_end_pos in tx_offsets:
                        buffer = bytes(self.shm.buf[tx_start_pos:tx_end_pos])

                    result = parse_txs(buffer, [0], datetime.now(), False)
                    tx_rows, in_rows, out_rows, set_pd_rows, _tx_shashes = result

                    coro = partial(
                        self.worker_asyncio_tx_parser_mempool_tx_queue.put,
                        (tx_rows, in_rows, out_rows, set_pd_rows),
                    )
                    self.run_coroutine_threadsafe(coro())

                    # need to ack from asyncio event loop because that's where the context
                    # is for when the data has indeed been flushed to db
                    # todo - batch up mempool txs before feeding them into the queue. currently
                    #  len(item) will always == 1 tx so there is no point in feeding this
                    item = len(item)
                    coro2 = partial(self.worker_asyncio_tx_parser_ack_queue_mempool.put, item)
                    self.run_coroutine_threadsafe(coro2())

                if msg_type == MsgType.MSG_BLOCK:
                    blk_hash, blk_height, blk_start_pos, blk_end_pos, tx_offsets_div = item
                    buffer = bytes(self.shm.buf[blk_start_pos:blk_end_pos])

                    if not self.initial_block_download_event_mp.is_set:
                        self.run_coroutine_threadsafe(
                            self.get_processed_vs_unprocessed_tx_offsets(buffer, tx_offsets_div)
                        )
                        item = self.processed_vs_unprocessed_queue.get()
                        unprocessed_tx_offsets, processed_tx_offsets = item
                    else:
                        unprocessed_tx_offsets = tx_offsets_div

                    result = parse_txs(buffer, unprocessed_tx_offsets, blk_height, True)
                    tx_rows, in_rows, out_rows, set_pd_rows, tx_shashes = result
                    coro = partial(
                        self.worker_asyncio_tx_parser_confirmed_tx_queue.put,
                        (tx_rows, in_rows, out_rows, set_pd_rows),
                    )
                    self.run_coroutine_threadsafe(coro())

                    # need to ack from asyncio event loop because that's where the context
                    # is for when the data has indeed been flushed to db
                    item = (blk_hash, len(tx_offsets_div))
                    coro2 = partial(self.worker_asyncio_tx_parser_ack_queue_confirmed.put, item)
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
