import asyncpg
import logging
import time

from constants import PROFILING


class PostgresBulkLoads:

    def __init__(self, pg_conn: asyncpg.Connection):
        self.logger = logging.getLogger("pg-tables")
        self.pg_conn = pg_conn

    async def pg_bulk_load_confirmed_tx_rows(self, tx_rows):
        t0 = time.time()
        await self.pg_conn.copy_records_to_table(
            "confirmed_transactions",
            columns=[
                "tx_shash",
                "tx_hash",
                "tx_height",
                "tx_position",
                "tx_offset_start",
                "tx_offset_end",
                "tx_has_collided",
            ],
            records=tx_rows,
        )
        t1 = time.time() - t0
        self.logger.log(PROFILING,
            f"elapsed time for pg_bulk_load_confirmed_tx_rows = {t1} seconds for {len(tx_rows)}"
        )

    async def pg_bulk_load_mempool_tx_rows(self, tx_rows):
        t0 = time.time()
        await self.pg_conn.copy_records_to_table(
            "mempool_transactions",
            columns=[
                "mp_tx_shash",
                "mp_tx_hash",
                "mp_tx_timestamp",
                "mp_tx_has_collided",
                "mp_rawtx",
            ],
            records=tx_rows,
        )
        t1 = time.time() - t0
        self.logger.log(PROFILING,
            f"elapsed time for pg_bulk_load_mempool_tx_rows = {t1} seconds for {len(tx_rows)}"
        )

    async def pg_bulk_load_output_rows(self, out_rows):
        t0 = time.time()
        await self.pg_conn.copy_records_to_table(
            "io_table",
            columns=[
                "out_tx_shash",
                "out_idx",
                "out_value",
                "out_has_collided",
                "in_tx_shash",
                "in_idx",
                "in_has_collided",
            ],
            records=out_rows,
        )
        t1 = time.time() - t0
        self.logger.log(PROFILING,
            f"elapsed time for pg_bulk_load_output_rows = {t1} seconds for {len(out_rows)}"
        )

    async def pg_bulk_load_input_rows(self, in_rows):
        # Todo - check for collisions in TxParser then:
        #  1) bulk copy to temp table
        #  2) update io table from temp table (no table joins needed)
        t0 = time.time()
        await self.pg_conn.copy_records_to_table(
            "temp_inputs",
            columns=["in_prevout_shash", "out_idx", "in_tx_shash", "in_idx", "in_has_collided"],
            records=in_rows,
        )
        await self.pg_conn.execute(
            """
            UPDATE io_table 
            SET in_tx_shash = temp_inputs.in_tx_shash, 
                in_idx = temp_inputs.in_idx,
                in_has_collided = temp_inputs.in_has_collided
            FROM temp_inputs
            WHERE temp_inputs.in_prevout_shash = io_table.out_tx_shash 
            AND temp_inputs.out_idx = io_table.out_idx
            ;"""
        )
        t1 = time.time() - t0
        self.logger.log(PROFILING,
            f"elapsed time for pg_bulk_load_input_rows = {t1} seconds for {len(in_rows)}"
        )

    async def pg_bulk_load_pushdata_rows(self, pd_rows):
        t0 = time.time()
        await self.pg_conn.copy_records_to_table(
            "pushdata",
            columns=[
                "pushdata_shash",
                "pushdata_hash",
                "tx_shash",
                "idx",
                "ref_type",
                "pd_tx_has_collided",
            ],
            records=pd_rows,  # exclude pushdata_hash there is no such column in the table
        )

        t1 = time.time() - t0
        self.logger.log(PROFILING,
            f"elapsed time for pg_bulk_load_pushdata_rows = {t1} seconds for {len(pd_rows)}"
        )

