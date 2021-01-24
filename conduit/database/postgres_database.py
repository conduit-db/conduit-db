import logging
import time
from typing import List

import asyncpg
from asyncpg import Record

from constants import PROFILING


class PG_Database:
    """simple container for common postgres queries"""

    def __init__(self, pg_conn: asyncpg.Connection):
        self.pg_conn = pg_conn
        self.logger = logging.getLogger("pg-database")
        self.logger.setLevel(logging.DEBUG)
        # self.logger.setLevel(PROFILING)
        # self.logger.debug("initialized PG_Database")

    async def close(self):
        await self.pg_conn.close()

    async def pg_update_settings(self):
        await self.pg_conn.execute(
            """
            UPDATE pg_settings
                SET setting = 'off'
                WHERE name='synchronous_commit';
            UPDATE pg_settings
                SET setting = 200
                WHERE name='temp_buffers';
            """
        )

    async def pg_drop_tables(self):
        try:
            await self.pg_conn.execute(
                """
                DROP TABLE IF EXISTS confirmed_transactions;
                DROP TABLE IF EXISTS mempool_transactions;
                DROP TABLE IF EXISTS io_table;
                DROP TABLE IF EXISTS pushdata;
                DROP TABLE IF EXISTS api_state"""
            )
        except asyncpg.exceptions.UndefinedTableError as e:
            self.logger.exception(e)

    async def pg_drop_temp_inputs(self):
        try:
            await self.pg_conn.execute("""
            DROP TABLE temp_inputs;
            """)
        except asyncpg.exceptions.UndefinedTableError as e:
            self.logger.exception(e)

    async def pg_drop_temp_mined_tx_shashes(self):
        try:
            await self.pg_conn.execute("""
            DROP TABLE IF EXISTS temp_mined_tx_shashes;
            """)
        except asyncpg.exceptions.UndefinedTableError as e:
            self.logger.exception(e)

    async def pg_create_permanent_tables(self):
        await self.pg_conn.execute(
            """
                -- PERMANENT TABLES
                CREATE UNLOGGED TABLE IF NOT EXISTS confirmed_transactions (
                    tx_shash bigint PRIMARY KEY,
                    tx_hash bytea,
                    tx_height integer,
                    tx_position bigint,
                    tx_offset_start bigint,
                    tx_offset_end bigint,
                    tx_has_collided boolean
                );
                -- need to store the full tx_hash (albeit non-indexed) because the client
                -- may not be providing the tx_hash in their query (e.g. for key history).

                CREATE UNLOGGED TABLE IF NOT EXISTS io_table (
                    out_tx_shash bigint,
                    out_idx integer,
                    out_value bigint,
                    out_has_collided boolean,
                    in_tx_shash bigint,
                    in_idx integer,
                    in_has_collided boolean
                );
                CREATE INDEX IF NOT EXISTS io_idx ON io_table (out_tx_shash, out_idx);

                -- I think I can get away with not storing full pushdata hashes
                -- unless they collide... because the client provides the full pushdata_hash
                CREATE UNLOGGED TABLE IF NOT EXISTS pushdata (
                    pushdata_shash bigint,
                    pushdata_hash bytea,
                    tx_shash bigint,
                    idx integer,
                    ref_type smallint,
                    pd_tx_has_collided boolean
                );
                -- NOTE - parsing stage ensures there are no duplicates otherwise would need
                -- to do UPSERT which is slow...
                -- dropped the tx_shash index and can instead do range scans (for a given
                -- pushdata_hash / key history) at lookup time...
                -- Things like B:// maybe could be dealt with as special cases perhaps?
                CREATE INDEX IF NOT EXISTS pushdata_multi_idx ON pushdata (
                pushdata_shash);
                
                -- ?? should this be an in-memory only table?
                CREATE UNLOGGED TABLE IF NOT EXISTS mempool_transactions (
                    mp_tx_shash bigint PRIMARY KEY,
                    mp_tx_hash bytea,
                    mp_tx_timestamp timestamptz,
                    mp_tx_has_collided boolean,
                    mp_rawtx bytea
                );
                -- need to store the full tx_hash (albeit non-indexed) because the client
                -- may not be providing the tx_hash in their query (e.g. for key history).
                
                CREATE UNLOGGED TABLE IF NOT EXISTS api_state (
                    id integer,
                    api_tip_height integer,
                    api_tip_hash bytea,
                    PRIMARY KEY (id)
                );
                
                """
        )

    async def pg_create_temp_inputs_table(self):
        # Todo - prefix the table name with the worker_id to avoid clashes
        #  currently there is only a single worker
        await self.pg_conn.execute(
            """           
            CREATE TEMPORARY TABLE IF NOT EXISTS temp_inputs (
                in_prevout_shash bigint,
                out_idx integer,
                in_tx_shash bigint,
                in_idx integer,
                in_has_collided boolean
            );
            """
        )

    async def pg_create_temp_mined_tx_shashes_table(self):
        # Todo - prefix the table name with the worker_id to avoid clashes
        #  currently there is only a single worker
        await self.pg_conn.execute("""
            CREATE TEMPORARY TABLE IF NOT EXISTS temp_mined_tx_shashes (
                mined_tx_shash bigint
            );
            """)

    async def pg_load_temp_mined_tx_shashes(self, mined_tx_shashes):
        await self.pg_create_temp_mined_tx_shashes_table()
        await self.pg_conn.copy_records_to_table(
            "temp_mined_tx_shashes", columns=["mined_tx_shash"], records=mined_tx_shashes,
        )

    async def pg_get_unprocessed_txs(self, mined_tx_shashes: List[int]):
        """
        NOTE: usually (if all mempool txs have been processed, this function will only return
        the coinbase tx)

        checking for colliding short hashes is not required -
        - see blueprint section: very fine print
        """
        result: List[Record] = await self.pg_conn.fetch(
            """
            -- tx_hash ISNULL implies no match therefore not previously processed yet
            SELECT *
            FROM temp_mined_tx_shashes
            LEFT OUTER JOIN mempool_transactions
            ON (mempool_transactions.mp_tx_shash = temp_mined_tx_shashes.mined_tx_shash)
            WHERE mempool_transactions.mp_tx_hash ISNULL
            ;"""
        )
        return result

    async def get_temp_mined_tx_shashes(self):
        result: List[Record] = await self.pg_conn.fetch(
            """
            SELECT *
            FROM temp_mined_tx_shashes;"""
        )
        self.logger.debug(f"get_temp_mined_tx_shashes: {result}")

    async def pg_invalidate_mempool_rows(self):
        """Need to deal with collisions here"""
        result = await self.pg_conn.execute(
            """
            DELETE FROM mempool_transactions
            WHERE mp_tx_shash in 
            (SELECT mined_tx_shash FROM temp_mined_tx_shashes)
            """
        )
        deleted_count = result.split(" ")[1]
        self.logger.debug(f"deleted {deleted_count} mempool rows (now included in a block)")

    async def pg_update_api_tip_height_and_hash(self, api_tip_height: int, api_tip_hash: bytes):
        assert isinstance(api_tip_height, int)
        assert isinstance(api_tip_hash, bytes)
        await self.pg_conn.execute(
            """
            INSERT INTO api_state(id, api_tip_height, api_tip_hash) 
            VALUES(0, $1, $2)
            ON CONFLICT (id)
            DO UPDATE SET id=0, api_tip_height = $1, api_tip_hash=$2;
            """, api_tip_height, api_tip_hash
        )

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


async def pg_connect() -> PG_Database:
    conn = await asyncpg.connect(
        user="conduitadmin",
        host="127.0.0.1",
        port=5432,
        password="conduitpass",
        database="conduitdb",
    )
    return PG_Database(conn)


async def pg_test_connect() -> PG_Database:
    conn = await asyncpg.connect(
        user="conduitadmin",
        host="127.0.0.1",
        port=5432,
        password="conduitpass",
        database="conduittestdb",
    )
    return PG_Database(conn)


async def load_pg_database() -> PG_Database:
    pg_database = await pg_connect()
    await pg_database.pg_update_settings()
    await pg_database.pg_create_permanent_tables()
    return pg_database


async def load_test_pg_database() -> PG_Database:
    pg_database = await pg_test_connect()
    await pg_database.pg_update_settings()
    await pg_database.pg_create_permanent_tables()
    return pg_database
