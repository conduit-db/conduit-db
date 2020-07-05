import logging

import asyncpg
from bitcoinx import hash_to_hex_str

async def pg_connect() -> asyncpg.Connection:
    conn = await asyncpg.connect(
        user="conduitadmin",
        host="127.0.0.1",
        port=5432,
        password="conduitpass",
        database="conduitdb",
    )
    return conn


async def pg_test_connect() -> asyncpg.Connection:
    conn = await asyncpg.connect(
        user="conduitadmin",
        host="127.0.0.1",
        port=5432,
        password="conduitpass",
        database="conduittestdb",
    )
    return conn


class PG_Database:
    """simple container for common postgres queries"""

    def __init__(self, pg_conn: asyncpg.Connection):
        self.pg_conn = pg_conn
        self.logger = logging.getLogger("pg_database")

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
                DROP TABLE IF EXISTS transactions;
                DROP TABLE IF EXISTS io_table;
                DROP TABLE IF EXISTS pushdata;"""
            )
        except asyncpg.exceptions.UndefinedTableError as e:
            self.logger.exception(e)

    async def pg_drop_temp_tables(self):
        try:
            await self.pg_conn.execute("""DROP TABLE temp_inputs;""")
        except asyncpg.exceptions.UndefinedTableError as e:
            self.logger.exception(e)

    async def pg_create_permanent_tables(self):
        await self.pg_conn.execute(
            """
                -- PERMANENT TABLES
                CREATE UNLOGGED TABLE IF NOT EXISTS transactions(
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
                """
        )

    async def pg_create_temp_tables(self):
        await self.pg_conn.execute(
            """
            CREATE TEMPORARY TABLE temp_inputs (
                in_prevout_shash bigint,
                out_idx integer,
                in_tx_shash bigint,
                in_idx integer,
                in_has_collided boolean
            );"""
        )

    async def pg_bulk_load_tx_rows(self, tx_rows):
        await self.pg_conn.copy_records_to_table(
            "transactions",
            columns=["tx_shash", "tx_hash", "tx_height", "tx_position", "tx_offset_start",
                "tx_offset_end", "tx_has_collided"],
            records=tx_rows,
        )

    async def pg_bulk_load_output_rows(self, out_rows):
        await self.pg_conn.copy_records_to_table(
            "io_table",
            columns=["out_tx_shash", "out_idx", "out_value", "out_has_collided", "in_tx_shash",
                "in_idx", "in_has_collided"],
            records=out_rows,
        )

    async def pg_bulk_load_input_rows(self, in_rows):
        # Todo - check for collisions in TxParser then:
        #  1) bulk copy to temp table
        #  2) update io table from temp table (no table joins needed)
        await self.pg_conn.copy_records_to_table(
            "temp_inputs",
            columns=["in_prevout_shash", "out_idx", "in_tx_shash", "in_idx",
                "in_has_collided"],
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

    async def pg_bulk_load_pushdata_rows(self, pd_rows):
        await self.pg_conn.copy_records_to_table(
            "pushdata",
            columns=["pushdata_shash", "pushdata_hash", "tx_shash", "idx", "ref_type", "pd_tx_has_collided"],
            records=pd_rows,  # exclude pushdata_hash there is no such column in the table
        )

    async def pg_bulk_insert_block_rows(self, tx_rows, out_rows, in_rows, pd_rows):
        await self.pg_create_temp_tables()

        await self.pg_bulk_load_tx_rows(tx_rows)  # todo - collision checks...
        await self.pg_bulk_load_output_rows(out_rows)
        await self.pg_bulk_load_input_rows(in_rows)
        await self.pg_bulk_load_pushdata_rows(pd_rows)

        # await self.pg_upsert_from_temp_txs()
        # await self.pg_upsert_from_temp_outputs()
        # await self.pg_upsert_from_temp_inputs()
        # rows = await pg_db.pg_conn.fetch("SELECT * FROM outputs;")
        # for row in rows:
        #     print(row)
        await self.pg_drop_temp_tables()

async def load_pg_database() -> PG_Database:
    pg_conn = await pg_connect()
    pg_database = PG_Database(pg_conn)
    await pg_database.pg_update_settings()
    await pg_database.pg_drop_tables()
    await pg_database.pg_create_permanent_tables()
    return PG_Database(pg_conn)


async def load_test_pg_database() -> PG_Database:
    pg_conn = await pg_test_connect()
    pg_database = PG_Database(pg_conn)
    await pg_database.pg_update_settings()
    await pg_database.pg_drop_tables()
    await pg_database.pg_create_permanent_tables()
    return PG_Database(pg_conn)
