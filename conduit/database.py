import asyncpg
from bitcoinx import hash_to_hex_str

from .logs import logs


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
        self.logger = logs.get_logger("pg_database")

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
                    tx_num bigint PRIMARY KEY,
                    tx_hash bytea,
                    height integer,
                    tx_position bigint,
                    tx_offset bigint
                );
                CREATE INDEX IF NOT EXISTS tx_hash_idx ON transactions (tx_hash);

                CREATE UNLOGGED TABLE IF NOT EXISTS io_table (
                    out_tx_num bigint,
                    out_idx integer,
                    out_value bigint,
                    in_tx_num integer,
                    in_idx integer
                );
                CREATE UNIQUE INDEX IF NOT EXISTS io_idx ON io_table (out_tx_num, out_idx);

                CREATE UNLOGGED TABLE IF NOT EXISTS pushdata (
                    tx_num bigint,
                    idx integer,
                    pushdata_hash bytea,
                    reftype smallint
                    -- pd_id bigint GENERATED ALWAYS AS IDENTITY
                );
                -- NOTE - parsing stage ensures there are no duplicates otherwise would need
                -- to do UPSERT which is slow...
                CREATE INDEX IF NOT EXISTS pushdata_multi_idx ON pushdata (pushdata_hash);
                """
        )

    async def pg_create_temp_tables(self):
        await self.pg_conn.execute(
            """
            CREATE TEMPORARY TABLE temp_inputs (
                prevout_hash bytea,
                out_idx integer,
                in_tx_num bigint,
                in_idx integer
            );"""
        )

    async def pg_bulk_load_tx_rows(self, tx_rows):
        await self.pg_conn.copy_records_to_table(
            "transactions",
            columns=["tx_num", "tx_hash", "height", "tx_position", "tx_offset"],
            records=tx_rows,
        )

    async def pg_bulk_load_output_rows(self, out_rows):
        await self.pg_conn.copy_records_to_table(
            "io_table",
            columns=["out_tx_num", "out_idx", "out_value", "in_tx_num", "in_idx"],
            records=out_rows,
        )

    async def pg_bulk_load_input_rows(self, in_rows):
        await self.pg_conn.copy_records_to_table(
            "temp_inputs",
            columns=["prevout_hash", "out_idx", "in_tx_num", "in_idx"],
            records=in_rows,
        )
        await self.pg_conn.execute(
            """
            -- get the tx_num for prevout_hash
            WITH full_io_rows AS (
                SELECT transactions.tx_num as out_tx_num, 
                       temp_inputs.out_idx, 
                       temp_inputs.in_tx_num, 
                       temp_inputs.in_idx
                FROM temp_inputs
                JOIN transactions 
                ON temp_inputs.prevout_hash = transactions.tx_hash
            )
            
            UPDATE io_table 
            SET in_tx_num = full_io_rows.in_tx_num, 
                in_idx = full_io_rows.in_idx
            FROM full_io_rows
            WHERE full_io_rows.out_tx_num = io_table.out_tx_num AND full_io_rows.out_idx = io_table.out_idx
            ;"""
        )

    async def pg_bulk_load_pushdata_rows(self, pd_rows):
        await self.pg_conn.copy_records_to_table(
            "pushdata",
            columns=["tx_num", "idx", "pushdata_hash", "reftype"],
            records=pd_rows,
        )

    async def pg_bulk_insert_block_rows(self, tx_rows, out_rows, in_rows):
        await self.pg_create_temp_tables()

        await self.pg_bulk_load_tx_rows(tx_rows)
        await self.pg_bulk_load_output_rows(out_rows)
        await self.pg_insert_input_copy_method(in_rows)

        await self.pg_upsert_from_temp_txs()
        await self.pg_upsert_from_temp_outputs()
        await self.pg_upsert_from_temp_inputs()
        # rows = await pg_db.pg_conn.fetch("SELECT * FROM outputs;")
        # for row in rows:
        #     print(row)
        await self.pg_drop_temp_tables()

    async def get_last_tx_num(self):
        max_tx_num = await self.pg_conn.fetchval("""
            SELECT tx_num 
            FROM transactions 
            ORDER BY tx_num DESC 
            LIMIT 1
        """)
        return max_tx_num


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
    # await pg_database.pg_drop_tables()
    await pg_database.pg_create_permanent_tables()
    return PG_Database(pg_conn)
