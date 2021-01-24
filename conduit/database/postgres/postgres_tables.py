import asyncpg
import logging


class PostgresTables:

    def __init__(self, pg_conn: asyncpg.Connection):
        self.logger = logging.getLogger("pg-tables")
        self.pg_conn = pg_conn

    async def pg_drop_tables(self):
        try:
            await self.pg_conn.execute("""
                DROP TABLE IF EXISTS confirmed_transactions;
                DROP TABLE IF EXISTS mempool_transactions;
                DROP TABLE IF EXISTS io_table;
                DROP TABLE IF EXISTS pushdata;
                DROP TABLE IF EXISTS api_state""")
        except asyncpg.exceptions.UndefinedTableError as e:
            self.logger.exception(e)

    async def pg_drop_temp_inputs(self):
        try:
            await self.pg_conn.execute("""
                DROP TABLE temp_inputs;
            """)
        except asyncpg.exceptions.UndefinedTableError as e:
            self.logger.exception(e)

    async def pg_drop_temp_mined_tx_hashes(self):
        try:
            await self.pg_conn.execute("""
            DROP TABLE IF EXISTS temp_mined_tx_hashes;
            """)
        except asyncpg.exceptions.UndefinedTableError as e:
            self.logger.exception(e)

    async def pg_drop_temp_inbound_tx_hashes(self):
        try:
            await self.pg_conn.execute("""
            DROP TABLE IF EXISTS temp_inbound_tx_hashes;
            """)
        except asyncpg.exceptions.UndefinedTableError as e:
            self.logger.exception(e)

    async def pg_create_permanent_tables(self):
        await self.pg_conn.execute("""
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

                """)

    async def pg_create_temp_inputs_table(self):
        # Todo - prefix the table name with the worker_id to avoid clashes
        #  currently there is only a single worker
        await self.pg_conn.execute("""           
            CREATE TEMPORARY TABLE IF NOT EXISTS temp_inputs (
                in_prevout_shash bigint,
                out_idx integer,
                in_tx_shash bigint,
                in_idx integer,
                in_has_collided boolean
            );
            """)

    async def pg_create_temp_mined_tx_hashes_table(self):
        await self.pg_conn.execute("""
            CREATE TEMPORARY TABLE IF NOT EXISTS temp_mined_tx_hashes (
                mined_tx_hash bytea,
                blk_height bigint
            );
            """)

    async def pg_create_temp_inbound_tx_hashes_table(self):
        await self.pg_conn.execute("""
            CREATE TEMPORARY TABLE IF NOT EXISTS temp_inbound_tx_hashes (
                inbound_tx_hashes bytea
            );
            """)
