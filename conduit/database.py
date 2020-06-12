import asyncpg

from .logs import logs

async def pg_connect() -> asyncpg.Connection:
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
        self.logger = logs.get_logger('pg_database')

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
                WHERE name='temp_buffers'
            """
        )

    async def pg_drop_tables(self):
        try:
            await self.pg_conn.execute(
                """
                DROP TABLE transactions;
                DROP TABLE inputs;
                DROP TABLE outputs;"""
            )
        except asyncpg.exceptions.UndefinedTableError as e:
            self.logger.exception(e)

    async def pg_drop_temp_tables(self):
        try:
            await self.pg_conn.execute(
                """
                DROP TABLE temp_txs;
                DROP TABLE temp_inputs;
                DROP TABLE temp_outputs;"""
            )
        except asyncpg.exceptions.UndefinedTableError as e:
            self.logger.exception(e)

    async def pg_create_permanent_tables(self):
        await self.pg_conn.execute(
            """
                -- PERMANENT TABLES
                CREATE UNLOGGED TABLE IF NOT EXISTS transactions(
                    tx_hash bytea,
                    height integer,
                    tx_position integer,
                    tx_offset bigint,
                    tx_num integer GENERATED ALWAYS AS IDENTITY PRIMARY KEY
                );
                CREATE UNIQUE INDEX IF NOT EXISTS tx_hash_idx ON transactions (tx_hash);

                CREATE UNLOGGED TABLE IF NOT EXISTS inputs(
                    in_prevout_tx_num bigint,
                    in_prevout_idx integer,
                    in_pushdata_hash bytea,
                    in_idx integer,
                    in_tx_num bigint
                );
                CREATE UNIQUE INDEX IF NOT EXISTS in_point_idx ON inputs (in_prevout_tx_num, 
                in_prevout_idx, in_pushdata_hash);

                CREATE UNLOGGED TABLE IF NOT EXISTS outputs(
                    out_tx_num bigint,
                    out_idx integer,
                    out_pushdata_hash bytea,
                    out_value bigint
                );
                CREATE UNIQUE INDEX IF NOT EXISTS out_point_idx ON outputs (out_tx_num, out_idx, 
                out_pushdata_hash);
                """
        )

    async def pg_create_temp_tables(self):
        await self.pg_conn.execute("""
            -- TEMP TABLES
            CREATE TEMPORARY TABLE temp_txs(
                tx_hash bytea PRIMARY KEY,
                height integer,
                tx_position integer,
                tx_offset bigint,
                tx_num bigint
            );

            CREATE TEMPORARY TABLE temp_inputs(
                in_prevout_hash bytea,
                in_prevout_idx integer,
                in_pushdata_hash bytea,
                in_idx bigint,
                in_tx_hash bytea,
                in_tx_num bigint  -- last because left blank initially
            );

            CREATE TEMPORARY TABLE temp_outputs(
                out_tx_hash bytea,
                out_idx integer,
                out_pushdata_hash bytea,
                out_value bigint
            );
        """)

    async def pg_insert_tx_copy_method(self, tx_rows):
        await self.pg_conn.copy_records_to_table(
            "temp_txs",
            columns=["tx_hash", "height", "tx_position", "tx_offset"],
            records=tx_rows,
        )  # await conn.execute("""CREATE UNIQUE INDEX tx_num_idx ON temp_txs (tx_num);""")

    async def pg_insert_input_copy_method(self, in_rows):
        # last column (in_tx_num) is left blank intentionally (filled with table join to temp_txs)
        await self.pg_conn.copy_records_to_table(
            "temp_inputs",
            columns=[
                "in_prevout_hash",
                "in_prevout_idx",
                "in_pushdata_hash",
                "in_idx",
                "in_tx_hash",
            ],
            records=in_rows,
        )
        await self.pg_conn.execute(
            """
            CREATE UNIQUE INDEX temp_in_point_idx 
            ON temp_inputs (in_prevout_hash, in_prevout_idx, in_pushdata_hash);"""
        )

    async def pg_insert_output_copy_method(self, out_rows):
        await self.pg_conn.copy_records_to_table(
            "temp_outputs",
            columns=["out_tx_hash", "out_idx", "out_pushdata_hash", "out_value"],
            records=out_rows,
        )
        await self.pg_conn.execute(
            """
            CREATE UNIQUE INDEX temp_out_point_idx 
            ON temp_outputs (out_tx_hash, out_idx, out_pushdata_hash);"""
        )

    async def pg_upsert_from_temp_txs(self):
        """upsert all txs - NOTE: this has a side effect of incrementing the tx_num but
           it's okay because if there are no tx entries then there will not be any
           inputs/outputs to break a link with OTOH if there ARE tx_entries the tx_num is
           not changed - the 'gap' in the integer index is a consmetic issue only..."""
        await self.pg_conn.execute(
            """
            INSERT INTO transactions 
            SELECT tx_hash, height, tx_position, tx_offset
            FROM temp_txs
            ON CONFLICT (tx_hash)
            DO UPDATE SET tx_hash=excluded.tx_hash, height=excluded.height, 
                tx_position=excluded.tx_position, tx_offset=excluded.tx_offset;

            -- needed to convert tx_hash -> tx_num for input and output rows and save space
            UPDATE temp_txs
            SET tx_num = transactions.tx_num
            FROM transactions
            WHERE temp_txs.tx_hash = transactions.tx_hash;
            """
        )

    async def pg_upsert_from_temp_inputs(self):
        await self.pg_conn.execute(
            """
            WITH add_utxo_tx_num AS (
                -- STEP 1 is essentially finding the tx_num for the utxos that were just 
                --  spent... (so maybe caching all (or some) utxos could improve performance...)
                SELECT temp_inputs.in_prevout_hash, tx_num as in_prevout_tx_num, 
                    temp_inputs.in_prevout_idx, temp_inputs.in_pushdata_hash, 
                    temp_inputs.in_idx, temp_inputs.in_tx_hash, temp_inputs.in_tx_num
                FROM temp_inputs 
                JOIN transactions 
                ON temp_inputs.in_prevout_hash = transactions.tx_hash
            ),
            add_in_tx_num AS (
                -- STEP 2 is essentially finding the tx_num for the in_tx_hash
                SELECT add_utxo_tx_num.in_prevout_hash, add_utxo_tx_num.in_prevout_tx_num, 
                    add_utxo_tx_num.in_prevout_idx, add_utxo_tx_num.in_pushdata_hash, 
                    add_utxo_tx_num.in_idx, add_utxo_tx_num.in_tx_hash, tx_num as in_tx_num
                FROM add_utxo_tx_num 
                JOIN temp_txs
                ON add_utxo_tx_num.in_prevout_hash = temp_txs.tx_hash
            )
            INSERT INTO inputs
            SELECT full_inputs.in_prevout_tx_num, full_inputs.in_prevout_idx, full_inputs.in_pushdata_hash, full_inputs.in_idx, full_inputs.in_tx_num
            FROM add_in_tx_num AS full_inputs;"""
        )

    async def pg_upsert_from_temp_outputs(self):
        await self.pg_conn.execute(
            """
            WITH tmp_outs_w_num AS (
                SELECT temp_txs.tx_num as out_tx_num, temp_outputs.out_idx, 
                    temp_outputs.out_pushdata_hash, temp_outputs.out_value
                FROM temp_outputs 
                JOIN temp_txs 
                ON temp_outputs.out_tx_hash = temp_txs.tx_hash
            )
            -- select * from tmp_outs_w_num
            INSERT INTO outputs
            SELECT full_outputs.out_tx_num, full_outputs.out_idx, full_outputs.out_pushdata_hash, full_outputs.out_value
            FROM tmp_outs_w_num AS full_outputs;
        """
        )

    async def pg_process_parsed_block_metadata(self, tx_rows, out_rows, in_rows):
        await self.pg_create_temp_tables()

        await self.pg_insert_tx_copy_method(tx_rows)
        await self.pg_insert_output_copy_method(out_rows)
        await self.pg_insert_input_copy_method(in_rows)

        await self.pg_upsert_from_temp_txs()
        await self.pg_upsert_from_temp_outputs()
        await self.pg_upsert_from_temp_inputs()
        # rows = await pg_db.pg_conn.fetch("SELECT * FROM outputs;")
        # for row in rows:
        #     print(row)
        await self.pg_drop_temp_tables()

async def load_pg_database() -> PG_Database:
    pg_conn = await pg_connect()
    pg_database = PG_Database(pg_conn)
    await pg_database.pg_update_settings()
    # await pg_database.pg_drop_tables()
    await pg_database.pg_create_permanent_tables()
    return PG_Database(pg_conn)
