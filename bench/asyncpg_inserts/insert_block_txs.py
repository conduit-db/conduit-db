import time

from asyncpg.prepared_stmt import PreparedStatement

from conduit.constants import REGTEST
from conduit.networks import NetworkConfig

try:
    from conduit._algorithms import preprocessor, parse_block  # cython
except ModuleNotFoundError:
    from conduit.algorithms import preprocessor, parse_block  # pure python
from bench.utils import print_results, print_results_asyncpg

from conduit.store import setup_storage

"""
- parse raw block
- establish asyncpg connection with asynchronous_commit and temp_buffer size increased
- take 1557 txs and copy to tx_temp table (should be fast) - with a tx_num column with all nulls
- then upsert to the transactions table and insert the resulting tx_nums into the txs_temp table
(which should have an index on the tx_hash column to make this faster) (but
don't drop this table until the next steps are done). This will be used
- then copy the inputs to a temp table
- then join this table with the inputs_temp table with the tx_hash and tx_num columns
- then upsert via the joined temp table
- then copy the outputs to outputs_temp table
- then join this table with the outputs_temp table with the tx_hash and tx_num columns
- then upsert via the joined temp table
- then drop all of the temp tables. DONE

- an extra option is to keep a **set** of pushdata_hashes and copy to a temp table
- then copy to pushdata_temp table (with column for pushdata_id as null)
- upsert to pushdata table and add the pushdata_id that is returned for each upsert row.
- then join the inputs_temp and outputs_temp tables with this pushdata_temp table to get the
pushdata_id for each row
table and outputs temp tables... the advantage is saving disc space for frequently re-used
pushdatas e.g. B protocol, BCAT etc. which probably amount.


so really a simplified strategy becomes...
create 4 x temp tables...
copy to txs_temp  (tx_hash, tx_num - nulls at first) + **index tx_hash!**
copy to pushdata_temp (pushdata_hash, pushdata_id - nulls at first) + **index pushdata_hash**
copy to inputs_temp  (standard parsed input rows)
copy to outputs_temp  (standard parsed output rows)

upsert transactions table and update txs_temp tx_num column for each row
upsert pushdata table and update pushdata_temp pushdata_id column for each row
join inputs_temp to txs_temp and pushdata_temp
upsert inputs table
join outputs_temp to txs_temp and pushdata_temp
upsert outputs table

drop all 4 temp tables
"""

"""
test db:

user="conduitadmin",
host="127.0.0.1",
port=5432,
password="conduitpass",
database="conduittestdb",

returned rows from block parsing:
- tx_rows:      [(tx_hash, height, position, offset)]
- in_rows:      [(tx_hash, prevout_hash, out_idx, pushdata_hash, in_idx)...]
- out_rows:     [(tx_hash, out_idx, pushdata_hash, value)...]

# tx_num is generated for all 3 tables

"""


import asyncio
import asyncpg


async def insert_tx_copy_method(conn: asyncpg.Connection, tx_rows):
    await conn.copy_records_to_table(
        "temp_txs",
        columns=["tx_hash", "height", "tx_position", "tx_offset"],
        records=tx_rows,
    )
    # await conn.execute("""CREATE UNIQUE INDEX tx_num_idx ON temp_txs (tx_num);""")


async def insert_input_copy_method(conn: asyncpg.Connection, in_rows):
    await conn.copy_records_to_table(
        "temp_inputs",
        columns=["tx_hash", "in_idx", "prevout_hash", "out_idx", "pushdata_hash"],
        records=in_rows,
    )
    # await conn.execute("""CREATE UNIQUE INDEX tx_num_idx ON temp_txs (tx_num);""")


# async def tx_upsert_from_temp_tx(stmt: PreparedStatement):
#     await stmt.fetchval()
async def upsert_from_temp_txs(conn: asyncpg.Connection):
     await conn.execute("""
        INSERT INTO transactions
        SELECT tx_hash, height, tx_position, tx_offset
        FROM temp_txs
        ON CONFLICT (tx_hash)
        DO UPDATE SET tx_hash=excluded.tx_hash, height=excluded.height, 
            tx_position=excluded.tx_position, tx_offset=excluded.tx_offset;
        
        UPDATE temp_txs
        SET tx_num = transactions.tx_num
        FROM transactions
        WHERE temp_txs.tx_hash = transactions.tx_hash;""")

async def upsert_from_temp_inputs(conn: asyncpg.Connection):
    await conn.execute("""
        WITH temp_inputs AS (
            SELECT temp_inputs.tx_hash, temp_inputs.in_idx, temp_inputs.prevout_hash, 
                temp_inputs.out_idx, temp_inputs.pushdata_hash, tx_num
            FROM temp_inputs 
            JOIN temp_txs 
            ON temp_inputs.tx_hash = temp_txs.tx_hash
        )
        
        -- conflicts should never happen
        INSERT INTO inputs 
        SELECT *
        FROM temp_inputs;""")

if __name__ == "__main__":

    async def main():
        with open("../data/block413567.raw", "rb") as f:
            raw_block = bytearray(f.read())

        t0 = time.time()
        tx_offsets = preprocessor(raw_block)
        tx_rows, in_rows, out_rows = parse_block(raw_block, tx_offsets, 413567)
        t1 = time.time() - t0
        print_results(len(tx_offsets), t1 / 1, raw_block)

        conn = await asyncpg.connect(
            user="conduitadmin",
            host="127.0.0.1",
            port=5432,
            password="conduitpass",
            database="conduittestdb",
        )
        await conn.execute("""
            DROP TABLE transactions;
            DROP TABLE inputs;""")

        await conn.execute(
            """
            UPDATE pg_settings
                SET setting = 'off'
                WHERE name='synchronous_commit';
            UPDATE pg_settings
                SET setting = 200
                WHERE name='temp_buffers'
            """
        )

        await conn.execute(
            """
            CREATE UNLOGGED TABLE transactions(
                tx_hash bytea PRIMARY KEY,
                height integer,
                tx_position integer,
                tx_offset bigint,
                tx_num integer generated always as identity
            );
            CREATE UNIQUE INDEX tx_num_idx ON transactions (tx_num);
            
            CREATE UNLOGGED TABLE inputs(
                tx_hash bytea,
                in_idx bigint,
                prevout_hash bytea,
                out_idx integer,
                pushdata_hash bytea,
                in_tx_num bigint
            );
            CREATE INDEX in_point_idx ON inputs (tx_hash, in_idx);

            CREATE TEMPORARY TABLE temp_txs(
                tx_hash bytea PRIMARY KEY,
                height integer,
                tx_position integer,
                tx_offset bigint,
                tx_num bigint
            );
            
            CREATE TEMPORARY TABLE temp_inputs(
                tx_hash bytea,
                in_idx bigint,
                prevout_hash bytea,
                out_idx integer,
                pushdata_hash bytea,
                in_tx_num bigint
            );
            CREATE INDEX in_point_idx ON temp_inputs (tx_hash, in_idx);
            """
        )

        t0 = time.time()
        await insert_tx_copy_method(conn, tx_rows)
        await insert_input_copy_method(conn, in_rows)
        await upsert_from_temp_txs(conn)
        await upsert_from_temp_inputs(conn)
        t1 = time.time() - t0
        print_results_asyncpg(len(tx_offsets), t1)

        # rows = await conn.fetch("SELECT * FROM inputs;")
        # for row in rows:
        #     print(row)

        # Close the connection.
        await conn.close()

    asyncio.get_event_loop().run_until_complete(main())
    store = setup_storage(NetworkConfig(REGTEST))
