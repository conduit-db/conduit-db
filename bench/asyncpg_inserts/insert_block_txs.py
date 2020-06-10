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

"""

import asyncio
import asyncpg


async def tx_insert_naive(conn, tx_rows):
    for tx_row in tx_rows:
        await conn.execute(
            """INSERT INTO transactions VALUES($1, $2, $3, $4)""", *tx_row
        )


async def tx_insert_copy_method(conn: asyncpg.Connection, tx_rows):
    await conn.copy_records_to_table(
        "temp_txs",
        columns=("tx_hash", "height", "tx_position", "tx_offset"),
        records=tx_rows,
    )
    # await conn.execute("""CREATE UNIQUE INDEX tx_num_idx ON temp_txs (tx_num);""")


async def tx_upsert_from_temp_tx(stmt: PreparedStatement):
    await stmt.fetchval()


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
        await conn.execute("""DROP TABLE transactions;""")

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
                tx_num bigint generated always as identity
            );
            CREATE TEMPORARY TABLE temp_txs(
                tx_hash bytea PRIMARY KEY,
                height integer,
                tx_position integer,
                tx_offset bigint,
                tx_num bigint
            );
            CREATE UNIQUE INDEX tx_num_idx ON transactions (tx_num);
            """
        )

        tx_upsert_stmt = await conn.prepare(
            """
            INSERT INTO transactions
            SELECT tx_hash, height, tx_position, tx_offset
            FROM temp_txs
            ON CONFLICT (tx_hash)
            DO UPDATE SET tx_hash=excluded.tx_hash, height=excluded.height, tx_position=excluded.tx_position, tx_offset=excluded.tx_offset;
        """
        )

        t0 = time.time()
        # await tx_insert_naive(conn, tx_rows)
        await tx_insert_copy_method(conn, tx_rows)
        await tx_upsert_from_temp_tx(tx_upsert_stmt)
        t1 = time.time() - t0
        print_results_asyncpg(len(tx_offsets), t1)

        # Select a row from the table.
        # rows = await conn.fetch("SELECT * FROM transactions")
        # for row in rows:
        #     print(row)

        # Close the connection.
        await conn.close()

    asyncio.get_event_loop().run_until_complete(main())
    store = setup_storage(NetworkConfig(REGTEST))
