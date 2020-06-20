import asyncio
import logging
import time

from conduit.database import load_test_pg_database

try:
    from conduit._algorithms import preprocessor, parse_block  # cython
except ModuleNotFoundError:
    from conduit.algorithms import preprocessor, parse_block  # pure python
from bench.utils import print_results, print_results_asyncpg

from conduit.store import setup_storage


if __name__ == "__main__":
    logger = logging.getLogger("insert_block_txs")

    async def main():
        with open("../data/block413567.raw", "rb") as f:
            raw_block = bytearray(f.read())

        t0 = time.time()
        tx_offsets = preprocessor(raw_block)
        tx_rows, in_rows, out_rows, set_pd_rows = parse_block(
            bytes(raw_block), tx_offsets, 413567, 0, 1556
        )

        t1 = time.time() - t0
        print_results(len(tx_offsets), t1 / 1, raw_block)

        pg_db = await load_test_pg_database()

        await pg_db.pg_update_settings()
        await pg_db.pg_drop_tables()
        await pg_db.pg_create_permanent_tables()
        await pg_db.pg_create_temp_tables()

        t0 = time.time()
        await pg_db.pg_bulk_load_tx_rows(tx_rows)
        await pg_db.pg_bulk_load_output_rows(out_rows)
        await pg_db.pg_bulk_load_input_rows(in_rows)
        await pg_db.pg_bulk_load_pushdata_rows(set_pd_rows)
        t1 = time.time() - t0
        print_results_asyncpg(len(tx_offsets), t1)

        # rows = await pg_db.pg_conn.fetch("SELECT * FROM outputs;")
        # for row in rows:
        #     print(row)

        # Close the connection.
        await pg_db.pg_drop_temp_tables()
        await pg_db.close()

    asyncio.get_event_loop().run_until_complete(main())
