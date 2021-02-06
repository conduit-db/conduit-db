import asyncio
import logging
import time

from conduit.database.mysql.mysql_database import load_mysql_database

try:
    from conduit.workers._algorithms import preprocessor, parse_block  # cython
except ModuleNotFoundError:
    from conduit.workers.algorithms import preprocessor, parse_txs  # pure python
from bench.utils import print_results, print_results_mysql_bench

if __name__ == "__main__":
    logger = logging.getLogger("insert_block_txs")

    async def main():
        with open("../data/block413567.raw", "rb") as f:
            raw_block = bytearray(f.read())

        t0 = time.time()
        tx_offsets = preprocessor(raw_block)
        tx_rows, in_rows, out_rows, set_pd_rows, pd_rows = parse_txs(
            bytes(raw_block), tx_offsets, 413567, True
        )

        t1 = time.time() - t0
        print_results(len(tx_offsets), t1 / 1, raw_block)

        mysql_db = await load_mysql_database()

        await mysql_db.mysql_update_settings()
        await mysql_db.mysql_drop_tables()
        await mysql_db.mysql_create_permanent_tables()
        await mysql_db.mysql_create_temp_inputs_table()

        t0 = time.time()
        await mysql_db.mysql_bulk_load_confirmed_tx_rows(tx_rows)
        await mysql_db.mysql_bulk_load_output_rows(out_rows)
        await mysql_db.mysql_bulk_load_input_rows(in_rows)
        await mysql_db.mysql_bulk_load_pushdata_rows(set_pd_rows)
        t1 = time.time() - t0
        print_results_mysql_bench(len(tx_offsets), t1)

        # rows = await mysql_db.mysql_conn.fetch("SELECT * FROM outputs;")
        # for row in rows:
        #     print(row)

        # Close the connection.
        await mysql_db.mysql_drop_temp_inputs()
        await mysql_db.close()

    asyncio.get_event_loop().run_until_complete(main())
