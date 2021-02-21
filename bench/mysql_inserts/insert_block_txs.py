import array
import asyncio
import logging
import time
import os
from pathlib import Path

from conduit.database.mysql.mysql_database import load_test_mysql_database
from conduit.workers.logging_server import TCPLoggingServer

try:
    from conduit.workers._algorithms import preprocessor, parse_txs  # cython
except ModuleNotFoundError:
    from conduit.workers.algorithms import preprocessor, parse_txs  # pure python
from bench.utils import print_results, print_results_mysql_bench

from conduit.logging_client import setup_tcp_logging

MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))

if __name__ == "__main__":
    setup_tcp_logging()
    # logging.basicConfig(level=9)
    logger = logging.getLogger("insert_block_txs")
    logger.setLevel(level=9)

    p = TCPLoggingServer()
    p.start()
    time.sleep(2)

    async def main():
        with open(MODULE_DIR.parent.joinpath("data/block413567.raw"), "rb") as f:
            raw_block = bytearray(f.read())

        tx_offsets_array = array.array("Q", [0 for i in range(10000)])

        t0 = time.time()
        count_added, tx_offsets_array = preprocessor(raw_block, tx_offsets_array, block_offset=0)
        tx_rows, in_rows, out_rows, set_pd_rows = parse_txs(
            bytes(raw_block), tx_offsets_array[0:count_added], 413567, True
        )
        t1 = time.time() - t0
        print_results(count_added, t1 / 1, raw_block)

        mysql_db = load_test_mysql_database()

        mysql_db.mysql_drop_tables()
        mysql_db.tables.mysql_create_permanent_tables()

        t0 = time.time()
        mysql_db.mysql_bulk_load_confirmed_tx_rows(tx_rows)
        mysql_db.mysql_bulk_load_output_rows(out_rows)
        mysql_db.mysql_bulk_load_input_rows(in_rows)
        mysql_db.mysql_bulk_load_pushdata_rows(set_pd_rows)
        t1 = time.time() - t0
        print_results_mysql_bench(count_added, t1)

        # rows = await mysql_db.mysql_conn.fetch("SELECT * FROM outputs;")
        # for row in rows:
        #     print(row)

        # Close the connection.
        mysql_db.close()

    asyncio.get_event_loop().run_until_complete(main())

    p.kill()
