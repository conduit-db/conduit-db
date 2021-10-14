"""This is NOT a performance benchmark - we would need data sets at least 1000 times larger
to even begin to be meaningful because the system relies heavily on batch-wise processing.

This is only used to explore how MySQL handles things like colliding hashes...e.g. is the
data lost? or are both kept? We need to know these kinds of things for sure...
"""
import array
import asyncio
import logging
import time
import os
from pathlib import Path

import bitcoinx

from conduit_lib.database.mysql.mysql_database import load_mysql_database
from conduit_lib.logging_server import TCPLoggingServer

try:
    from conduit_lib._algorithms import preprocessor  # cython
except ModuleNotFoundError:
    from conduit_lib.algorithms import preprocessor   # pure python

try:
    from conduit_lib._algorithms import parse_txs  # cython
except ModuleNotFoundError:
    from conduit_lib.algorithms import parse_txs   # pure python

from bench_and_experiments.utils import print_results, print_results_mysql_bench

from conduit_lib.logging_client import setup_tcp_logging

MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))

if __name__ == "__main__":
    setup_tcp_logging(port=65421)
    # logging.basicConfig(level=9)
    logger = logging.getLogger("insert_block_txs")
    logger.setLevel(level=9)

    p = TCPLoggingServer(port=65421, service_name='test_service')
    p.start()
    time.sleep(2)

    async def main():
        mysql_db = load_mysql_database()
        mysql_db.mysql_drop_tables()
        mysql_db.tables.mysql_create_permanent_tables()

        with open(MODULE_DIR.parent.joinpath("data/block413567.raw"), "rb") as f:
            raw_block = array.array('B', (f.read()))

        tx_offsets_array = array.array("Q", [0 for i in range(10000)])

        t0 = time.perf_counter()
        count_added, tx_offsets_array = preprocessor(raw_block, tx_offsets_array, block_offset=0)
        tx_rows, in_rows, out_rows, set_pd_rows = parse_txs(raw_block, tx_offsets_array[0:count_added], 413567, True, 0)
        t1 = time.perf_counter() - t0
        print_results(count_added, t1 / 1, raw_block)

        def check_same_on_subsequent_runs():
            """I got paranoid that the pushdatas were changing each time (they are not)
            turns out it was the conversion from set -> list causing the ordering to be
            jumbled (which is fine) - this here puts my mind back at ease..."""
            tx_rows, in_rows, out_rows, set_pd_rows_second_time = parse_txs(raw_block,
                tx_offsets_array[0:count_added], 413567, True, 0)
            for x in set_pd_rows:
                for y in set_pd_rows_second_time:
                    if x[0] == y[0] and x[1] == y[1] and x[2] == y[2] and x[3] == y[3]:
                        # print(f"x[0] == y[0]: {x[0] == y[0]} & x[1] == y[1]: {x[1] == y[1]}")
                        break
                else:
                    return False
            return True

        check_same_on_subsequent_runs()

        # --------- Ensure colliding tx doesn't go missing ---------- #
        # NOTE: This only works because I have placed a secondary index on the
        # block height. Otherwise the row would be overwritten with the new key: value pair.
        fake_colliding_tx = ('0ee8325ec400e665714d171606ca645c926accbfd8d951393a1b34be5e113032', 123456, 0, 83, 268)
        print(f"original tx: ('0feb3dff7fd3caf22f6dd32f4c1e14d7b7a0d20bdf5d38705d62e4f4f3ae4a5b', 413567, 0, 83, 268)")
        print(f"fake_colliding_tx: {fake_colliding_tx}")
        tx_rows.append(fake_colliding_tx)

        # --------- Ensure colliding input doesn't go missing ---------- #
        # NOTE: This only works because I have placed a secondary index on the
        # start_offset as a 'distinguishing field'.
        # Otherwise the row would be overwritten with the new key: value pair.
        fake_colliding_input = ('0ee8325ec400e665714d171606ca645c926accbfd8d951393a1b34be5e113032', 1, 'eac5baef46156384776a5a30555d71269ae1f1443b2c3bffb4216583ba8228eb', 0, 123456, 149695)
        print(f"original input: ('0ee8325ec400e665714d171606ca645c926accbfd8d951393a1b34be5e113032', 1, 'eac5baef46156384776a5a30555d71269ae1f1443b2c3bffb4216583ba8228eb', 0, 149547, 149695)")
        print(f"fake_colliding_input: {fake_colliding_input}")
        in_rows.append(fake_colliding_input)

        # --------- Ensure colliding pushdata doesn't go missing ---------- #
        # NOTE: This only works because I have placed a secondary index on the
        # start_offset as a 'distinguishing field'.
        # Otherwise the row would be overwritten with the new key: value pair.
        fake_colliding_pushdata1 = ('53955e89899505471b2ff7e927181433a9c92c542ba49239c3f50c4619c671ae', 'deadbeefc0edf64496f834d27d7b3be38c0a808070c33b154753ff76400de832', 0, 1)
        fake_colliding_pushdata2 = ('53955e89899505471b2ff7e927181433a9c92c542ba49239c3f50c4619c671ae', 'befc484bc0edf64496f834d27d7b3be38c0a808070c33b154753ff76400de832', 1, 1)

        print(f"original pushdata: ('53955e89899505471b2ff7e927181433a9c92c542ba49239c3f50c4619c671ae', 'befc484bc0edf64496f834d27d7b3be38c0a808070c33b154753ff76400de832', 0, 1)")
        print(f"fake_colliding_pushdata: {fake_colliding_pushdata1}")
        print(f"fake_colliding_pushdata: {fake_colliding_pushdata2}")
        set_pd_rows.append(fake_colliding_pushdata1)
        set_pd_rows.append(fake_colliding_pushdata2)

        # This dataset is waaaaaaay too small to give meaningful benchmark results but this
        # code helps me to test things like collision handling
        t0 = time.perf_counter()
        mysql_db.mysql_bulk_load_confirmed_tx_rows(tx_rows)
        mysql_db.mysql_bulk_load_output_rows(out_rows)
        mysql_db.mysql_bulk_load_input_rows(in_rows)
        mysql_db.mysql_bulk_load_pushdata_rows(set_pd_rows)
        t1 = time.perf_counter() - t0
        print_results_mysql_bench(count_added, t1)

        mysql_db.mysql_conn.query("select * from confirmed_transactions;")
        result = mysql_db.mysql_conn.store_result()
        for row in result.fetch_row(0):
            # print(row)
            pass
        # Close the connection.
        mysql_db.close()

    asyncio.get_event_loop().run_until_complete(main())

    p.kill()
