import array
import logging
import time
import os
from pathlib import Path
from struct import Struct

from bitcoinx import double_sha256

from conduit_lib.database.lmdb.lmdb_database import LMDB_Database
# simulating that the proprocessor has already provided this
from bench.merkle_tree.offsets import TX_OFFSETS
# try:
#     from conduit_raw.conduit_raw.workers._algorithms import calc_mtree  # cython
# except ModuleNotFoundError:
#     from conduit_raw.conduit_raw.workers.algorithms import calc_mtree  # pure python
from conduit_lib.algorithms import calc_mtree  # pure python

logging.basicConfig(level=logging.DEBUG)

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
struct_le_I = Struct("<I")

if __name__ == "__main__":
    lmdb_db = LMDB_Database(storage_path=Path(MODULE_DIR).joinpath("lmdb_test_data").__str__())
    TX_OFFSETS_ARRAY = array.array("Q", TX_OFFSETS)

    with open("../data/block413567.raw", "rb") as f:
        raw_block = bytearray(f.read())
    blk_hash = double_sha256(raw_block[0:80])
    t0 = time.perf_counter()
    mtree = calc_mtree(raw_block, TX_OFFSETS_ARRAY)
    lmdb_db.put_merkle_tree(blk_hash, mtree)
    lmdb_db.put_tx_offsets(blk_hash, TX_OFFSETS_ARRAY)
    lmdb_db.sync()
    t1 = time.perf_counter() - t0

    mbps = int(len(TX_OFFSETS_ARRAY) * 642 / t1 / 1024 / 1024)
    print(f"time taken={t1} seconds for {len(TX_OFFSETS_ARRAY)} txs. Therefore {mbps}MB/sec "
          f"for av. tx size of 642 bytes/tx")

    for level in mtree.keys():
        result = lmdb_db.get_mtree_row(blk_hash, level)
        print(len(result))

    # 142MB/sec to calculate mtree
    # 35MB/sec for calculate mtree and push to LMDB...

    # Now try without storing base level

