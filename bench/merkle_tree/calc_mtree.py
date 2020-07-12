import time
import os
from pathlib import Path
from struct import Struct

from bitcoinx import double_sha256, hash_to_hex_str

from conduit.database.lmdb_database import LMDB_Database
from offsets import TX_OFFSETS
try:
    from conduit._algorithms import calc_mtree  # cython
except ModuleNotFoundError:
    from conduit.algorithms import calc_mtree  # pure python

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
struct_le_I = Struct("<I")

if __name__ == "__main__":
    with open("../data/block413567.raw", "rb") as f:
        raw_block = bytearray(f.read())
    blk_hash = double_sha256(raw_block[0:80])
    t0 = time.time()
    mtree = calc_mtree(raw_block, TX_OFFSETS)

    lmdb_db = LMDB_Database(storage_path=Path(MODULE_DIR).joinpath("lmdb_test_data").__str__())
    lmdb_db.put_merkle_tree(mtree, blk_hash)
    t1 = time.time() - t0
    print(f"time taken={t1} seconds")

    for level in mtree.keys():
        key = blk_hash + struct_le_I.pack(level)
        result = lmdb_db.get_mtree_row(key)
        print(len(result))

    # roughly 77mbps for naive, full merkle tree (single core) approach - good enough for a start!
