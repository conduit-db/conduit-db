import logging
import os
import time
from pathlib import Path
from struct import Struct
from typing import List, Tuple, Dict, Optional

import lmdb

from ..constants import PROFILING

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
struct_be_I = Struct(">I")
struct_le_I = Struct("<I")


class LMDB_Database:
    """simple interface to LMDB"""

    BLOCKS_DB = b"blocks_db"
    BLOCK_NUMS_DB = b"block_nums_db"
    MTREE_DB = b"mtree_db"
    DEFAULT_DIR = Path(MODULE_DIR).parent.parent.parent.joinpath('lmdb_data').__str__()

    def __init__(self, storage_path: str=DEFAULT_DIR):
        self.logger = logging.getLogger("lmdb-database")
        self.logger.setLevel(PROFILING)
        self.env: Optional[lmdb.Environment] = None
        self.blocks_db = None
        self.block_nums_db = None
        self._opened = False

        # on windows there is a bug where this requires the disc space to be free
        # on linux can set this to a very large number (e.g. 10 terabytes)
        self._map_size = pow(1024, 3) * 30
        self._storage_path = storage_path
        self.open()
        # self.logger.debug("opened LMDB database")

    def open(self):
        self.env = lmdb.open(self._storage_path, max_dbs=3, map_size=self._map_size,
            metasync=True, readahead=False)

        self.blocks_db = self.env.open_db(self.BLOCKS_DB)
        self.block_nums_db = self.env.open_db(self.BLOCK_NUMS_DB)
        self.mtree_db = self.env.open_db(self.MTREE_DB)
        self._opened = True
        return True

    def close(self):
        if not self._opened:
            return
        self.env.close()

        self.env = None
        self._db1 = None
        self._db2 = None
        self._opened = False

    def get_block(self, key: int):
        with self.env.begin(db=self.blocks_db) as txn:
            return txn.get(struct_be_I.pack(key))

    def get_mtree_row(self, key: bytes):
        with self.env.begin(db=self.mtree_db) as txn:
            return txn.get(key)

    def get_last_block_num(self) -> int:
        with self.env.begin(db=self.blocks_db, write=False) as txn:
            cur = txn.cursor()
            # print(f"cur.last()={cur.last()}")
            if cur.last():
                # print(f"cur.key()={cur.key()}")
                last_block_num = struct_be_I.unpack(cur.key())[0]
                # print(f"last_block_num: {last_block_num}")
                return last_block_num
            else:
                return -1  # so that fist entry is key==0 (+=1 of 'last_block_num')

    def put_blocks(self, batched_blocks: List[Tuple[bytes, int, int]], shared_mem_buffer :memoryview):
        """write blocks in append-only mode to disc"""
        try:
            t0 = time.time()
            block_nums_map = {}  # block_hash: block_num
            last_block_num = self.get_last_block_num()
            next_block_num = last_block_num + 1

            with self.env.begin(db=self.blocks_db, write=True, buffers=True) as txn:

                block_nums = range(next_block_num, (len(batched_blocks) + next_block_num))
                # print(f"block_nums={block_nums}")
                for block_num, block_row in zip(block_nums, batched_blocks):
                    blk_hash, blk_start_pos, blk_end_pos = block_row
                    # print(f"put: ({block_num}, {shared_mem_buffer[blk_start_pos: blk_end_pos]})")
                    txn.put(struct_be_I.pack(block_num), shared_mem_buffer[blk_start_pos: blk_end_pos].tobytes(),
                        append=True, overwrite=False)
                    block_nums_map[blk_hash] = block_num

            # print(block_nums_map)
            with self.env.begin(db=self.block_nums_db, write=True, buffers=True) as txn:
                for blk_hash, block_num in block_nums_map.items():
                    txn.put(blk_hash, struct_be_I.pack(block_num), overwrite=False)

            t1 = time.time() - t0
            if len(batched_blocks) > 0:
                self.logger.log(PROFILING,
                    f"elapsed time for {len(batched_blocks)} raw_blocks took {t1} seconds")
        except Exception as e:
            self.logger.exception(e)

    def put_merkle_tree(self, mtree: Dict, block_hash: bytes):
        """stores an array of key: values where key is a concatenation of the block_hash + level
        of the mtree. Value is an array of 32 byte hashes."""
        def pack_list_to_concatenated_bytes(hashes):
            # Todo - see algorithms.py for future optimization plans
            byte_array = bytearray()
            for _hash in hashes:
                byte_array += _hash
            return byte_array

        with self.env.begin(db=self.mtree_db, write=True, buffers=True) as txn:
            for level, hashes in mtree.items():
                key = block_hash + struct_le_I.pack(level)
                value = pack_list_to_concatenated_bytes(hashes)
                txn.put(key, value)

if __name__ == "__main__":
    lmdb_db = LMDB_Database()

    # fake_blocks = b"11111222223333344444"
    # fake_batched_blocks = [(b'blkhash1', 0, 5), (b'blkhash2', 5, 10), (b'blkhash3', 10, 15),
    #     (b'blkhash4', 15, 20)]
    # lmdb_db.put_blocks(fake_batched_blocks, memoryview(fake_blocks))
    for key in range(100):
        # val = lmdb_db.get(key)
        # print(val)
        txn = lmdb_db.env.begin(buffers=True)
        buf = txn.get_block(struct_be_I.pack(key), db=lmdb_db.blocks_db)
        print(bytes(buf[50:100]))
    lmdb_db.close()
