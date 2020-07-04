import logging
import time
from struct import Struct
from typing import List, Tuple, Dict, Optional

import lmdb
from bitcoinx import hash_to_hex_str

struct_be_I = Struct(">I")

class LMDB_Database:
    """simple interface to LMDB"""

    BLOCKS_DB = b"blocks_db"
    BLOCK_NUMS_DB = b"block_nums_db"

    def __init__(self, storage_path: str="lmdb_data"):
        self._env: Optional[lmdb.Environment] = None
        self._blocks_db = None
        self._block_nums_db = None
        self._opened = False

        # on windows there is a bug where this requires the disc space to be free
        # on linux can set this to a very large number (e.g. 10 terabytes)
        self._map_size = pow(1024, 3) * 30
        self._storage_path = storage_path
        self.open()

        self.logger = logging.getLogger("lmdb_database")

    def open(self):
        self._env = lmdb.open(self._storage_path, max_dbs=2, map_size=self._map_size,
            metasync=True, readahead=False)

        self._blocks_db = self._env.open_db(self.BLOCKS_DB)
        self._block_nums_db = self._env.open_db(self.BLOCK_NUMS_DB)
        self._opened = True
        return True

    def close(self):
        if not self._opened:
            return
        self._env.close()

        self._env = None
        self._db1 = None
        self._db2 = None
        self._opened = False

    def get(self, key: int):
        with self._env.begin(db=self._blocks_db) as txn:
            return txn.get(struct_be_I.pack(key))

    def get_last_block_num(self) -> int:
        with self._env.begin(db=self._blocks_db, write=False) as txn:
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
        t0 = time.time()
        block_nums_map = {}  # block_hash: block_num
        last_block_num = self.get_last_block_num()
        next_block_num = last_block_num + 1

        with self._env.begin(db=self._blocks_db, write=True, buffers=True) as txn:

            block_nums = range(next_block_num, (len(batched_blocks) + next_block_num))
            # print(f"block_nums={block_nums}")
            for block_num, block_row in zip(block_nums, batched_blocks):
                blk_hash, blk_start_pos, blk_end_pos = block_row
                # print(f"put: ({block_num}, {shared_mem_buffer[blk_start_pos: blk_end_pos]})")
                txn.put(struct_be_I.pack(block_num), shared_mem_buffer[blk_start_pos: blk_end_pos],
                    append=True, overwrite=False)
                block_nums_map[blk_hash] = block_num

        # print(block_nums_map)
        with self._env.begin(db=self._block_nums_db, write=True, buffers=True) as txn:
            for blk_hash, block_num in block_nums_map.items():
                txn.put(blk_hash, struct_be_I.pack(block_num), overwrite=False)

        t1 = time.time() - t0
        if len(batched_blocks) > 0:
            self.logger.debug(f"elapsed time for {len(batched_blocks)} raw_blocks took {t1} seconds")

if __name__ == "__main__":
    lmdb_db = LMDB_Database()
    fake_blocks = b"11111222223333344444"
    fake_batched_blocks = [(b'blkhash1', 0, 5), (b'blkhash2', 5, 10), (b'blkhash3', 10, 15),
        (b'blkhash4', 15, 20)]
    lmdb_db.put_blocks(fake_batched_blocks, memoryview(fake_blocks))
    print(lmdb_db.get(0))
    print(lmdb_db.get(1))
    print(lmdb_db.get(2))
    print(lmdb_db.get(3))
    lmdb_db.close()
