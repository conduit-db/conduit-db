import array
import logging
import os
import time
from pathlib import Path
from struct import Struct
from typing import List, Tuple, Dict, Optional

import bitcoinx
import lmdb
from bitcoinx.packing import struct_le_Q

try:
    from ...constants import PROFILING
except ImportError:
    from conduit_lib.constants import PROFILING

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
struct_be_I = Struct(">I")
struct_le_I = Struct("<I")


class EntryNotFound(Exception):
    pass


class LMDB_Database:
    """simple interface to LMDB"""

    BLOCKS_DB = b"blocks_db"
    BLOCK_NUMS_DB = b"block_nums_db"
    MTREE_DB = b"mtree_db"
    TX_OFFSETS_DB = b"tx_offsets_db"
    BLOCK_METADATA_DB = b"block_metadata_db"

    def __init__(self, storage_path: Optional[str]=None):
        self.logger = logging.getLogger("lmdb-database")
        self.logger.setLevel(PROFILING)

        self.DEFAULT_DIR = Path(MODULE_DIR).parent.parent.parent.parent.joinpath('lmdb_data').__str__()
        self.LMDB_READONLY: int = int(os.environ.get("LMDB_READONLY", '0'))  # defaults to writable mode
        self.LMDB_DATABASE_PATH: str = os.environ.get("LMDB_DATABASE_PATH",
            None)  # defaults to writable mode

        # self.logger.debug(f"self.DEFAULT_DIR={self.DEFAULT_DIR}")
        # self.logger.debug(f"self.LMDB_READONLY={self.LMDB_READONLY}")
        # self.logger.debug(f"self.LMDB_DATABASE_PATH={self.LMDB_DATABASE_PATH}")

        if self.LMDB_DATABASE_PATH:
            self.DEFAULT_DIR = self.LMDB_DATABASE_PATH

        if not storage_path:
            storage_path = self.DEFAULT_DIR

        if not Path(storage_path).exists():
            os.makedirs(Path(storage_path), exist_ok=True)

        self.env: Optional[lmdb.Environment] = None
        self.blocks_db = None
        self.block_nums_db = None
        self.mtree_db = None
        self.tx_offsets_db = None
        self.block_metadata_db = None
        self._opened = False

        # on windows there is a bug where this requires the disc space to be free
        # on linux can set this to a very large number (e.g. 10 terabytes)
        self._map_size = pow(1024, 3) * 50
        self._storage_path = storage_path
        self.open()
        self.logger.debug(f"opened LMDB database at {storage_path}")

    def open(self):
        # try:
        if self.LMDB_READONLY == 1:
            self.env = lmdb.open(self._storage_path, max_dbs=5, readonly=self.LMDB_READONLY,
                readahead=False, sync=False)
        else:
            self.env = lmdb.open(self._storage_path, max_dbs=5, readonly=False,
                readahead=False, sync=False, map_size=self._map_size)
        # except lmdb.LockError:
        #     self.logger.error("LMDB temporarily unavailable - retrying...")

        self.blocks_db = self.env.open_db(self.BLOCKS_DB)
        self.block_nums_db = self.env.open_db(self.BLOCK_NUMS_DB)
        self.mtree_db = self.env.open_db(self.MTREE_DB)
        self.tx_offsets_db = self.env.open_db(self.TX_OFFSETS_DB)
        self.block_metadata_db = self.env.open_db(self.BLOCK_METADATA_DB)
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

    def get_block_num(self, block_hash: bytes) -> Optional[int]:
        with self.env.begin(db=self.block_nums_db) as txn:
            result = txn.get(block_hash)
            if result:
                return struct_be_I.unpack(txn.get(block_hash))[0]
            raise EntryNotFound(f"Block num for block_hash: "
                                f"{bitcoinx.hash_to_hex_str(block_hash)} not found")

    # Todo - raw blocks need to be stored in files (identified by block_num in filename) t
    #  to seek to offset
    def get_block(self, block_num: int, start_offset: int=0,
            end_offset: int=0, buffers=False) -> bytes:
        """If end_offset=0 then it goes to the end of the block"""
        with self.env.begin(db=self.blocks_db, buffers=buffers) as txn:
            buf: memoryview = txn.get(struct_be_I.pack(block_num))
            if not buf:
                raise EntryNotFound(f"Block for block_num: {block_num} not found")

            if end_offset == 0:
                result = buf[start_offset:]
            else:
                result = buf[start_offset: end_offset]

            if buffers:
                return result.tobytes()
            return result

    def get_mtree_row(self, blk_hash: bytes, level: int) -> bytes:
        with self.env.begin(db=self.mtree_db) as txn:
            return txn.get(blk_hash + struct_le_I.pack(level))

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
                return 0  # so that first entry is key==1

    def put_blocks(self, batched_blocks: List[Tuple[bytes, int, int]], shared_mem_buffer: memoryview):
        """write blocks in append-only mode to disc"""
        try:
            t0 = time.time()
            block_nums_map = {}  # block_hash: block_num
            last_block_num = self.get_last_block_num()
            next_block_num = last_block_num + 1

            tx = self.env.begin(write=True, buffers=False)

            block_nums = range(next_block_num, (len(batched_blocks) + next_block_num))
            # print(f"block_nums={block_nums}")
            for block_num, block_row in zip(block_nums, batched_blocks):
                blk_hash, blk_start_pos, blk_end_pos = block_row
                raw_block = shared_mem_buffer[blk_start_pos: blk_end_pos].tobytes()
                # self.logger.debug(f"put_blocks: (block_num={block_num}, blk_hash={blk_hash}")

                tx.put(struct_be_I.pack(block_num), raw_block, db=self.blocks_db, append=True, overwrite=False)
                tx.put(blk_hash, struct_be_I.pack(block_num), db=self.block_nums_db, overwrite=False)
                tx.put(blk_hash, struct_le_Q.pack(len(raw_block)), db=self.block_metadata_db)

            tx.commit()

            t1 = time.time() - t0
            if len(batched_blocks) > 0:
                self.logger.log(PROFILING,
                    f"elapsed time for {len(batched_blocks)} raw_blocks took {t1} seconds")
        except Exception as e:
            self.logger.exception(e)

    def put_merkle_tree(self, block_hash: bytes, mtree: Dict):
        """In the current design we store the entire txid set (all levels of the merkle tree) as
        this is the simplest way to cover all use cases at the moment with low latency. It is not
        the long-term design as it is very wasteful of disc space and io stress compared to what
        it can be.
        """
        def pack_list_to_concatenated_bytes(hashes):
            # Todo - see algorithms.py for future optimization plans
            byte_array = bytearray()
            for _hash in hashes:
                byte_array += _hash
            return byte_array

        with self.env.begin(db=self.mtree_db, write=True, buffers=False) as txn:
            cursor = txn.cursor()
            entries = []

            for level, hashes in reversed(mtree.items()):
                key = block_hash + struct_le_I.pack(level)
                value = pack_list_to_concatenated_bytes(hashes)
                entries.append((key, value))
            cursor.putmulti(entries)

            #self.env.sync(True)

    def put_tx_offsets(self, block_hash: bytes, tx_offsets: array.ArrayType):
        with self.env.begin(db=self.tx_offsets_db, write=True, buffers=False) as txn:
            txn.put(block_hash, tx_offsets)

    def get_tx_offsets(self, block_hash: bytes) -> array.ArrayType:
        with self.env.begin(db=self.tx_offsets_db, write=False, buffers=True) as txn:
            return array.array("Q", bytes(txn.get(block_hash)))

    def get_block_metadata(self, block_hash: bytes) -> int:
        """Namely size in bytes but could later include things like compression dictionary id and
        maybe interesting things like MinerID"""
        with self.env.begin(db=self.block_metadata_db, write=False, buffers=True) as txn:
            val = txn.get(block_hash)
            if val:
                # Note: This can return zero - which is a "falsey" type value. Take care
                return struct_le_Q.unpack(bytes(val))[0]

    def sync(self):
        self.env.sync(True)


if __name__ == "__main__":
    # Debugging script - gives a dump of all data for a given hex blockhash
    logger = logging.getLogger("debug-lmdb")
    logging.basicConfig(level=logging.DEBUG)
    lmdb_db = LMDB_Database()

    with lmdb_db.env.begin(db=lmdb_db.block_metadata_db, write=False, buffers=False) as txn:
        cur = txn.cursor()
        print(cur.first())
        print(bytes(cur.value()))
        while cur.next():
            print(bytes(cur.value()))

    # fake_blocks = b"11111222223333344444"
    # fake_batched_blocks = [(b'blkhash1', 0, 5), (b'blkhash2', 5, 10), (b'blkhash3', 10, 15),
    #     (b'blkhash4', 15, 20)]
    # lmdb_db.put_blocks(fake_batched_blocks, memoryview(fake_blocks))
    # for key in range(100):
    #     # val = lmdb_db.get(key)
    #     # print(val)
    #     txn = lmdb_db.env.begin(buffers=True)
    #     buf = txn.get_block(struct_be_I.pack(key), db=lmdb_db.blocks_db)
    #     print(bytes(buf[50:100]))
    # lmdb_db.close()
