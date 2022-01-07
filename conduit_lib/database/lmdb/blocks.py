import logging
import os
import time

import typing
from io import BytesIO
from typing import Tuple, Optional, List
from pathlib import Path

import bitcoinx
import cbor2
from bitcoinx.packing import struct_le_Q, struct_be_I
from lmdb import Cursor

from conduit_lib.database.ffdb.flat_file_db import FlatFileDb, DataLocation
from conduit_lib.types import BlockMetadata, Slice

if typing.TYPE_CHECKING:
    from conduit_lib import LMDB_Database


from conduit_lib.constants import PROFILING

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))


class LmdbBlocks:

    logger = logging.getLogger("lmdb-tx-offsets")
    logger.setLevel(PROFILING)
    BLOCKS_DB = b"blocks_db"
    BLOCK_NUMS_DB = b"block_nums_db"
    BLOCK_METADATA_DB = b"block_metadata_db"
    RAW_BLOCKS_DIR_DEFAULT = Path(MODULE_DIR).parent.parent.parent / 'raw_blocks'
    RAW_BLOCKS_DIR = os.environ.get("RAW_BLOCKS_DIR", str(RAW_BLOCKS_DIR_DEFAULT))

    _last_dat_file_num = 0

    def __init__(self, db: 'LMDB_Database'):
        self.db = db
        self.ffdb = FlatFileDb(Path(self.RAW_BLOCKS_DIR))
        self.blocks_db = self.db.env.open_db(self.BLOCKS_DB)
        self.block_nums_db = self.db.env.open_db(self.BLOCK_NUMS_DB)
        self.block_metadata_db = self.db.env.open_db(self.BLOCK_METADATA_DB)

    def _get_last_block_num(self) -> int:
        with self.db.env.begin(db=self.blocks_db, write=False) as txn:
            cur = txn.cursor()
            if cur.last():
                last_block_num: int = struct_be_I.unpack(cur.key())[0]
                return last_block_num
            else:
                return 0  # so that first entry is key==1

    # -------------------- EXTERNAL API -------------------- #

    def get_block_num(self, block_hash: bytes) -> Optional[int]:
        with self.db.env.begin(db=self.block_nums_db) as txn:
            result = txn.get(block_hash)
            if result:
                return typing.cast(int, struct_be_I.unpack(result)[0])
            self.logger.error(f"Block num for block_hash: "
                              f"{bitcoinx.hash_to_hex_str(block_hash)} not found")
            return None

    def get_block(self, block_num: int, slice: Optional[Slice]=None) -> Optional[bytes]:
        """If end_offset=0 then it goes to the end of the block"""
        with self.db.env.begin(db=self.blocks_db, buffers=False) as txn:
            val: bytes = txn.get(struct_be_I.pack(block_num))
            if not val:
                self.logger.error(f"Block for block_num: {block_num} not found")
                return None
            read_path, start_offset_in_dat_file, end_offset_in_dat_file = cbor2.loads(val)

        data_location = DataLocation(read_path,
            start_offset_in_dat_file, end_offset_in_dat_file)
        self.ffdb.read_from_db(data_location, slice)
        return self.ffdb.read_from_db(data_location, slice)

    def put_blocks(self, batched_blocks: List[Tuple[bytes, int, int]],
            shared_mem_buffer: memoryview) -> None:
        """write blocks in append-only mode to disc. The whole batch is written to a single
        file."""
        t0 = time.time()

        try:
            last_block_num = self._get_last_block_num()
            next_block_num = last_block_num + 1

            with self.db.env.begin(write=True, buffers=False) as txn:
                cursor_blocks: Cursor = txn.cursor(db=self.blocks_db)
                cursor_block_nums: Cursor = txn.cursor(db=self.block_nums_db)
                cursor_block_metadata: Cursor = txn.cursor(db=self.block_metadata_db)

                block_nums = range(next_block_num, (len(batched_blocks) + next_block_num))
                raw_blocks_arr = bytearray()
                for block_num, block_row in zip(block_nums, batched_blocks):
                    blk_hash, blk_start_pos, blk_end_pos = block_row
                    raw_block = shared_mem_buffer[blk_start_pos: blk_end_pos].tobytes()
                    stream = BytesIO(raw_block[80:89])
                    tx_count = bitcoinx.read_varint(stream.read)
                    raw_blocks_arr += raw_block

                    data_location: DataLocation = self.ffdb.write_from_bytes(raw_block)

                    block_num_bytes = struct_be_I.pack(block_num)
                    len_block_bytes = struct_le_Q.pack(len(raw_block))
                    tx_count_bytes = struct_le_Q.pack(tx_count)
                    cursor_blocks.put(block_num_bytes, cbor2.dumps(data_location),
                        append=True, overwrite=False)
                    cursor_block_nums.put(blk_hash, block_num_bytes, overwrite=False)
                    cursor_block_metadata.put(blk_hash,
                        len_block_bytes + tx_count_bytes)
        except Exception as e:
            self.logger.exception(e)

    def get_block_metadata(self, block_hash: bytes) -> Optional[BlockMetadata]:
        """Namely size in bytes but could later include things like compression dictionary id and
        maybe interesting things like MinerID"""
        assert self.db.env is not None
        with self.db.env.begin(db=self.block_metadata_db, write=False, buffers=True) as txn:
            val = txn.get(block_hash)
            if val:
                # Note: This can return zero - which is a "falsey" type value. Take care
                val = bytes(val)
                block_size = struct_le_Q.unpack(val[0:8])[0]
                tx_count = struct_le_Q.unpack(val[8:16])[0]
                return BlockMetadata(block_size, tx_count)
            return None
