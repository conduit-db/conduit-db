import logging
import os

import typing
from io import BytesIO
from pathlib import Path

import bitcoinx
import cbor2
from bitcoinx import double_sha256
from bitcoinx.packing import struct_le_Q, struct_be_I
from lmdb import Cursor

from conduit_lib.database.ffdb.flat_file_db import FlatFileDb
from conduit_lib.types import BlockMetadata, Slice, DataLocation

if typing.TYPE_CHECKING:
    from conduit_lib import LMDB_Database

from conduit_lib.constants import PROFILING

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))


class LmdbBlocks:
    logger = logging.getLogger("lmdb-blocks")
    logger.setLevel(PROFILING)
    BLOCKS_DB = b"blocks_db"
    BLOCK_NUMS_DB = b"block_nums_db"
    BLOCK_METADATA_DB = b"block_metadata_db"

    def __init__(self, db: "LMDB_Database"):
        self.db = db

        raw_blocks_dir = Path(os.environ["DATADIR_HDD"]) / "raw_blocks"
        raw_blocks_lockfile = Path(os.environ["DATADIR_SSD"]) / "raw_blocks.lock"

        self.ffdb = FlatFileDb(raw_blocks_dir, raw_blocks_lockfile)
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

    def get_block_num(self, block_hash: bytes) -> int | None:
        with self.db.env.begin(db=self.block_nums_db) as txn:
            result = txn.get(block_hash)
            if result:
                return typing.cast(int, struct_be_I.unpack(result)[0])
            self.logger.error(
                f"Block num for block_hash: " f"{bitcoinx.hash_to_hex_str(block_hash)} not found"
            )
            return None

    def get_data_location(self, block_num: int) -> DataLocation | None:
        with self.db.env.begin(db=self.blocks_db, buffers=False) as txn:
            val: bytes = txn.get(struct_be_I.pack(block_num))
            if not val:
                self.logger.error(f"Block for block_num: {block_num} not found")
                return None
        (
            read_path,
            start_offset_in_dat_file,
            end_offset_in_dat_file,
        ) = cbor2.loads(val)
        return DataLocation(read_path, start_offset_in_dat_file, end_offset_in_dat_file)

    def get_block(self, block_num: int, slice: Slice | None = None) -> bytes | None:
        """If end_offset=0 then it goes to the end of the block"""
        data_location = self.get_data_location(block_num)
        if not data_location:
            return None

        with self.ffdb:
            return self.ffdb.get(data_location, slice, lock_free_access=True)

    def put_blocks(self, small_batched_blocks: list[bytes]) -> None:
        """write blocks in append-only mode to disc."""
        try:
            last_block_num = self._get_last_block_num()
            next_block_num = last_block_num + 1

            with self.db.env.begin(write=True, buffers=False) as txn:
                cursor_blocks: Cursor = txn.cursor(db=self.blocks_db)
                cursor_block_nums: Cursor = txn.cursor(db=self.block_nums_db)
                cursor_block_metadata: Cursor = txn.cursor(db=self.block_metadata_db)
                with self.ffdb:
                    block_nums = range(
                        next_block_num,
                        (len(small_batched_blocks) + next_block_num),
                    )
                    raw_blocks_arr = bytearray()
                    for block_num, raw_block in zip(block_nums, small_batched_blocks):
                        stream = BytesIO(raw_block[0:89])
                        raw_header = stream.read(80)
                        blk_hash = double_sha256(raw_header)
                        tx_count = bitcoinx.read_varint(stream.read)
                        raw_blocks_arr += raw_block

                        data_location: DataLocation = self.ffdb.put(raw_block)

                        block_num_bytes = struct_be_I.pack(block_num)
                        len_block_bytes = struct_le_Q.pack(len(raw_block))
                        tx_count_bytes = struct_le_Q.pack(tx_count)
                        cursor_blocks.put(
                            block_num_bytes,
                            cbor2.dumps(data_location),
                            append=True,
                            overwrite=False,
                        )
                        cursor_block_nums.put(blk_hash, block_num_bytes, overwrite=False)
                        cursor_block_metadata.put(blk_hash, len_block_bytes + tx_count_bytes)

        except Exception as e:
            self.logger.exception("Caught exception")

    def put_big_block(self, big_block: tuple[bytes, DataLocation, int]) -> None:
        """For big blocks that are larger than the network buffer size they are already streamed
        to a temporary file and just need to be os.move'd into the correct resting place.
        """
        try:
            last_block_num = self._get_last_block_num()
            next_block_num = last_block_num + 1

            with self.db.env.begin(write=True, buffers=False) as txn:
                cursor_blocks: Cursor = txn.cursor(db=self.blocks_db)
                cursor_block_nums: Cursor = txn.cursor(db=self.block_nums_db)
                cursor_block_metadata: Cursor = txn.cursor(db=self.block_metadata_db)
                with self.ffdb:
                    blk_hash, data_location, tx_count = big_block
                    new_data_location: DataLocation = self.ffdb.put_big_block(data_location)
                    block_size = data_location.end_offset - data_location.start_offset
                    block_num_bytes = struct_be_I.pack(next_block_num)
                    len_block_bytes = struct_le_Q.pack(block_size)
                    tx_count_bytes = struct_le_Q.pack(tx_count)
                    cursor_blocks.put(
                        block_num_bytes,
                        cbor2.dumps(new_data_location),
                        append=True,
                        overwrite=False,
                    )
                    cursor_block_nums.put(blk_hash, block_num_bytes, overwrite=False)
                    cursor_block_metadata.put(blk_hash, len_block_bytes + tx_count_bytes)
        except Exception as e:
            self.logger.exception("Caught exception")

    def get_block_metadata(self, block_hash: bytes) -> BlockMetadata | None:
        """Namely size in bytes but could later include things like compression dictionary id and
        maybe interesting things like MinerID"""
        assert self.db.env is not None
        with self.db.env.begin(db=self.block_metadata_db, write=False, buffers=False) as txn:
            val = txn.get(block_hash)
            if val:
                # Note: This can return zero - which is a "falsey" type value. Take care
                val = bytes(val)
                block_size = struct_le_Q.unpack(val[0:8])[0]
                tx_count = struct_le_Q.unpack(val[8:16])[0]
                return BlockMetadata(block_size, tx_count)
            return None
