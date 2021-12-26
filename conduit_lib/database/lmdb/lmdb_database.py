import array
import functools
import io
import logging
import os
import sys
import time
import typing
from io import BytesIO
from math import ceil, log
from pathlib import Path
from struct import Struct
from typing import List, Tuple, Optional, IO, Set

import bitcoinx
import cbor2
import filelock
import lmdb
from bitcoinx import hash_to_hex_str
from bitcoinx.packing import struct_le_Q
from filelock import FileLock

from conduit_lib.algorithms import calc_depth, get_mtree_node_counts_per_level
from conduit_lib.types import TxLocation, BlockMetadata, TxMetadata, MerkleTreeArrayLocation, \
    ChainHashes, TxOffsetsArrayLocation

try:
    from ...constants import PROFILING, MAX_DAT_FILE_SIZE, SIZE_UINT64_T
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

    LMDB_DATABASE_PATH_DEFAULT = Path(MODULE_DIR).parent.parent.parent.parent / 'lmdb_data'
    LMDB_DATABASE_PATH: str = os.environ.get("LMDB_DATABASE_PATH", str(LMDB_DATABASE_PATH_DEFAULT))

    RAW_BLOCKS_DIR_DEFAULT = Path(MODULE_DIR).parent.parent.parent / 'raw_blocks'
    RAW_BLOCKS_DIR = os.environ.get("RAW_BLOCKS_DIR", str(RAW_BLOCKS_DIR_DEFAULT))

    MERKLE_TREES_DIR_DEFAULT = Path(MODULE_DIR).parent.parent.parent / 'merkle_trees'
    MERKLE_TREES_DIR = os.environ.get("MERKLE_TREES_DIR", str(MERKLE_TREES_DIR_DEFAULT))

    TX_OFFSETS_DIR_DEFAULT = Path(MODULE_DIR).parent.parent.parent / 'tx_offsets'
    TX_OFFSETS_DIR = os.environ.get("TX_OFFSETS_DIR", str(TX_OFFSETS_DIR_DEFAULT))

    def __init__(self, storage_path: Optional[str]=None):
        self.logger = logging.getLogger("lmdb-database")
        self.logger.setLevel(PROFILING)

        if not storage_path:
            storage_path = self.LMDB_DATABASE_PATH

        if not Path(self.RAW_BLOCKS_DIR).exists():
            os.makedirs(Path(self.RAW_BLOCKS_DIR), exist_ok=True)

        if not Path(self.MERKLE_TREES_DIR).exists():
            os.makedirs(Path(self.MERKLE_TREES_DIR), exist_ok=True)

        if not Path(self.TX_OFFSETS_DIR).exists():
            os.makedirs(Path(self.TX_OFFSETS_DIR), exist_ok=True)

        if not Path(storage_path).exists():
            os.makedirs(Path(storage_path), exist_ok=True)

        self.env: Optional[lmdb.Environment] = None
        self.blocks_db = None
        self.block_nums_db = None
        self.mtree_db = None
        self.tx_offsets_db = None
        self.block_metadata_db = None
        self._opened = False

        if sys.platform == 'linux':
            self._map_size = pow(1024, 4) * 32  # 64 terabytes
        else:
            # on windows there is a bug where this requires the disc space to be free
            # on linux can set this to a very large number (e.g. 10 terabytes)
            # windows is for development use only...
            self._map_size = pow(1024, 3) * 20
        self._storage_path = storage_path
        self.open()
        self.logger.debug(f"opened LMDB database at {storage_path}")

        self._last_dat_file_num_blocks = 0
        self._last_dat_file_num_merkle_trees = 0
        self._last_dat_file_num_tx_offsets = 0
        # This will do the initial scan and cache the correct _last_dat_file_num
        self.next_write_path = \
            self._get_next_write_path_for_blocks()
        self.next_write_path_merkle, self.next_merkle_flock = \
            self._get_next_write_path_for_merkle_trees()
        self.next_write_path_tx_offsets, self.next_tx_offset_flock = \
            self._get_next_write_path_for_tx_offsets()
        # self.logger.debug(f"Next write path blocks: {self.next_write_path}")
        # self.logger.debug(f"Next write path merkle: {self.next_write_path_merkle}")

    def open(self):
        self.env = lmdb.open(self._storage_path, max_dbs=5, readonly=False,
            readahead=False, sync=False, map_size=self._map_size)
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

    def _read_slice(self, read_path: str, start_offset: int, end_offset: int,
            start_offset_in_dat_file: int, end_offset_in_dat_file: int):
        with open(read_path, 'rb') as f:
            f.seek(start_offset_in_dat_file + start_offset)
            if end_offset == 0:
                len_of_block = end_offset_in_dat_file - start_offset_in_dat_file
                len_of_slice = len_of_block - start_offset
                return f.read(len_of_slice)
            else:
                len_of_slice = end_offset - start_offset
                return f.read(len_of_slice)

    def get_block_num(self, block_hash: bytes) -> int:
        with self.env.begin(db=self.block_nums_db) as txn:
            result = txn.get(block_hash)
            if result:
                return struct_be_I.unpack(result)[0]
            raise EntryNotFound(f"Block num for block_hash: "
                                f"{bitcoinx.hash_to_hex_str(block_hash)} not found")

    def get_block(self, block_num: int, start_offset: int=0,
            end_offset: int=0, buffers=False) -> bytes:
        """If end_offset=0 then it goes to the end of the block"""
        with self.env.begin(db=self.blocks_db, buffers=False) as txn:
            val: bytes = txn.get(struct_be_I.pack(block_num))
            if not val:
                raise EntryNotFound(f"Block for block_num: {block_num} not found")
            read_path, start_offset_in_dat_file, end_offset_in_dat_file = cbor2.loads(val)

        return self._read_slice(read_path, start_offset, end_offset, start_offset_in_dat_file,
            end_offset_in_dat_file)

    def get_merkle_tree_data(self, block_hash: bytes, start_offset: int=0,
            end_offset: int=0, buffers=False) -> bytes:
        """If end_offset=0 then it goes to the end of the block"""
        with self.env.begin(db=self.mtree_db, buffers=False) as txn:
            val: bytes = txn.get(block_hash)
            if not val:
                raise EntryNotFound(f"Merkle tree for block_hash: {block_hash} not found")
            read_path, start_offset_in_dat_file, end_offset_in_dat_file, base_node_count = cbor2.loads(val)

        with open(read_path, 'rb') as f:
            f.seek(start_offset)
            if end_offset == 0:
                len_of_block = end_offset_in_dat_file - start_offset_in_dat_file
                len_of_slice = len_of_block - start_offset
                return f.read(len_of_slice)
            else:
                len_of_slice = end_offset - start_offset
                return f.read(len_of_slice)

    @functools.lru_cache(maxsize=256)
    def get_tx_hash_by_loc(self, tx_loc: TxLocation, mtree_base_level: int) -> bytes:
        """Todo: Merkle Tree rows need to be stored in flat files the same way that blocks are"""
        with self.env.begin(db=self.mtree_db) as txn:
            tx_hashes_bytes = self.get_mtree_row(tx_loc.block_hash, level=mtree_base_level)
            tx_hash = tx_hashes_bytes[tx_loc.tx_position*32:(tx_loc.tx_position+1)*32]
            return tx_hash

    def get_single_tx_offset(self, tx_loc: TxLocation) -> Tuple[int, int]:
        """If end_offset=0 then it goes to the end of the block"""
        self.logger.debug(f"get_single_tx_offset: block_hash={bitcoinx.hash_to_hex_str(tx_loc.block_hash)}")
        with self.env.begin(db=self.tx_offsets_db, write=False, buffers=True) as txn:
            val: bytes = txn.get(tx_loc.block_hash)
            if not val:
                raise EntryNotFound(f"Tx offsets for block_hash: {tx_loc.block_hash} not found")
            read_path, start_offset_in_dat_file, end_offset_in_dat_file = cbor2.loads(val)

        # Starting offset of tx
        start_offset = start_offset_in_dat_file + (tx_loc.tx_position * SIZE_UINT64_T)
        with open(read_path, 'rb') as f:
            f.seek(start_offset)
            tx_start_offset = bitcoinx.unpack_le_uint64(f.read(8))[0]

        # End offset of tx
        # If it's the last in the block there won't be an offset hence get_block_metadata()
        start_offset = start_offset_in_dat_file + (tx_loc.tx_position+1)*SIZE_UINT64_T
        if start_offset == end_offset_in_dat_file:
            tx_end_offset = self.get_block_metadata(tx_loc.block_hash).block_size
        else:
            with open(read_path, 'rb') as f:
                f.seek(start_offset)
                tx_end_offset = bitcoinx.unpack_le_uint64(f.read(8))[0]

        return tx_start_offset, tx_end_offset

    @functools.lru_cache(maxsize=256)
    def get_rawtx_by_loc(self, tx_loc: TxLocation) -> bytes:
        tx_start_offset, tx_end_offset = self.get_single_tx_offset(tx_loc)
        rawtx = bytes(self.get_block(tx_loc.block_num, tx_start_offset, tx_end_offset))
        return rawtx

    def _get_merkle_file_offsets_for_level(self, mtree_array_loc: MerkleTreeArrayLocation,
            node_counts: list[int], level: int):
        hash_length = 32
        to_subtract_from_end = 0
        for lvl in range(0, level):
            # self.logger.debug(
            # f"node_counts[lvl] * hash_length={node_counts[lvl] * hash_length}; level={lvl}")
            to_subtract_from_end += node_counts[lvl] * hash_length

        to_add_to_start = 0
        # self.logger.debug(f"calc_depth(node_counts[-1]) - 1={calc_depth(node_counts[-1]) - 1}")
        for lvl in range(calc_depth(node_counts[-1]) - 1, level, -1):
            # self.logger.debug(f"to_add_to_start:
            # node_counts[lvl] * hash_length={node_counts[lvl] * hash_length}; level={lvl}")
            to_add_to_start += node_counts[lvl] * hash_length

        end_offset = mtree_array_loc.end_offset - to_subtract_from_end
        start_offset = mtree_array_loc.start_offset + to_add_to_start
        # self.logger.debug(f"mtree_array_loc={mtree_array_loc}; node_counts={node_counts}; "
        #                   f"level={level} start_offset={start_offset}; end_offset={end_offset}; "
        #                   f"to_subtract_from_end={to_subtract_from_end}; to_add_to_start={to_add_to_start}")
        return start_offset, end_offset

    def get_mtree_row(self, blk_hash: bytes, level: int, cursor: Optional[lmdb.Cursor]=None) -> bytes:
        if cursor:
            val = bytes(cursor.get(blk_hash))
        else:
            with self.env.begin(db=self.mtree_db) as txn:
                val = bytes(txn.get(blk_hash))

        mtree_array_location: MerkleTreeArrayLocation = MerkleTreeArrayLocation(*cbor2.loads(val))
        node_counts = get_mtree_node_counts_per_level(mtree_array_location.base_node_count)
        start, end = self._get_merkle_file_offsets_for_level(mtree_array_location, node_counts, level)
        mtree_row = self.get_merkle_tree_data(blk_hash, start_offset=start, end_offset=end)
        return mtree_row

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

    def _get_next_write_path_for_merkle_trees(self) -> [Path, FileLock]:
        # self.logger.debug(f"Getting next write path for merkle trees...")
        padded_str_num = str(self._last_dat_file_num_merkle_trees).zfill(8)
        filename = f"blk_{padded_str_num}.dat"
        write_path = Path(self.MERKLE_TREES_DIR) / filename

        while os.path.exists(write_path):
            self._last_dat_file_num_merkle_trees += 1
            padded_str_num = str(self._last_dat_file_num_merkle_trees).zfill(8)
            filename = f"blk_{padded_str_num}.dat"
            write_path = Path(self.MERKLE_TREES_DIR) / filename

        lock_path = filename

        flock = FileLock(lock_path, timeout=10)  # pylint: disable=E0110
        return write_path, flock

    def _get_next_write_path_for_tx_offsets(self) -> [Path, FileLock]:
        self.logger.debug(f"Getting next write path for tx offsets...")
        padded_str_num = str(self._last_dat_file_num_tx_offsets).zfill(8)
        filename = f"blk_{padded_str_num}.dat"
        write_path = Path(self.TX_OFFSETS_DIR) / filename

        while os.path.exists(write_path):
            self._last_dat_file_num_tx_offsets += 1
            padded_str_num = str(self._last_dat_file_num_tx_offsets).zfill(8)
            filename = f"blk_{padded_str_num}.dat"
            write_path = Path(self.TX_OFFSETS_DIR) / filename

        lock_path = filename

        flock = FileLock(lock_path, timeout=10)  # pylint: disable=E0110
        return write_path, flock

    def _get_next_write_path_for_blocks(self) -> str:
        # self.logger.debug(f"Getting next write path for raw blocks...")
        padded_str_num = str(self._last_dat_file_num_blocks).zfill(8)
        filename = f"blk_{padded_str_num}.dat"
        write_path = Path(self.RAW_BLOCKS_DIR) / filename

        while os.path.exists(write_path):
            self._last_dat_file_num_blocks += 1
            padded_str_num = str(self._last_dat_file_num_blocks).zfill(8)
            filename = f"blk_{padded_str_num}.dat"
            write_path = Path(self.RAW_BLOCKS_DIR) / filename

        return write_path

    def write_blocks_to_file(self, buffer: bytes, write_path: str):
        with open(write_path, 'wb') as f:
            f.write(buffer)

    def append_to_file(self, buffer: bytes, file: IO) -> int:
        file.write(buffer)
        return file.tell()

    def put_blocks(self, batched_blocks: List[Tuple[bytes, int, int]], shared_mem_buffer: memoryview):
        """write blocks in append-only mode to disc. The whole batch is written to a single
        file."""
        try:
            # self.logger.debug(f"Got batched_blocks: {len(batched_blocks)}")
            t0 = time.time()

            last_block_num = self.get_last_block_num()
            next_block_num = last_block_num + 1

            tx = self.env.begin(write=True, buffers=False)

            block_nums = range(next_block_num, (len(batched_blocks) + next_block_num))
            # self.logger.debug(f"block_nums={block_nums}")
            start_offset = 0
            raw_blocks_arr = bytearray()
            write_path = self._get_next_write_path_for_blocks()

            for block_num, block_row in zip(block_nums, batched_blocks):
                blk_hash, blk_start_pos, blk_end_pos = block_row
                raw_block = shared_mem_buffer[blk_start_pos: blk_end_pos].tobytes()
                stream = BytesIO(raw_block[80:89])
                tx_count = bitcoinx.read_varint(stream.read)
                len_block = blk_end_pos - blk_start_pos

                raw_blocks_arr += raw_block
                write_path = Path(write_path).as_posix()
                end_offset = start_offset + len_block

                key = struct_be_I.pack(block_num)
                val = cbor2.dumps((write_path, start_offset, end_offset))
                # self.logger.debug(f"Writing block num: {block_num} to "
                #                   f"write_path: {write_path}; "
                #                   f"start_offset: {start_offset}; "
                #                   f"end_offset: {end_offset}")
                # self.logger.debug(f"Putting key: {struct_be_I.unpack(key)[0]}; val: {val}")

                tx.put(key, val, db=self.blocks_db, append=True, overwrite=False)
                tx.put(blk_hash, struct_be_I.pack(block_num), db=self.block_nums_db, overwrite=False)
                tx.put(blk_hash, struct_le_Q.pack(len(raw_block)) + struct_le_Q.pack(tx_count),
                    db=self.block_metadata_db)

                start_offset += len_block

            self.write_blocks_to_file(raw_blocks_arr, write_path)

            tx.commit()

            t1 = time.time() - t0
            if len(batched_blocks) > 0:
                self.logger.log(PROFILING,
                    f"elapsed time for {len(batched_blocks)} raw_blocks took {t1} seconds")
        except Exception as e:
            self.logger.exception(e)

    def get_mtree_node(self, block_hash: bytes, level: int, position: int, cursor: lmdb.Cursor) \
            -> bytes:
        # self.logger.debug(f"get_mtree_node called for block_hash:
        # {hash_to_hex_str(block_hash)} ({block_hash}), level: {level}, position: {position}")
        array = self.get_mtree_row(block_hash, level, cursor)
        assert array is not None
        node_hash = array[position*32:(position+1)*32]
        assert node_hash is not None
        assert len(node_hash) == 32, f"this is the array: {array}"
        return bytes(node_hash)  # convert from memory view if needed

    def merkle_branch_length(self, tx_count: int):
        '''Return the length of a merkle branch given the number of hashes.'''
        return ceil(log(tx_count, 2))

    def get_merkle_branch(self, tx_metadata: TxMetadata) -> tuple[list[str], str]:
        block_metadata = self.get_block_metadata(tx_metadata.block_hash)
        base_level = calc_depth(block_metadata.tx_count) - 1
        block_hash = tx_metadata.block_hash

        odd_node_count_for_level = lambda x: current_level_node_count & 1
        with self.env.begin(db=self.mtree_db, write=False, buffers=True) as txn:
            cur = txn.cursor()
            index = tx_metadata.tx_position

            branch = []
            current_level_node_count = block_metadata.tx_count

            # level = 0 is the merkle root. Increasing levels move toward the base.
            for level in reversed(range(0, base_level + 1)):
                pair_index = index ^ 1
                if level == 0:
                    merkle_root = self.get_mtree_node(block_hash, level, 0, cur)

                    break

                if odd_node_count_for_level(current_level_node_count):
                    current_level_node_count += 1  # to account for the duplicate hash

                    # Asterix used in place of "duplicated" hashes in TSC format (derivable by client)
                    is_last_node_in_level = (index ^ 1 == current_level_node_count - 1)
                    if is_last_node_in_level:
                        branch.append("*")
                    else:
                        branch_node = self.get_mtree_node(block_hash, level, pair_index, cur)
                        branch.append(hash_to_hex_str(branch_node))
                else:
                    branch_node = self.get_mtree_node(block_hash, level, pair_index, cur)
                    branch.append(hash_to_hex_str(branch_node))
                index >>= 1
                current_level_node_count = current_level_node_count // 2
            return branch, hash_to_hex_str(merkle_root)

    def _pack_list_to_concatenated_bytes(self, hashes):
        # Todo - see algorithms.py for future optimization plans
        byte_array = bytearray()
        for _hash in hashes:
            byte_array += _hash
        return byte_array

    def _pack_mtree_to_array(self, mtree: dict):
        """Note: Each level has an even number of nodes (duplicate hashes appended as needed)"""
        mtree_array = bytearray()
        for level in reversed(range(len(mtree))):
            value = self._pack_list_to_concatenated_bytes(mtree[level])
            # self.logger.debug(f"_pack_mtree_to_array: level: {level}; mtree: {mtree}; value: {value}")
            mtree_array += value
        return mtree_array

    def _open_next_merkle_tree_file(self, file: typing.IO) -> [typing.IO, str]:
        write_path, flock = self._get_next_write_path_for_merkle_trees()
        flock.acquire()
        file = open(write_path, 'ab')
        return file, write_path, flock

    def _open_initial_merkle_tree_file(self) -> [typing.IO, int, str]:
        """Note: Leaves an open file - must remember to close"""
        # cached so that subsequent batches can append to the same file
        try:
            write_path = self.next_write_path_merkle
            flock = self.next_merkle_flock

            flock.acquire()

            file = open(str(self.next_write_path_merkle), 'ab')
            file.seek(0, io.SEEK_END)
            file_size = file.tell()
            if file_size < MAX_DAT_FILE_SIZE:
                return file, file_size, write_path, flock
            else:
                file.close()
                flock.release()
                write_path, other_flock = self._get_next_write_path_for_merkle_trees()
                other_flock.acquire()
                file = open(write_path, 'ab')
                return file, file_size, write_path, other_flock
        except filelock.Timeout:
            self.logger.exception("Filelock timed out")

    def put_merkle_trees(self, batched_merkle_trees: list[tuple[bytes, dict, int]]):
        """In the current design we store the entire txid set and all levels of the merkle tree)
        We need the full, ordered txid set for other queries anyway so there is little to be gained
        by not storing the interior hashes as well. """
        file, file_size, write_path, acquired_flock = self._open_initial_merkle_tree_file()
        start_offset = file_size
        try:
            with self.env.begin(db=self.mtree_db, write=True, buffers=False) as txn:
                cursor = txn.cursor()
                for block_hash, mtree, tx_count in batched_merkle_trees:
                    tree_depth = len(mtree) - 1
                    # base_node_count is not the same as tx_count depending on an odd/even tx count
                    base_node_count = len(mtree[tree_depth])
                    assert base_node_count == tx_count if not (tx_count & 1) else tx_count + 1

                    # Write byte array to flat files (concatenated, packed, merkle trees)
                    mtree_array = self._pack_mtree_to_array(mtree)

                    counts_for_all_levels = get_mtree_node_counts_per_level(base_node_count)
                    assert len(mtree_array) == sum([x * 32 for x in counts_for_all_levels])

                    new_file_size = self.append_to_file(mtree_array, file)
                    end_offset = new_file_size
                    if new_file_size > MAX_DAT_FILE_SIZE:
                        file.close()
                        acquired_flock.release()
                        file, write_path, acquired_flock = self._open_next_merkle_tree_file(file)

                    # LMDB only stores the mapping of hash -> location
                    mtree_array_location = MerkleTreeArrayLocation(str(write_path), start_offset,
                        end_offset, base_node_count)
                    # self.logger.debug(f"writing merkle tree array (
                    # {hash_to_hex_str(block_hash)}): {mtree_array_location}")
                    cursor.put(block_hash, cbor2.dumps(mtree_array_location))

                    start_offset += len(mtree_array)
        finally:
            file.close()
            acquired_flock.release()

    def _open_next_tx_offsets_file(self, file: typing.IO) -> [typing.IO, str]:
        write_path, flock = self._get_next_write_path_for_tx_offsets()
        flock.acquire()
        file = open(write_path, 'ab')
        return file, write_path, flock

    def _open_initial_tx_offsets_file(self) -> [typing.IO, int, str]:
        """Note: Leaves an open file - must remember to close"""
        # cached so that subsequent batches can append to the same file
        try:
            write_path = self.next_write_path_tx_offsets
            flock = self.next_tx_offset_flock

            flock.acquire()

            file = open(str(self.next_write_path_tx_offsets), 'ab')
            file.seek(0, io.SEEK_END)
            file_size = file.tell()
            if file_size < MAX_DAT_FILE_SIZE:
                return file, file_size, write_path, flock
            else:
                file.close()
                flock.release()
                write_path, other_flock = self._get_next_write_path_for_tx_offsets()
                other_flock.acquire()
                file = open(write_path, 'ab')
                return file, file_size, write_path, other_flock
        except filelock.Timeout:
            self.logger.exception("Filelock timed out")

    def put_tx_offsets(self, batched_tx_offsets: list[tuple[bytes, array.ArrayType]]):
        """Maybe should write this out to flat files as for mtrees and blocks..."""
        file, file_size, write_path, acquired_flock = self._open_initial_tx_offsets_file()
        try:
            start_offset = file_size
            with self.env.begin(db=self.tx_offsets_db, write=True, buffers=False) as txn:
                cursor = txn.cursor()
                for block_hash, tx_offsets in batched_tx_offsets:
                    new_file_size = self.append_to_file(tx_offsets.tobytes(), file)
                    end_offset = new_file_size
                    if new_file_size > MAX_DAT_FILE_SIZE:
                        file.close()
                        acquired_flock.release()
                        file, write_path, flock = self._open_next_tx_offsets_file(file)

                    # LMDB only stores the mapping of hash -> location
                    tx_offsets_location = TxOffsetsArrayLocation(str(write_path), start_offset,
                        end_offset)
                    # self.logger.debug(f"writing tx offsets array ({hash_to_hex_str(block_hash)}): "
                    #                   f"{tx_offsets_location}")
                    cursor.put(block_hash, cbor2.dumps(tx_offsets_location))

                    start_offset += len(tx_offsets*SIZE_UINT64_T)
                    # self.logger.debug(f"block_hash={bitcoinx.hash_to_hex_str(block_hash)};"
                    #                   f"tx_offsets={tx_offsets}")
        finally:
            file.close()
            acquired_flock.release()

    def get_tx_offsets(self, block_hash: bytes) -> bytes:
        """If end_offset=0 then it goes to the end of the block"""
        with self.env.begin(db=self.tx_offsets_db, write=False, buffers=True) as txn:
            val: bytes = txn.get(block_hash)
            if not val:
                raise EntryNotFound(f"Tx offsets for block_hash: {block_hash} not found")
            read_path, start_offset_in_dat_file, end_offset_in_dat_file = cbor2.loads(val)

        return self._read_slice(read_path, 0, 0, start_offset_in_dat_file, end_offset_in_dat_file)

    def get_block_metadata(self, block_hash: bytes) -> BlockMetadata:
        """Namely size in bytes but could later include things like compression dictionary id and
        maybe interesting things like MinerID"""
        with self.env.begin(db=self.block_metadata_db, write=False, buffers=True) as txn:
            val = txn.get(block_hash)
            if val:
                # Note: This can return zero - which is a "falsey" type value. Take care
                val = bytes(val)
                block_size = struct_le_Q.unpack(val[0:8])[0]
                tx_count = struct_le_Q.unpack(val[8:16])[0]
                return BlockMetadata(block_size, tx_count)

    def _make_reorg_tx_set(self, chain: ChainHashes):
        chain_tx_hashes = set()
        for block_hash in chain:
            block_metadata = self.get_block_metadata(block_hash)
            base_level = calc_depth(block_metadata.tx_count) - 1
            stream = BytesIO(self.get_mtree_row(block_hash, level=base_level))
            for i in range(block_metadata.tx_count):
                if i == 0:  # skip coinbase txs because they can never be in the mempool
                    _ = stream.read(32)
                    continue
                tx_hash = stream.read(32)
                chain_tx_hashes.add(tx_hash)
        return chain_tx_hashes

    def get_reorg_differential(self, old_chain: ChainHashes, new_chain: ChainHashes) \
            -> Tuple[Set[bytes], Set[bytes], Set[bytes]]:
        # Todo - technically if there was a second rapid-fire reorg covering the same block hashes
        #  we'd need to check all N chains against each other. Would need to query the old_chain
        #  heights in MySQL to see if there are any other block_hashes to check at each height
        #  would also need to return a 3rd return type for this and have a segregated temp table
        #  (because the mempool diff only needs the prev. longest chain and doesn't care about
        #  older orphans).
        """This query basically wants to find out the ** differential ** in terms of tx hashes
         between the orphaned chain of blocks vs the new reorging longest chain.
         It should go without saying that we only care about the block hashes going back to
         the common parent height.

         We want two sets:
         1) What tx hashes must be put back to the current mempool
         2) What tx hashes must be removed from the current mempool
         """
        # NOTE: This could in theory use a lot of memory but the reorg would have to be very
        # deep to cause problems. It wouldn't be too hard to run the same check in bite sized
        # chunks to avoid too much memory allocation but I don't want to spend the time on it.
        old_chain_tx_hashes = self._make_reorg_tx_set(old_chain)
        new_chain_tx_hashes = self._make_reorg_tx_set(new_chain)
        additions_to_mempool = old_chain_tx_hashes - new_chain_tx_hashes

        removals_from_mempool = new_chain_tx_hashes - old_chain_tx_hashes
        return removals_from_mempool, additions_to_mempool, old_chain_tx_hashes

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
