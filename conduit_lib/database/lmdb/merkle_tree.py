import logging
import os
import typing
from typing import Optional, List, Callable
from pathlib import Path

import cbor2
import lmdb
from bitcoinx import hash_to_hex_str

from conduit_lib.algorithms import get_mtree_node_counts_per_level, calc_depth
from conduit_lib.constants import PROFILING
from conduit_lib.database.ffdb.flat_file_db import FlatFileDb, DataLocation
from conduit_lib.database.lmdb.types import MerkleTree, MerkleTreeRow
from conduit_lib.types import MerkleTreeArrayLocation, TxMetadata, TxLocation, Slice

if typing.TYPE_CHECKING:
    from conduit_lib import LMDB_Database


MODULE_DIR = os.path.dirname(os.path.abspath(__file__))


from conduit_lib.algorithms import MTree


def tree_depth_for_mtree(mtree: MTree) -> int:
    return len(mtree) - 1


def _pack_list_to_concatenated_bytes(hashes: List[bytes]) -> bytearray:
    byte_array = bytearray()
    for _hash in hashes:
        byte_array += _hash
    return byte_array


class LmdbMerkleTree:

    logger = logging.getLogger("lmdb-merkle-tree")
    logger.setLevel(PROFILING)
    MTREE_DB = b"mtree_db"

    def __init__(self, db: 'LMDB_Database'):
        self.db = db

        merkle_trees_dir = Path(os.environ["MERKLE_TREES_DIR"])
        merkle_trees_lockfile = Path(os.environ['MERKLE_TREES_LOCKFILE'])
        self.ffdb = FlatFileDb(merkle_trees_dir, merkle_trees_lockfile)
        self.mtree_db = self.db.env.open_db(self.MTREE_DB)

    def _pack_mtree_to_array(self, mtree: MerkleTree) -> bytes:
        """Note: Each level has an even number of nodes (duplicate hashes appended as needed)"""
        mtree_array = bytearray()
        for level in reversed(range(len(mtree))):
            value = _pack_list_to_concatenated_bytes(mtree[level])
            mtree_array += value
        return mtree_array

    def _get_merkle_slice_for_level(self,
            mtree_array_loc: MerkleTreeArrayLocation,
            node_counts: list[int], level: int) -> Slice:
        """Given a flattened array of all levels of the merkle tree, give back
        the slice to get the desired level"""
        hash_length = 32
        to_subtract_from_end = 0
        for lvl in range(0, level):
            to_subtract_from_end += node_counts[lvl] * hash_length

        to_add_to_start = 0
        for lvl in range(calc_depth(node_counts[-1]) - 1, level, -1):
            to_add_to_start += node_counts[lvl] * hash_length

        len_mtree_array = mtree_array_loc.end_offset - mtree_array_loc.start_offset
        end_offset = len_mtree_array - to_subtract_from_end
        start_offset = to_add_to_start
        return Slice(start_offset, end_offset)

    def _get_merkle_tree_data(self, block_hash: bytes, slice: Optional[Slice]) -> Optional[bytes]:
        """If end_offset=0 then it goes to the end of the block"""
        data_location = self.get_data_location(block_hash)
        if not data_location:
            return None
        with self.ffdb:
            return self.ffdb.get(data_location, slice, lock_free_access=True)

    # -------------------- EXTERNAL API -------------------- #

    def get_tx_hash_by_loc(self, tx_loc: TxLocation, mtree_base_level: int) -> bytes:
        with self.db.env.begin(db=self.mtree_db) as txn:
            tx_hashes_bytes = self.get_mtree_row(tx_loc.block_hash, level=mtree_base_level)
            assert tx_hashes_bytes is not None, "If there is a TxLocation, there should be a " \
                                                "valid tx_hash"
            tx_hash = tx_hashes_bytes[tx_loc.tx_position*32:(tx_loc.tx_position+1)*32]
            return tx_hash

    def get_data_location(self,  block_hash: bytes) -> Optional[DataLocation]:
        with self.db.env.begin(db=self.mtree_db, buffers=False) as txn:
            mtree_location_bytes: bytes = txn.get(block_hash)
            if not mtree_location_bytes:
                self.logger.error(f"Merkle tree for block_hash: "
                                  f"{hash_to_hex_str(block_hash)} not found")
                return None
        read_path, start_offset_in_dat_file, end_offset_in_dat_file, base_node_count = cbor2.loads(
            mtree_location_bytes)
        return DataLocation(read_path, start_offset_in_dat_file, end_offset_in_dat_file)

    def get_mtree_node(self, block_hash: bytes, level: int, position: int, cursor: lmdb.Cursor) \
            -> bytes:
        """level zero is the merkle root node"""
        if cursor:
            val = bytes(cursor.get(block_hash))
        else:
            with self.db.env.begin(db=self.mtree_db) as txn:
                val = bytes(txn.get(block_hash))
        mtree_array_location: MerkleTreeArrayLocation = \
            MerkleTreeArrayLocation(*cbor2.loads(val))
        node_counts = get_mtree_node_counts_per_level(
            mtree_array_location.base_node_count)

        slice = self._get_merkle_slice_for_level(
            mtree_array_location, node_counts, level)
        node_slice = Slice(
            slice.start_offset + position*32,
            slice.start_offset + (position+1)*32
        )
        node_hash = self._get_merkle_tree_data(block_hash, node_slice)
        assert node_hash is not None
        assert len(node_hash) == 32
        return node_hash

    def get_mtree_row(self, block_hash: bytes, level: int,
            cursor: Optional[lmdb.Cursor]=None) -> Optional[bytes]:
        """level zero is the merkle root node"""
        if cursor:
            val = bytes(cursor.get(block_hash))
        else:
            with self.db.env.begin(db=self.mtree_db) as txn:
                val = bytes(txn.get(block_hash))

        mtree_array_location: MerkleTreeArrayLocation = \
            MerkleTreeArrayLocation(*cbor2.loads(val))
        node_counts = get_mtree_node_counts_per_level(
            mtree_array_location.base_node_count)
        slice = self._get_merkle_slice_for_level(mtree_array_location, node_counts, level)
        return self._get_merkle_tree_data(block_hash, slice)

    def get_merkle_branch(self, tx_metadata: TxMetadata) \
            -> Optional[tuple[list[str], str]]:
        block_metadata = self.db.get_block_metadata(tx_metadata.block_hash)
        assert block_metadata is not None, "Null checks should already have " \
                                           "been done in the caller"
        base_level = calc_depth(block_metadata.tx_count) - 1
        block_hash = tx_metadata.block_hash

        odd_node_count_for_level: Callable[[int], int] = lambda x: x & 1
        with self.db.env.begin(db=self.mtree_db, write=False, buffers=True) as txn:
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

                    # Asterix in place of "duplicated" hashes in TSC format (derivable by client)
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

    def put_merkle_trees(self, batched_merkle_trees: list[MerkleTreeRow]) -> None:
        """In the current design we store the entire txid set and all levels of the merkle tree)
        We need the full, ordered txid set for other queries anyway so there is little to be gained
        by not storing the interior hashes as well. """
        with self.db.env.begin(db=self.mtree_db, write=True, buffers=False) as txn:
            cursor = txn.cursor()
            with self.ffdb:
                for block_hash, mtree, tx_count in batched_merkle_trees:
                    tree_depth = tree_depth_for_mtree(mtree)
                    # base_node_count is tx_count + 1 if the tx count is an odd number
                    base_node_count = len(mtree[tree_depth])
                    assert base_node_count == tx_count if not (tx_count & 1) else tx_count + 1

                    # Write byte array to flat files (concatenated, packed, merkle trees)
                    mtree_array = self._pack_mtree_to_array(mtree)

                    counts_for_all_levels = get_mtree_node_counts_per_level(base_node_count)
                    assert len(mtree_array) == sum([x * 32 for x in counts_for_all_levels])

                    data_location = self.ffdb.put(mtree_array)
                    mtree_array_location = MerkleTreeArrayLocation(
                        write_path=data_location.file_path,
                        start_offset=data_location.start_offset,
                        end_offset=data_location.end_offset,
                        base_node_count=base_node_count
                    )
                    cursor.put(block_hash, cbor2.dumps(mtree_array_location))
