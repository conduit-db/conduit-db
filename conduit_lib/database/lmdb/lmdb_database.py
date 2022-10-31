from __future__ import annotations

import asyncio
import logging
import os
import threading
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
from pathlib import Path
import lmdb
import typing


from bitcoinx import hex_str_to_hash, double_sha256, hash_to_hex_str

from conduit_lib.database.lmdb.merkle_tree import LmdbMerkleTree
from .blocks import LmdbBlocks
from .tx_offsets import LmdbTxOffsets
from ...bitcoin_p2p_types import BlockDataMsg

if typing.TYPE_CHECKING:
    import array

from conduit_lib.algorithms import calc_depth
from conduit_lib.database.lmdb.types import MerkleTreeRow
from conduit_lib.types import TxLocation, BlockMetadata, TxMetadata, ChainHashes, Slice, \
    DataLocation

try:
    from ...constants import PROFILING
except ImportError:
    from conduit_lib.constants import PROFILING

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))


class LMDB_Database:
    """Simple interface to LMDB - This interface must be used at all times for thread-safety"""

    def __init__(self, storage_path: str | None=None, lock: bool=True) -> None:
        self.logger = logging.getLogger("lmdb-database")
        self.logger.setLevel(PROFILING)

        lmdb_database_dir: str = os.environ["LMDB_DATABASE_DIR"]

        if not storage_path:
            storage_path = lmdb_database_dir

        if not Path(storage_path).exists():
            os.makedirs(Path(storage_path), exist_ok=True)

        self._map_size = pow(1024, 3) * 5
        self._storage_path = storage_path
        self.env = lmdb.open(self._storage_path, max_dbs=5, readahead=False, sync=False,
            map_size=self._map_size, lock=lock)
        self._opened = True

        self.blocks = LmdbBlocks(self)
        self.merkle_tree = LmdbMerkleTree(self)
        self.tx_offsets = LmdbTxOffsets(self)
        self.logger.debug(f"Opened LMDB database at {storage_path}")

        # Thread-safety if the LMDB_Database instance is shared across multiple threads of the
        # same Process.
        self.global_lock = threading.RLock()

    def close(self) -> None:
        with self.global_lock:
            if not self._opened:
                return

            self.env.close()
            self.env = None
            self._db1 = None
            self._db2 = None
            self._opened = False

    def _make_reorg_tx_set(self, chain: ChainHashes) -> set[bytes]:
        chain_tx_hashes = set()
        for block_hash in chain:
            block_metadata = self.get_block_metadata(block_hash)
            assert block_metadata is not None, \
                "all necessary block metadata should always be available for reorg handling"
            base_level = calc_depth(block_metadata.tx_count) - 1
            mtree_row = self.get_mtree_row(block_hash, level=base_level)
            assert mtree_row is not None
            stream = BytesIO(mtree_row)
            for i in range(block_metadata.tx_count):
                if i == 0:  # skip coinbase txs because they can never be in the mempool
                    _ = stream.read(32)
                    continue
                tx_hash = stream.read(32)
                chain_tx_hashes.add(tx_hash)
        return chain_tx_hashes

    # -------------------- EXTERNAL API  -------------------- #

    def sync(self) -> None:
        with self.global_lock:
            self.env.sync(True)

    def get_tx_hash_by_loc(self, tx_loc: TxLocation, mtree_base_level: int) -> bytes:
        with self.global_lock:
            return self.merkle_tree.get_tx_hash_by_loc(tx_loc, mtree_base_level)

    def get_mtree_node(self, block_hash: bytes, level: int, position: int) -> bytes:
        with self.global_lock:
            with self.env.begin(db=self.merkle_tree.mtree_db, write=False, buffers=False) as txn:
                cursor = txn.cursor()
                return self.merkle_tree.get_mtree_node(block_hash, level, position, cursor)

    def get_mtree_row(self, blk_hash: bytes, level: int, cursor: lmdb.Cursor | None=None) \
            -> bytes | None:
        with self.global_lock:
            return self.merkle_tree.get_mtree_row(blk_hash, level, cursor)

    def get_merkle_branch(self, tx_metadata: TxMetadata) -> tuple[list[str], str] | None:
        with self.global_lock:
            return self.merkle_tree.get_merkle_branch(tx_metadata)

    def put_merkle_trees(self, batched_merkle_trees: list[MerkleTreeRow]) -> None:
        with self.global_lock:
            self.merkle_tree.put_merkle_trees(batched_merkle_trees)

    def get_rawtx_by_loc(self, tx_loc: TxLocation) -> bytes | None:
        with self.global_lock:
            return self.tx_offsets.get_rawtx_by_loc(tx_loc)

    async def get_rawtx_by_loc_async(self, executor: ThreadPoolExecutor, tx_loc: TxLocation) \
            -> bytes | None:
        return await asyncio.get_running_loop().run_in_executor(executor,
            self.get_rawtx_by_loc, tx_loc)

    def get_single_tx_slice(self, tx_loc: TxLocation) -> Slice | None:
        with self.global_lock:
            return self.tx_offsets._get_single_tx_slice(tx_loc)

    def get_tx_offsets(self, block_hash: bytes) -> bytes | None:
        with self.global_lock:
            return self.tx_offsets.get_tx_offsets(block_hash)

    def put_tx_offsets(self, batched_tx_offsets: list[tuple[bytes, 'array.ArrayType[int]']]) \
            -> None:
        with self.global_lock:
            return self.tx_offsets.put_tx_offsets(batched_tx_offsets)

    def get_block_num(self, block_hash: bytes) -> int | None:
        with self.global_lock:
            return self.blocks.get_block_num(block_hash)

    def check_block(self, block_hash: bytes) -> None:
        if block_hash == hex_str_to_hash(os.environ["GENESIS_BLOCK_HASH"]):
            return None

        block_num = self.get_block_num(block_hash)

        with self.global_lock:
            assert block_num is not None
            data_location = self.blocks.get_data_location(block_num)
            assert data_location is not None
            file_size = os.path.getsize(data_location.file_path)
            assert data_location.end_offset <= file_size, \
                f"There is data missing from the data file for {hex_str_to_hash(block_hash)}"

        # Ensure that the merkle tree table and tx offsets table are aligned to return the
        # correct coinbase transaction from the raw block data.
        block_metadata = self.get_block_metadata(block_hash)
        assert block_metadata is not None
        base_level = calc_depth(block_metadata.tx_count) - 1
        coinbase_tx_hash = self.get_mtree_node(block_hash, level=base_level, position=0)
        coinbase_tx_location = TxLocation(block_hash, block_num, tx_position=0)
        rawtx = self.get_rawtx_by_loc(coinbase_tx_location)
        assert double_sha256(rawtx) == coinbase_tx_hash

    def try_purge_block_data(self, block_hash: bytes) -> None:
        """Scrubs all records for this block hash.

        raises `AssertionError` if any of the checks fail"""
        try:
            self.global_lock.acquire()
            block_num = self.get_block_num(block_hash)
            assert block_num is not None
            with self.env.begin(write=True, buffers=False) as txn:
                data_location_block = self.blocks.get_data_location(block_num)
                assert data_location_block is not None
                txn.delete(block_num, db=self.blocks.blocks_db)
                txn.delete(block_hash, db=self.blocks.block_nums_db)
                txn.delete(block_hash, db=self.blocks.block_metadata_db)

                data_location_tx_offsets = self.tx_offsets.get_data_location(block_hash)
                assert data_location_tx_offsets is not None
                txn.delete(block_hash, db=self.tx_offsets.tx_offsets_db)

                data_location_merkle_tree = self.merkle_tree.get_data_location(block_hash)
                assert data_location_merkle_tree is not None
                txn.delete(block_hash, db=self.merkle_tree.mtree_db)

            # These calls are irreversible. There is no rollback
            self.blocks.ffdb.delete_file(Path(data_location_block.file_path))
            self.tx_offsets.ffdb.delete_file(Path(data_location_tx_offsets.file_path))
            self.merkle_tree.ffdb.delete_file(Path(data_location_merkle_tree.file_path))

        # NOTE If this situation is ever encountered. E.g. maybe some tables have data but the
        # metadata needed to do a full purge is incomplete. The only realistic solution is start
        # syncing the blockchain from the last good height and overwrite the records. There will
        # be some wasted / dead disc usage that will never be reclaimed but at least the server
        # can remain operational.
        except Exception:
            self.logger.exception(f"Failed to complete full purge of block data for block hash: "
                                  f"{hash_to_hex_str(block_hash)}")
        finally:
            self.global_lock.release()

    def get_block(self, block_num: int, slice: Slice | None=None) -> bytes | None:
        with self.global_lock:
            return self.blocks.get_block(block_num, slice)

    def get_block_metadata(self, block_hash: bytes) -> BlockMetadata | None:
        with self.global_lock:
            return self.blocks.get_block_metadata(block_hash)

    def put_blocks(self, small_batched_blocks: list[bytes]) -> None:
        with self.global_lock:
            return self.blocks.put_blocks(small_batched_blocks)

    def put_big_block(self, big_block: tuple[bytes, DataLocation, int]) -> None:
        with self.global_lock:
            return self.blocks.put_big_block(big_block)

    def get_reorg_differential(self, old_chain: ChainHashes, new_chain: ChainHashes) \
            -> tuple[set[bytes], set[bytes], set[bytes]]:
        """This query basically wants to find out the ** differential ** in terms of tx hashes
         between the orphaned chain of blocks vs the new reorging longest chain.
         It should go without saying that we only care about the block hashes going back to
         the common parent height.

         We want two sets:
         1) What tx hashes must be put back to the current mempool
         2) What tx hashes must be removed from the current mempool

         NOTE technically if there was a second, rapid-fire reorg covering overlapping
         transactions, the mempool diff only needs the prev. longest chain and doesn't care about
         older orphans. However, there could be some rows from the oldest of the three overlapping
         chains that get overwritten with the current model. So long as the pushdata, input, output
         rows etc. are indeed ** overwritten ** and no duplicate rows result from flushing the same
         rows twice, everything should be okay.
         """
        # NOTE: This could in theory use a lot of memory but the reorg would have to be very
        # deep to cause problems. It wouldn't be too hard to run the same check in bite sized
        # chunks to avoid too much memory allocation but I don't want to spend the time on it.
        with self.global_lock:
            old_chain_tx_hashes = self._make_reorg_tx_set(old_chain)
            new_chain_tx_hashes = self._make_reorg_tx_set(new_chain)
            additions_to_mempool = old_chain_tx_hashes - new_chain_tx_hashes

            removals_from_mempool = new_chain_tx_hashes - old_chain_tx_hashes
            return removals_from_mempool, additions_to_mempool, old_chain_tx_hashes
