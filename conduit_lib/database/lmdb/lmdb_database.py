# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.

import asyncio
from bitcoinx import double_sha256, hash_to_hex_str
import cbor2
from concurrent.futures import ThreadPoolExecutor
from io import BytesIO
import logging
import lmdb
import os
from pathlib import Path
import threading
import typing

from bitcoinx.packing import struct_be_I

from .blocks import LmdbBlocks
from .tx_offsets import LmdbTxOffsets
from .merkle_tree import LmdbMerkleTree

if typing.TYPE_CHECKING:
    import array

from conduit_lib.algorithms import calc_depth
from conduit_lib.database.lmdb.types import MerkleTreeRow
from conduit_lib.types import (
    TxLocation,
    BlockMetadata,
    TxMetadata,
    ChainHashes,
    Slice,
    DataLocation,
    BlockHeaderRow,
)

try:
    from ...constants import PROFILING, MAX_BLOCK_PER_BATCH_COUNT_CONDUIT_INDEX
except ImportError:
    from conduit_lib.constants import PROFILING, MAX_BLOCK_PER_BATCH_COUNT_CONDUIT_INDEX

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))


class LMDB_Database:
    """Simple interface to LMDB - This interface must be used at all times for thread-safety"""

    def __init__(self, storage_path: str | None = None, lock: bool = True, worker_id: str = "",
            integrity_check_raw_blocks: bool = False) -> None:

        logger_name = "lmdb-database" if not worker_id else f"lmdb-database-{worker_id}"
        self.logger = logging.getLogger(logger_name)
        self.logger.setLevel(PROFILING)

        lmdb_database_dir: str = str(Path(os.environ["DATADIR_SSD"]) / "lmdb_data")

        if not storage_path:
            storage_path = lmdb_database_dir

        if not Path(storage_path).exists():
            os.makedirs(Path(storage_path), exist_ok=True)

        self._map_size = pow(1024, 3) * 5
        self._storage_path = storage_path
        self.env = lmdb.open(
            self._storage_path,
            max_dbs=7,
            readahead=False,
            sync=False,
            map_size=self._map_size,
            lock=lock,
        )
        self._opened = True

        self.blocks = LmdbBlocks(self, worker_id=worker_id, check_zstd_file=integrity_check_raw_blocks)
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
            assert (
                block_metadata is not None
            ), "all necessary block metadata should always be available for reorg handling"
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

    def get_mtree_row(self, blk_hash: bytes, level: int, cursor: lmdb.Cursor | None = None) -> bytes | None:
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

    async def get_rawtx_by_loc_async(self, executor: ThreadPoolExecutor, tx_loc: TxLocation) -> bytes | None:
        return await asyncio.get_running_loop().run_in_executor(executor, self.get_rawtx_by_loc, tx_loc)

    def get_single_tx_slice(self, tx_loc: TxLocation) -> Slice | None:
        with self.global_lock:
            return self.tx_offsets._get_single_tx_slice(tx_loc)

    def get_tx_offsets(self, block_hash: bytes) -> bytes | None:
        with self.global_lock:
            return self.tx_offsets.get_tx_offsets(block_hash)

    def put_tx_offsets(self, batched_tx_offsets: list[tuple[bytes, "array.ArrayType[int]"]]) -> None:
        with self.global_lock:
            return self.tx_offsets.put_tx_offsets(batched_tx_offsets)

    def get_block_num(self, block_hash: bytes) -> int | None:
        with self.global_lock:
            return self.blocks.get_block_num(block_hash)

    def get_block_delete_markers(self, block_hash: bytes) -> BlockHeaderRow | None:
        with self.global_lock:
            return self.blocks.get_block_delete_markers(block_hash)

    def check_block(self, block_hash: bytes) -> None:
        block_num = self.get_block_num(block_hash)

        with self.global_lock:
            assert block_num is not None
            data_location = self.blocks.get_data_location(block_num)
            if os.environ['PRUNE_MODE'] == "0":
                assert data_location is not None
                uncompressed_size = self.blocks.ffdb.uncompressed_file_size(data_location.file_path)
                assert (
                    data_location.end_offset <= uncompressed_size
                ), f"There is data missing from the data file for {hash_to_hex_str(block_hash)}"
            else:
                if data_location is None:  # it was pruned
                    return None

        # Ensure that the merkle tree table and tx offsets table are aligned to return the
        # correct coinbase transaction from the raw block data.
        block_metadata = self.get_block_metadata(block_hash)
        assert block_metadata is not None
        base_level = calc_depth(block_metadata.tx_count) - 1
        coinbase_tx_hash = self.get_mtree_node(block_hash, level=base_level, position=0)
        coinbase_tx_location = TxLocation(block_hash, block_num, tx_position=0)
        rawtx = self.get_rawtx_by_loc(coinbase_tx_location)
        assert double_sha256(rawtx) == coinbase_tx_hash

    def try_purge_block_data(self, block_hash: bytes, height: int) -> None:
        """Scrubs all records for this block hash.

        raises `AssertionError` if any of the checks fail"""
        try:
            self.logger.debug(f"Purging all references for block: {hash_to_hex_str(block_hash)}, height: {height}")
            self.global_lock.acquire()
            block_num = self.get_block_num(block_hash)
            assert block_num is not None
            with self.env.begin(write=True, buffers=False) as txn:
                data_location_block = self.blocks.get_data_location(block_num)
                assert data_location_block is not None
                block_num_bytes = struct_be_I.pack(block_num)
                txn.delete(block_num_bytes, db=self.blocks.blocks_db)
                txn.delete(block_hash, db=self.blocks.block_nums_db)
                txn.delete(block_hash, db=self.blocks.block_metadata_db)

                data_location_tx_offsets = self.tx_offsets.get_data_location(block_hash)
                assert data_location_tx_offsets is not None
                txn.delete(block_hash, db=self.tx_offsets.tx_offsets_db)

                data_location_merkle_tree = self.merkle_tree.get_data_location(block_hash)
                assert data_location_merkle_tree is not None
                txn.delete(block_hash, db=self.merkle_tree.mtree_db)

            # TODO(db-repair): This is wrong. Deleting a whole file will affect other blocks.
            #  In future, could overwrite the raw block slices with zeros and zstd would compress
            #  it down to nothing. This way it would not change the uncompressed byte offsets of
            #  the other data so the index remains valid for other entries.
            # self.blocks.ffdb.delete_file(Path(data_location_block.file_path))
            # self.tx_offsets.ffdb.delete_file(Path(data_location_tx_offsets.file_path))
            # self.merkle_tree.ffdb.delete_file(Path(data_location_merkle_tree.file_path))

        # NOTE If this situation is ever encountered. E.g. maybe some tables have
        # data but the metadata needed to do a full purge is incomplete. The only
        # realistic solution is start syncing the blockchain from the last good
        # height and overwrite the records. There will be some wasted / dead disc
        # usage that will never be reclaimed but at least the server can remain
        # operational.
        except Exception:
            self.logger.exception(
                f"Failed to complete full purge of block data for block hash: "
                f"{hash_to_hex_str(block_hash)}, height: {height}. Backtracking..."
            )
        finally:
            self.logger.debug(f"Successfully purged all references for block: "
                f"{hash_to_hex_str(block_hash)}, height: {height}")
            self.global_lock.release()

    def get_block(self, block_num: int, slice: Slice | None = None) -> bytes | None:
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

    def get_reorg_differential(
        self, old_chain: ChainHashes, new_chain: ChainHashes
    ) -> tuple[set[bytes], set[bytes], set[bytes]]:
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
            return (
                removals_from_mempool,
                additions_to_mempool,
                old_chain_tx_hashes,
            )

    def is_safe_to_delete_block_file(
        self, block_hashes: list[bytearray], block_num_prune_threshold: int
    ) -> bool:
        # We are iterating over the blocks in a single file to make sure
        # that ALL of them have been marked safe for deletion.
        # Only delete when all have been marked safe for deletion and they blocks
        # are old enough to allow pruning.
        for block_hash in block_hashes:
            block_hash_bytes = bytes(block_hash)
            block_num = self.get_block_num(block_hash_bytes)
            assert block_num is not None
            if block_num >= block_num_prune_threshold:
                return False
            result = self.get_block_delete_markers(block_hash_bytes)
            if result is None:
                return False
        return True

    def prune_data_file_if_safe(self, filepath: Path, block_num_prune_threshold: int) -> None:
        block_hashes_arr = self.blocks.get_block_hashes_in_file(str(filepath))
        assert block_hashes_arr is not None
        block_hashes = [block_hashes_arr[i * 32 : (i + 1) * 32] for i in range(len(block_hashes_arr) // 32)]
        # We can't delete the tx_offsets or merkle_tree data files on the same schedule because
        # tx_offsets pack much more tightly into a 128MB file, so we'd need to
        # run an independent check for these other types of data files
        # Merkle Tree data files are also needed to unpack the short hashes from ScyllaDB
        # So pretty much need to be kept around

        safe_to_delete = self.is_safe_to_delete_block_file(block_hashes, block_num_prune_threshold)
        if safe_to_delete:
            self.logger.info(
                f"Pruning raw block data file: {str(filepath)} "
                f"Size: {filepath.stat().st_size // 1024 ** 2:.3f}MB"
            )
            block_hash = b""
            try:
                self.global_lock.acquire()
                # Delete the LMDB Entries
                for block_hash in block_hashes:
                    block_num = self.get_block_num(block_hash)
                    assert block_num is not None
                    with self.env.begin(write=True, buffers=False) as txn:
                        txn.delete(struct_be_I.pack(block_num), db=self.blocks.blocks_db)
                        # NOTE: We do not delete the entry in the block_nums_db because
                        # block_num must always increment correctly and always be unique.
                        # Disc usage to keep this in storage is trivial anyway

                # Delete the FFDB Entries
                if Path(filepath).exists():
                    self.blocks.ffdb.delete_file(Path(filepath))
            except Exception:
                self.logger.exception(
                    f"Failed to complete full purge of block data for block hash: "
                    f"{hash_to_hex_str(block_hash)}"
                )
            finally:
                self.global_lock.release()

    def delete_blocks_when_safe(self, blocks: list[BlockHeaderRow], tip_hash_conduit_index: bytes) -> None:
        assert self.env is not None
        # ConduitIndex might be up to 250 blocks behind so we want to find the tip hash
        # of ConduitIndex and preserve a further 250 blocks deep from there. So it might be
        # up to 500 blocks that are preserved.
        # This is done for two reasons:
        # 1) We cannot delete the last block_num -> block location entry because it will break
        #    the sequencing procedure ensuring that each allocated block_num is unique
        # 2) ConduitIndex needs to have blocks on disc to complete its db repair procedure
        #    for rewinding parsed tx data that was part of a batch that did not fully complete.
        block_num_tip = self.get_block_num(tip_hash_conduit_index)
        assert block_num_tip is not None
        block_num_prune_threshold = block_num_tip - MAX_BLOCK_PER_BATCH_COUNT_CONDUIT_INDEX

        # Persist intention to delete these blocks as soon as it is safe
        # There can be multiple blocks per file and deleting one would affect the byte offsets
        # which would then invalidate the other stored metadata about where to locate each block
        # and rawtxs within each block. We only want to delete files when all blocks in a given
        # file are marked for deletion. Until then, we wait.
        self.logger.debug(f"LMDB delete_blocks_when_safe was called for {len(blocks)} blocks")
        with self.env.begin(write=True, buffers=False) as txn:
            for block_info in blocks:
                txn.put(
                    bytes.fromhex(block_info.block_hash),
                    cbor2.dumps(block_info),
                    db=self.blocks.block_delete_markers_db,
                )

        last_block_num = self.blocks._get_last_block_num()
        data_location = self.blocks.get_data_location(last_block_num)
        assert data_location is not None
        filepath = Path(data_location.file_path)
        filename = filepath.name
        if self.blocks.ffdb.use_compression:
            file_num = int(filename.lstrip('data_').rstrip('.dat.zst'))
        else:
            file_num = int(filename.lstrip('data_').rstrip('.dat'))
        self.logger.debug(
            f"Current mutable raw block data file: {filepath}. "
            f"Size: {filepath.stat().st_size//1024**2:.3f}MB"
        )
        for file_num in reversed(range(0, file_num)):
            self.logger.debug(f"delete_blocks_when_safe: file_num: {file_num}")
            filepath = self.blocks.ffdb._file_num_to_mutable_file_path(file_num)
            if not filepath.exists():
                return  # early exit if these earlier blocks have already been cleaned up
            self.prune_data_file_if_safe(filepath, block_num_prune_threshold)


def get_full_tx_hash(tx_location: TxLocation, lmdb: LMDB_Database) -> bytes | None:
    # get base level of merkle tree with the tx hashes array
    block_metadata = lmdb.get_block_metadata(tx_location.block_hash)
    if block_metadata is None:
        return None
    base_level = calc_depth(block_metadata.tx_count) - 1

    tx_loc = TxLocation(tx_location.block_hash, tx_location.block_num, tx_location.tx_position)
    tx_hash = lmdb.get_tx_hash_by_loc(tx_loc, base_level)
    return tx_hash
