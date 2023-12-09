# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.

import logging
import os
from pathlib import Path
from typing import cast
import typing

import bitcoinx
import cbor2
from bitcoinx import hash_to_hex_str

from conduit_lib.database.ffdb.flat_file_db import FlatFileDb
from conduit_lib.types import TxLocation, Slice, DataLocation

if typing.TYPE_CHECKING:
    from conduit_lib import LMDB_Database
    import array

from conduit_lib.constants import SIZE_UINT64_T

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))


class LmdbTxOffsets:
    TX_OFFSETS_DB = b"tx_offsets_db"

    def __init__(self, db: "LMDB_Database", worker_id: str = ""):
        self.db = db
        logger_name = "lmdb-tx-offsets" if not worker_id else f"lmdb-tx-offsets-{worker_id}"
        self.logger = logging.getLogger(logger_name)

        tx_offsets_dir = Path(os.environ["DATADIR_HDD"]) / "tx_offsets"
        tx_offsets_lockfile = Path(os.environ["DATADIR_SSD"]) / "tx_offsets.lock"

        self.ffdb = FlatFileDb(tx_offsets_dir, tx_offsets_lockfile)
        self.tx_offsets_db = self.db.env.open_db(self.TX_OFFSETS_DB)

    def _get_single_tx_slice(self, tx_loc: TxLocation) -> Slice | None:
        """If end_offset=0 then it goes to the end of the block"""
        with self.db.env.begin(db=self.tx_offsets_db, write=False, buffers=True) as txn:
            val: bytes = txn.get(tx_loc.block_hash)
            if val is None:
                self.logger.error(
                    f"Tx offsets for block_hash: {hash_to_hex_str(tx_loc.block_hash)} " f"not found"
                )
                return None
            assert tx_loc is not None
            (
                read_path,
                start_offset_in_dat_file,
                end_offset_in_dat_file,
            ) = cast(tuple[str, int, int], cbor2.loads(val))

        # Starting offset of tx
        start_offset = start_offset_in_dat_file + (tx_loc.tx_position * SIZE_UINT64_T)
        with open(read_path, "rb") as f:
            f.seek(start_offset)
            tx_start_offset = bitcoinx.unpack_le_uint64(f.read(8))[0]

        # End offset of tx
        # If it's the last in the block there won't be an offset hence get_block_metadata()
        start_offset = start_offset_in_dat_file + (tx_loc.tx_position + 1) * SIZE_UINT64_T
        if start_offset == end_offset_in_dat_file:
            block_metadata = self.db.get_block_metadata(tx_loc.block_hash)
            assert block_metadata is not None
            tx_end_offset = block_metadata.block_size
        else:
            with open(read_path, "rb") as f:
                f.seek(start_offset)
                tx_end_offset = bitcoinx.unpack_le_uint64(f.read(8))[0]

        return Slice(tx_start_offset, tx_end_offset)

    # -------------------- EXTERNAL API -------------------- #

    def get_rawtx_by_loc(self, tx_loc: TxLocation) -> bytes | None:
        slice = self._get_single_tx_slice(tx_loc)
        if slice is None:
            return None
        return self.db.get_block(tx_loc.block_num, slice)

    def get_data_location(self, block_hash: bytes) -> DataLocation | None:
        with self.db.env.begin(db=self.tx_offsets_db, write=False, buffers=True) as txn:
            val: bytes = txn.get(block_hash)
            if not val:
                self.logger.error(f"Tx offsets for block_hash: {hash_to_hex_str(block_hash)} " f"not found")
                return None
        read_path, start_offset, end_offset = cast(tuple[str, int, int], cbor2.loads(val))
        return DataLocation(read_path, start_offset, end_offset)

    def get_tx_offsets(self, block_hash: bytes) -> bytes | None:
        """If end_offset=0 then it goes to the end of the block"""
        data_location = self.get_data_location(block_hash)
        if not data_location:
            return None

        with self.ffdb:
            try:
                return self.ffdb.get(data_location, lock_free_access=True)
            except FileNotFoundError:
                return None

    def put_tx_offsets(self, batched_tx_offsets: list[tuple[bytes, "array.ArrayType[int]"]]) -> None:
        with self.db.env.begin(db=self.tx_offsets_db, write=True, buffers=False) as txn:
            cursor = txn.cursor()
            with self.ffdb:
                for block_hash, tx_offsets in batched_tx_offsets:
                    data_location: DataLocation = self.ffdb.put(tx_offsets.tobytes())
                    cursor.put(block_hash, cbor2.dumps(data_location))
