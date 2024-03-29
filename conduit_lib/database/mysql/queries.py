# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.

import logging
import math
import os
from pathlib import Path
import time
import typing
from typing import Any, cast, Iterator
import uuid

import MySQLdb
import bitcoinx

from .bulk_loads import MySQLBulkLoads
from .tables import MySQLTables
from ..db_interface.types import MinedTxHashXes, CheckpointStateRow
from ...constants import PROFILING
from ...types import ChainHashes

if typing.TYPE_CHECKING:
    from .db import MySQLDatabase


class MySQLQueries:
    def __init__(
        self,
        conn: MySQLdb.Connection,
        tables: MySQLTables,
        bulk_loads: MySQLBulkLoads,
        db: "MySQLDatabase",
    ) -> None:
        self.logger = logging.getLogger("mysql-queries")
        self.logger.setLevel(logging.DEBUG)
        self.conn = conn
        self.tables = tables
        self.bulk_loads = bulk_loads
        self.db = db

    def load_temp_mined_tx_hashXes(self, mined_tx_hashes: list[MinedTxHashXes]) -> None:
        """columns: tx_hashes, blk_num"""
        self.tables.create_temp_mined_tx_hashXes_table()

        outfile = Path(str(uuid.uuid4()) + ".csv")
        try:
            string_rows = ["%s,%s\n" % (row) for row in mined_tx_hashes]
            column_names = ["mined_tx_hash", "blk_num"]
            self.bulk_loads._load_data_infile(
                "temp_mined_tx_hashXes",
                string_rows,
                column_names,
                binary_column_indices=[0],
            )
        finally:
            if os.path.exists(outfile):
                os.remove(outfile)

    def load_temp_inbound_tx_hashes(
        self, inbound_tx_hashes: list[tuple[str]], inbound_tx_table_name: str
    ) -> None:
        """columns: tx_hashes, blk_height"""
        self.logger.debug(f"creating memory table: {inbound_tx_table_name}...")
        self.tables.create_temp_inbound_tx_hashes_table(inbound_tx_table_name)
        string_rows = ["%s\n" % (row) for row in inbound_tx_hashes]
        column_names = ["inbound_tx_hash"]
        self.bulk_loads._load_data_infile(
            inbound_tx_table_name,
            string_rows,
            column_names,
            binary_column_indices=[0],
        )

    def get_unprocessed_txs(
        self,
        is_reorg: bool,
        new_tx_hashes: list[tuple[str]],
        inbound_tx_table_name: str,
    ) -> set[bytes]:
        """
        Usually if all mempool txs have been processed, this function will only return
        the coinbase tx. If a reorg has occurred it will use the temp_orphaned_txs to avoid
        writing duplicated pushdata, input, output rows for transactions that were already
        processed for an orphan block
        """
        self.load_temp_inbound_tx_hashes(new_tx_hashes, inbound_tx_table_name)
        try:
            self.conn.query(
                f"""
                -- tx_hash ISNULL implies no match therefore not previously processed yet
                SELECT *
                FROM {inbound_tx_table_name}
                LEFT OUTER JOIN mempool_transactions
                ON (mempool_transactions.mp_tx_hash = {inbound_tx_table_name}.inbound_tx_hash)
                WHERE mempool_transactions.mp_tx_hash IS NULL
                ;"""
            )
            result = self.conn.store_result()
            final_result = set(x[0] for x in result.fetch_row(0))
        finally:
            self.db.commit_transaction()

        if not is_reorg:
            self.tables.drop_temp_inbound_tx_hashXes(inbound_tx_table_name)
            return final_result
        else:
            try:
                self.conn.query(f"""SELECT * FROM temp_orphaned_txs;""")
                # Todo - Where tx_hash = inbound_tx_table_name.inbound_tx_hash
                result = self.conn.store_result()
                orphaned_txs = set(x[0] for x in result.fetch_row(0))
            finally:
                self.db.commit_transaction()

            final_result = final_result - orphaned_txs
            self.tables.drop_temp_inbound_tx_hashXes(inbound_tx_table_name)
            return set(final_result)

    # # Debugging
    # def get_temp_mined_tx_hashXes(self):
    #     self.conn.query(
    #         """
    #         SELECT *
    #         FROM temp_mined_tx_hashXes;"""
    #     )
    #     result = self.conn.store_result()
    #     self.logger.debug(f"get_temp_mined_tx_hashXes: "
    #                       f"{[hash_to_hex_str(x[0]) for x in result.fetch_row(0)]}")

    def invalidate_mempool_rows(self) -> None:
        self.logger.debug(f"Deleting mined mempool txs")
        # self.get_temp_mined_tx_hashXes()
        query = f"""
            DELETE FROM mempool_transactions
            WHERE mp_tx_hash in (
                SELECT mined_tx_hash
                FROM temp_mined_tx_hashXes
            );
            """
        # NOTE: It would have been nice to count the rows that were deleted for accounting
        # purposes but MySQL makes this difficult.
        self.conn.query(query)
        self.conn.commit()

    def load_temp_mempool_removals(self, removals_from_mempool: set[bytes]) -> None:
        """i.e. newly mined transactions in a reorg context"""
        self.db.create_temp_mempool_removals_table()

        outfile = Path(str(uuid.uuid4()) + ".csv")
        try:
            string_rows = ["%s\n" % (tx_hash.hex()) for tx_hash in removals_from_mempool]
            column_names = ["tx_hash"]
            self.bulk_loads._load_data_infile(
                "temp_mempool_removals",
                string_rows,
                column_names,
                binary_column_indices=[0],
            )
        finally:
            if os.path.exists(outfile):
                os.remove(outfile)

    def load_temp_mempool_additions(self, additions_to_mempool: set[bytes]) -> None:
        self.db.create_temp_mempool_additions_table()

        outfile = Path(str(uuid.uuid4()) + ".csv")
        try:
            string_rows = ["%s\n" % tx_hash.hex() for tx_hash in additions_to_mempool]
            column_names = ["tx_hash"]
            self.bulk_loads._load_data_infile(
                "temp_mempool_additions",
                string_rows,
                column_names,
                binary_column_indices=[0],
            )
        finally:
            if os.path.exists(outfile):
                os.remove(outfile)

    def load_temp_orphaned_tx_hashXes(self, orphaned_tx_hashXes: set[bytes]) -> None:
        self.db.create_temp_orphaned_txs_table()

        outfile = Path(str(uuid.uuid4()) + ".csv")
        try:
            string_rows = ["%s\n" % tx_hash.hex() for tx_hash in orphaned_tx_hashXes]
            column_names = ["tx_hash"]
            self.bulk_loads._load_data_infile(
                "temp_orphaned_txs",
                string_rows,
                column_names,
                binary_column_indices=[0],
            )
        finally:
            if os.path.exists(outfile):
                os.remove(outfile)

    def remove_from_mempool(self) -> None:
        self.logger.debug(f"Removing reorg differential from mempool")
        # Note the loading of the temp_mempool_removals is not done here
        query = f"""
            DELETE FROM mempool_transactions
            WHERE mempool_transactions.mp_tx_hash in (
                SELECT tx_hash
                FROM temp_mempool_removals
            );
            """
        self.conn.query(query)
        self.conn.commit()
        self.tables.drop_temp_mempool_removals()

    def add_to_mempool(self) -> None:
        self.logger.debug(f"Adding reorg differential to mempool")
        # Note the loading of the temp_mempool_additions is not done here
        query = f"""
            INSERT INTO mempool_transactions
                SELECT tx_hash, tx_timestamp
                FROM temp_mempool_additions;
            """
        self.conn.query(query)
        self.conn.commit()
        self.tables.drop_temp_mempool_additions()

    def update_checkpoint_tip(self, checkpoint_tip: bitcoinx.Header) -> None:
        try:
            assert isinstance(checkpoint_tip, bitcoinx.Header)
            self.db.start_transaction()
            query = f"""
                UPDATE checkpoint_state
                SET id = 0,
                    best_flushed_block_height = {checkpoint_tip.height},
                    best_flushed_block_hash = X'{checkpoint_tip.hash.hex()}'
                WHERE id = 0;
            """
            self.conn.query(query)
        finally:
            self.db.commit_transaction()

    def get_checkpoint_state(
        self,
    ) -> CheckpointStateRow | None:
        """We 'allocate' work up to a given block hash and then we set the new checkpoint only
        after every block in the batch is successfully flushed"""
        try:
            query = f"""SELECT * FROM checkpoint_state;"""
            self.conn.query(query)
            result = self.conn.store_result()
            rows = result.fetch_row(0)
            if len(rows) != 0:
                row = rows[0]
                return CheckpointStateRow(
                    best_flushed_block_height=row[1],
                    best_flushed_block_hash=row[2],
                    reorg_was_allocated=row[3],
                    first_allocated_block_hash=row[4],
                    last_allocated_block_hash=row[5],
                    old_hashes_array=row[6],
                    new_hashes_array=row[7],
                )
            else:
                return None
        finally:
            self.db.commit_transaction()

    def get_txids_above_last_good_block_num(self, last_good_block_num: int) -> Iterator[Any]:
        """This query will do a full table scan and so will not scale - instead would need to
        pull txids by blockhash from merkleproof to get the set of txids..."""
        assert isinstance(last_good_block_num, int)
        try:
            query = f"""
                SELECT tx_hash
                FROM confirmed_transactions
                WHERE tx_block_num > {last_good_block_num}
                ORDER BY tx_block_num DESC;
                """
            # self.logger.debug(query)
            self.conn.query(query)
            result = self.conn.store_result()
            count = result.num_rows()
            if count != 0:
                self.logger.warning(
                    f"The database was abruptly shutdown ({count} unsafe txs for "
                    f"rollback) - beginning repair process..."
                )
            return cast(Iterator[Any], result.fetch_row(0))
        finally:
            self.db.commit_transaction()

    def delete_transaction_rows(self, tx_hash_hexes: list[str]) -> None:
        # Deletion is very slow for large batch sizes
        t0 = time.time()
        BATCH_SIZE = 2000
        BATCHES_COUNT = math.ceil(len(tx_hash_hexes) / BATCH_SIZE)
        for i in range(BATCHES_COUNT):
            self.db.start_transaction()
            try:
                if i == BATCHES_COUNT - 1:
                    tx_hash_hexes_batch = tx_hash_hexes[i * BATCH_SIZE :]
                else:
                    tx_hash_hexes_batch = tx_hash_hexes[i * BATCH_SIZE : (i + 1) * BATCH_SIZE]
                stringified_tx_hash_hexes = ",".join(
                    [f"X'{tx_hash_hex}'" for tx_hash_hex in tx_hash_hexes_batch]
                )

                query = f"""
                    DELETE FROM confirmed_transactions
                    WHERE tx_hash in ({stringified_tx_hash_hexes})"""
                self.conn.query(query)
            finally:
                self.db.commit_transaction()
        t1 = time.time() - t0
        self.logger.log(
            PROFILING,
            f"elapsed time for bulk delete of transactions = {t1} seconds for " f"{len(tx_hash_hexes)}",
        )

    def update_orphaned_headers(self, block_hashes: list[bytes]) -> None:
        """This allows us to filter out any query results that do not lie on the longest chain"""
        for block_hash in block_hashes:
            try:
                self.db.start_transaction()
                query = f"""
                    UPDATE headers
                    SET is_orphaned = 1
                    WHERE block_hash = X'{block_hash.hex()}'
                    """
                self.conn.query(query)
            finally:
                self.db.commit_transaction()

    def update_allocated_state(
        self,
        reorg_was_allocated: bool,
        first_allocated: bitcoinx.Header,
        last_allocated: bitcoinx.Header,
        old_hashes: ChainHashes | None,
        new_hashes: ChainHashes | None,
    ) -> None:
        # old_hashes and new_hashes are null unless there was a reorg in which case we
        # need to be precise about how we do the db repair / rollback (if ever needed)
        if old_hashes is not None:
            old_hashes_array = bytearray()
            for block_hash in old_hashes:
                old_hashes_array += block_hash
        else:
            old_hashes_array = None

        if new_hashes is not None:
            new_hashes_array = bytearray()
            for block_hash in new_hashes:
                new_hashes_array += block_hash
        else:
            new_hashes_array = None

        try:
            self.db.start_transaction()
            if old_hashes_array and new_hashes_array is not None:
                query = f"""
                    UPDATE checkpoint_state
                    SET reorg_was_allocated = {reorg_was_allocated},
                        first_allocated_block_hash = X'{first_allocated.hash.hex()}',
                        last_allocated_block_hash = X'{last_allocated.hash.hex()}',
                        old_hashes_array = X'{old_hashes_array.hex()}',
                        new_hashes_array = X'{new_hashes_array.hex()}'
                    WHERE id = 0
                    """
            else:
                query = f"""
                    UPDATE checkpoint_state
                    SET reorg_was_allocated = {reorg_was_allocated},
                        first_allocated_block_hash = X'{first_allocated.hash.hex()}',
                        last_allocated_block_hash = X'{last_allocated.hash.hex()}',
                        old_hashes_array = null,
                        new_hashes_array = null
                    WHERE id = 0
                    """
            self.conn.query(query)
        finally:
            self.db.commit_transaction()

    def get_mempool_size(self) -> int:
        sql = (
            "SELECT TABLE_ROWS FROM information_schema.tables "
            "WHERE table_schema = DATABASE() and table_name = 'mempool_transactions';"
        )
        self.conn.query(sql)
        result = self.conn.store_result()
        return int(result.fetch_row()[0][0])
