import logging
import math
import os
import time
import typing
from typing import Iterator

import bitcoinx
from bitcoinx import hash_to_hex_str
from cassandra import ConsistencyLevel
from cassandra.cluster import Session
from cassandra.query import BatchStatement

from ..db_interface.types import MinedTxHashes, ConfirmedTransactionRow, InputRow, PushdataRow, \
    OutputRow
from ...constants import PROFILING
from ...types import ChainHashes

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))

if typing.TYPE_CHECKING:
    from .db import ScyllaDB


BATCH_SIZE = 10000


class ScyllaDBQueries:
    def __init__(self, db: "ScyllaDB") -> None:
        self.db = db
        self.session: Session = self.db.session
        self.logger = logging.getLogger('scylladb-queries')

    def load_temp_mined_tx_hashes(self, mined_tx_hashes: list[MinedTxHashes]) -> None:
        """columns: tx_hashes, blk_num"""
        self.db.tables.create_temp_mined_tx_hashes_table()
        insert_statement = self.session.prepare(
            "INSERT INTO temp_mined_tx_hashes (mined_tx_hash, blk_num) " "VALUES (?, ?)"
        )
        self.db.load_data_batched(insert_statement, mined_tx_hashes)

    def load_temp_inbound_tx_hashes(
        self, inbound_tx_hashes: list[tuple[str]], inbound_tx_table_name: str
    ) -> None:
        """columns: tx_hashes, blk_height"""
        self.db.tables.self.create_temp_inbound_tx_hashes_table(inbound_tx_table_name)
        insert_statement = self.session.prepare(
            "INSERT INTO inbound_tx_table_name (inbound_tx_hashes) VALUES (?)"
        )
        self.db.load_data_batched(insert_statement, inbound_tx_hashes)

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
        try:
            self.db.load_temp_inbound_tx_hashes(new_tx_hashes, inbound_tx_table_name)
            result_set = self.session.execute(f"SELECT inbound_tx_hashes FROM {inbound_tx_table_name}")
            inbound_tx_hashes = [row.inbound_tx_hash for row in result_set]
            prepared_statement = self.session.prepare(
                "SELECT mp_tx_hash FROM mempool_transactions WHERE mp_tx_hash = ?"
            )
            rows = [(hash_value,) for hash_value in inbound_tx_hashes]
            results = self.db.execute_with_concurrency(prepared_statement, rows)
            unprocessed_transactions = {
                inbound_tx_hashes[i] for i, (success, result) in enumerate(results) if success and not result
            }
            if not is_reorg:
                # Now unprocessed_transactions contains the list of transactions that have not been
                # processed yet
                return unprocessed_transactions
            else:
                result = self.session.execute(f"""SELECT * FROM temp_orphaned_txs;""")
                orphaned_txs = set(row.inbound_tx_hash for row in result)
                final_result = unprocessed_transactions - orphaned_txs
                return set(final_result)
        finally:
            self.db.drop_temp_inbound_tx_hashes(inbound_tx_table_name)

    def get_temp_mined_tx_hashes(self) -> list[bytes]:
        mined_tx_hashes_result_set = self.session.execute(
            "SELECT mined_tx_hash FROM temp_mined_tx_hashes")
        mined_tx_hashes = [row.mined_tx_hash for row in mined_tx_hashes_result_set]
        self.logger.debug("get_temp_mined_tx_hashes: %s",
            [hash_to_hex_str(x[0]) for x in mined_tx_hashes])
        return mined_tx_hashes

    def invalidate_mempool_rows(self) -> None:
        self.logger.debug(f"Deleting mined mempool txs")
        mined_tx_hashes = self.get_temp_mined_tx_hashes()
        delete_statement = self.session.prepare("DELETE FROM mempool_transactions WHERE mp_tx_hash = ?")
        delete_args = [(tx_hash,) for tx_hash in mined_tx_hashes]
        self.db.execute_with_concurrency(delete_statement, delete_args)

    def load_temp_mempool_removals(self, removals_from_mempool: set[bytes]) -> None:
        """i.e. newly mined transactions in a reorg context"""
        self.db.create_temp_mempool_removals_table()
        insert_statement = self.session.prepare("INSERT INTO temp_mempool_removals (tx_hash) VALUES (?)")
        insert_args = [(x,) for x in removals_from_mempool]
        self.db.execute_with_concurrency(insert_statement, insert_args)

    def load_temp_mempool_additions(self, additions_to_mempool: set[bytes]) -> None:
        self.db.create_temp_mempool_additions_table()
        insert_statement = self.session.prepare("INSERT INTO temp_mempool_additions (tx_hash, tx_timestamp) VALUES (?,?)")
        insert_args = [(x, int(time.time())) for x in additions_to_mempool]
        self.db.execute_with_concurrency(insert_statement, insert_args)

    def load_temp_orphaned_tx_hashes(self, orphaned_tx_hashes: set[bytes]) -> None:
        self.db.create_temp_orphaned_txs_table()
        insert_statement = self.session.prepare("INSERT INTO temp_orphaned_txs (tx_hash,) VALUES (?)")
        insert_args = [(x,) for x in orphaned_tx_hashes]
        self.db.execute_with_concurrency(insert_statement, insert_args)

    def remove_from_mempool(self) -> None:
        self.logger.debug("Removing reorg differential from mempool")
        tx_hashes_to_remove = self.session.execute(
            "SELECT tx_hash FROM temp_mempool_removals"
        )
        batch = BatchStatement()
        for tx_hash_row in tx_hashes_to_remove:
            batch.add(
                "DELETE FROM mempool_transactions WHERE mp_tx_hash = %s",
                (tx_hash_row.tx_hash,)
            )
        self.session.execute(batch)
        self.db.tables.drop_temp_mempool_removals()

    def add_to_mempool(self) -> None:
        self.logger.debug("Adding reorg differential to mempool")
        additions = self.session.execute(
            "SELECT tx_hash, tx_timestamp FROM temp_mempool_additions"
        )
        batch = BatchStatement()
        for addition in additions:
            batch.add(
                "INSERT INTO mempool_transactions (mp_tx_hash, mp_tx_timestamp) VALUES (%s, %s)",
                (addition.tx_hash, addition.tx_timestamp)
            )
        self.session.execute(batch)
        self.db.tables.drop_temp_mempool_additions()

    def update_checkpoint_tip(self, checkpoint_tip: bitcoinx.Header) -> None:
        assert isinstance(checkpoint_tip, bitcoinx.Header)
        block_hash_bytes = bytes.fromhex(checkpoint_tip.hash.hex())
        self.session.execute(
            """
            UPDATE checkpoint_state
            SET best_flushed_block_height = %s,
                best_flushed_block_hash = %s
            WHERE id = %s
            """,
            (checkpoint_tip.height, block_hash_bytes, 0)
        )

    def get_checkpoint_state(self) -> tuple[int, bytes, bool, bytes, bytes, bytes, bytes] | None:
        # Execute the SELECT query and fetch all results
        rows = self.session.execute("SELECT * FROM checkpoint_state;")
        # In Cassandra and ScyllaDB, we usually get a list of rows directly
        if rows:
            row = rows[0]
            best_flushed_block_height = row.best_flushed_block_height
            best_flushed_block_hash = row.best_flushed_block_hash
            reorg_was_allocated = bool(row.reorg_was_allocated)
            first_allocated_block_hash = row.first_allocated_block_hash
            last_allocated_block_hash = row.last_allocated_block_hash
            old_hashes_array = row.old_hashes_array
            new_hashes_array = row.new_hashes_array
            return (
                best_flushed_block_height,
                best_flushed_block_hash,
                reorg_was_allocated,
                first_allocated_block_hash,
                last_allocated_block_hash,
                old_hashes_array,
                new_hashes_array,
            )
        else:
            return None

    def get_txids_above_last_good_block_num(self, last_good_block_num: int) -> Iterator[bytes]:
        assert isinstance(last_good_block_num, int)
        # Perform the query with ordering by tx_block_num
        query = (
            "SELECT tx_hash FROM confirmed_transactions "
            "WHERE tx_block_num > %s "
            "ORDER BY tx_block_num DESC;"
        )
        # Execute the query
        rows = self.session.execute(query, (last_good_block_num,))
        count = len(rows)
        if count != 0:
            self.logger.warning(
                f"The database was abruptly shutdown ({count} unsafe txs for "
                f"rollback) - beginning repair process..."
            )
        # Return an iterator over the fetched rows
        return (row.tx_hash for row in rows)

    from cassandra.query import BatchStatement, ConsistencyLevel

    def delete_transaction_rows(self, tx_hash_hexes: list[str]) -> None:
        t0 = time.time()
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        for i, tx_hash_hex in enumerate(tx_hash_hexes):
            tx_hash_bytes = bytes.fromhex(tx_hash_hex)
            batch.add("DELETE FROM confirmed_transactions WHERE tx_hash = %s", (tx_hash_bytes,))
            if (i + 1) % BATCH_SIZE == 0 or i == len(tx_hash_hexes) - 1:
                self.session.execute(batch)
                batch.clear()
        t1 = time.time() - t0
        self.logger.log(
            PROFILING,
            f"elapsed time for bulk delete of transactions = {t1} seconds for {len(tx_hash_hexes)}"
        )

    def delete_pushdata_rows(self, pushdata_rows: list[PushdataRow]) -> None:
        t0 = time.time()
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        for i, (pushdata_hash, tx_hash, idx, ref_type) in enumerate(pushdata_rows):
            pushdata_bytes = bytes.fromhex(pushdata_hash)
            tx_hash_bytes = bytes.fromhex(tx_hash)
            batch.add(
                "DELETE FROM pushdata WHERE pushdata_hash = %s AND tx_hash = %s AND idx = %s AND ref_type = %s",
                (pushdata_bytes, tx_hash_bytes, idx, ref_type))
            if (i + 1) % BATCH_SIZE == 0 or i == len(pushdata_rows) - 1:
                self.session.execute(batch)
                batch.clear()
        t1 = time.time() - t0
        self.logger.log(
            PROFILING,
            f"elapsed time for bulk delete of pushdata rows = {t1} seconds for {len(pushdata_rows)}"
        )

    def delete_output_rows(self, output_rows: list[OutputRow]) -> None:
        t0 = time.time()
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        for i, (out_tx_hash, out_idx, out_value) in enumerate(output_rows):
            out_tx_hash_bytes = bytes.fromhex(out_tx_hash)
            batch.add("DELETE FROM txo_table WHERE out_tx_hash = %s AND out_idx = %s",
                (out_tx_hash_bytes, out_idx))
            if (i + 1) % BATCH_SIZE == 0 or i == len(output_rows) - 1:
                self.session.execute(batch)
                batch.clear()
        t1 = time.time() - t0
        self.logger.log(
            PROFILING,
            f"elapsed time for bulk delete of output rows = {t1} seconds for {len(output_rows)}"
        )

    def delete_input_rows(self, input_rows: list[InputRow]) -> None:
        t0 = time.time()
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        for i, (out_tx_hash, out_idx, in_tx_hash, in_idx) in enumerate(input_rows):
            out_tx_hash_bytes = bytes.fromhex(out_tx_hash)
            in_tx_hash_bytes = bytes.fromhex(in_tx_hash)
            batch.add(
                "DELETE FROM inputs_table WHERE out_tx_hash = %s AND out_idx = %s AND in_tx_hash = %s AND in_idx = %s",
                (out_tx_hash_bytes, out_idx, in_tx_hash_bytes, in_idx))
            if (i + 1) % BATCH_SIZE == 0 or i == len(input_rows) - 1:
                self.session.execute(batch)
                batch.clear()
        t1 = time.time() - t0
        self.logger.log(
            PROFILING,
            f"elapsed time for bulk delete of input rows = {t1} seconds for {len(input_rows)}"
        )

    def delete_header_row(self, block_hash: bytes) -> None:
        t0 = time.time()
        query = "DELETE FROM headers WHERE block_hash = %s"
        self.session.execute(query, (block_hash,))
        t1 = time.time() - t0
        self.logger.log(PROFILING, f"elapsed time for delete of header row = {t1} seconds")

    def get_duplicate_tx_hashes(self, tx_rows: list[ConfirmedTransactionRow]) -> list[
        ConfirmedTransactionRow]:
        t0 = time.time()
        BATCH_SIZE = 2000
        BATCHES_COUNT = math.ceil(len(tx_rows) / BATCH_SIZE)
        results = []
        for i in range(BATCHES_COUNT):
            if i == BATCHES_COUNT - 1:
                batched_txs = tx_rows[i * BATCH_SIZE:]
            else:
                batched_txs = tx_rows[i * BATCH_SIZE: (i + 1) * BATCH_SIZE]
            # We need to convert each tx_hash to bytes because ScyllaDB/Cassandra
            # expects blob data in this format.
            batched_tx_hashes = [bytes.fromhex(row.tx_hash) for row in batched_txs]
            # Use the IN clause with a tuple to select multiple rows.
            query = "SELECT * FROM confirmed_transactions WHERE tx_hash IN %s"
            fetched = self.session.execute(query, (tuple(batched_tx_hashes),))
            results.extend(fetched)
        t1 = time.time() - t0
        self.logger.log(
            PROFILING,
            f"elapsed time for selecting duplicate tx_hashes = {t1} seconds for {len(tx_rows)}",
        )
        return results

    def update_orphaned_headers(self, block_hashes: list[bytes]) -> None:
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        for block_hash in block_hashes:
            query = "UPDATE headers SET is_orphaned = %s WHERE block_hash = %s"
            batch.add(query, (True, block_hash))
        self.session.execute(batch)

    def update_allocated_state(
            self,
            reorg_was_allocated: bool,
            first_allocated: bitcoinx.Header,
            last_allocated: bitcoinx.Header,
            old_hashes: ChainHashes | None,
            new_hashes: ChainHashes | None,
    ) -> None:
        # Convert hashes to bytearray if they exist
        old_hashes_array = bytearray().join(old_hashes) if old_hashes else None
        new_hashes_array = bytearray().join(new_hashes) if new_hashes else None

        # Since ScyllaDB doesn't have a concept of transactions in the same way that
        # relational databases do, you don't need to start or commit a transaction.
        if old_hashes_array is not None and new_hashes_array is not None:
            query = """
                UPDATE checkpoint_state
                SET reorg_was_allocated = %s,
                    first_allocated_block_hash = %s,
                    last_allocated_block_hash = %s,
                    old_hashes_array = %s,
                    new_hashes_array = %s
                WHERE id = 0
            """
            self.session.execute(
                query,
                (
                    reorg_was_allocated,
                    first_allocated.hash,
                    last_allocated.hash,
                    old_hashes_array,
                    new_hashes_array,
                ),
            )
        else:
            query = """
                UPDATE checkpoint_state
                SET reorg_was_allocated = %s,
                    first_allocated_block_hash = %s,
                    last_allocated_block_hash = %s,
                    old_hashes_array = null,
                    new_hashes_array = null
                WHERE id = 0
            """
            self.session.execute(
                query,
                (
                    reorg_was_allocated,
                    first_allocated.hash,
                    last_allocated.hash,
                ),
            )

    def get_mempool_size(self) -> int:
        query = "SELECT COUNT(*) FROM mempool_transactions"
        count = self.session.execute(query).one()
        return count[0] if count else 0
