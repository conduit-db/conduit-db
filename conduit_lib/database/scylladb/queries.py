import logging
import os
import time
import typing
from typing import cast, Iterator

import bitcoinx
from cassandra import ConsistencyLevel  # pylint:disable=E0611
from cassandra.cluster import Session  # pylint:disable=E0611
from cassandra.concurrent import execute_concurrent_with_args  # pylint:disable=E0611
from cassandra.query import BatchStatement  # pylint:disable=E0611

from ..db_interface.types import MinedTxHashes, InputRow, PushdataRow
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
        # NOTE: the row.block_number value is not actually used because no queries
        # actually use it!
        if mined_tx_hashes:
            values_to_add = [bytes.fromhex(row.txid) for row in mined_tx_hashes]
            self.db.cache.r.sadd('temp_mined_tx_hashes', *values_to_add)

    def load_temp_inbound_tx_hashes(
        self, inbound_tx_hashes: list[tuple[str]], inbound_tx_table_name: str
    ) -> None:
        if inbound_tx_hashes:
            values_to_add = [bytes.fromhex(row[0]) for row in inbound_tx_hashes]
            self.db.cache.r.sadd(inbound_tx_table_name, *values_to_add)

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
        self.db.load_temp_inbound_tx_hashes(new_tx_hashes, inbound_tx_table_name)
        try:
            if not is_reorg:
                unprocessed_transactions = self.db.cache.r.sdiff(inbound_tx_table_name, b"mempool")
                return cast(set[bytes], unprocessed_transactions)
            else:
                unprocessed_transactions = self.db.cache.r.sdiff(inbound_tx_table_name, b"mempool",
                    b'temp_orphaned_txs')
                return cast(set[bytes], unprocessed_transactions)
        finally:
            self.db.drop_temp_inbound_tx_hashes(inbound_tx_table_name)

    def _get_temp_mined_tx_hashes(self) -> set[bytes]:
        return self.db.cache.r.smembers('temp_mined_tx_hashes')

    def invalidate_mempool_rows(self) -> None:
        self.logger.debug("Deleting mined mempool txs")
        mined_tx_hashes: set[bytes] = self._get_temp_mined_tx_hashes()

        def remove_batch(batch: list[bytes]) -> None:
            self.db.cache.r.srem(b"mempool", *batch)

        batch_size = 10000
        mined_tx_hashes_list = list(mined_tx_hashes)

        futures = []
        assert self.db.executor is not None
        for i in range(0, len(mined_tx_hashes_list), batch_size):
            batch = mined_tx_hashes_list[i:i+batch_size]
            future = self.db.executor.submit(remove_batch, batch)
            futures.append(future)

        for future in futures:
            future.result()

        self.logger.debug("Mined mempool txs deletion complete")

    def load_temp_mempool_removals(self, removals_from_mempool: set[bytes]) -> None:
        """i.e. newly mined transactions in a reorg context"""
        if removals_from_mempool:
            self.db.cache.r.sadd("temp_mempool_removals", *removals_from_mempool)

    def load_temp_mempool_additions(self, additions_to_mempool: set[bytes]) -> None:
        if additions_to_mempool:
            self.db.cache.r.sadd("temp_mempool_additions", *additions_to_mempool)

    def load_temp_orphaned_tx_hashes(self, orphaned_tx_hashes: set[bytes]) -> None:
        if orphaned_tx_hashes:
            self.db.cache.r.sadd("temp_orphaned_txs", *orphaned_tx_hashes)

    def remove_from_mempool(self) -> None:
        self.logger.debug("Removing reorg differential from mempool")
        tx_hashes_to_remove = self.db.cache.r.smembers("temp_mempool_removals")
        if tx_hashes_to_remove:
            self.db.cache.r.srem("mempool", *tx_hashes_to_remove)
            self.db.tables.drop_temp_mempool_removals()

    def add_to_mempool(self) -> None:
        self.logger.debug("Adding reorg differential to mempool")
        additions = self.session.execute("SELECT tx_hash FROM temp_mempool_additions")
        batch = BatchStatement()
        for addition in additions:
            batch.add(
                "INSERT INTO mempool_transactions (mp_tx_hash) VALUES (%s)",
                (addition.tx_hash,),
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
            (checkpoint_tip.height, block_hash_bytes, 0),
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
            PROFILING, f"elapsed time for bulk delete of transactions = {t1} seconds for {len(tx_hash_hexes)}"
        )

    def delete_pushdata_rows(self, pushdata_rows: list[PushdataRow]) -> None:
        t0 = time.time()
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        for i, (pushdata_hash, tx_hash, idx, ref_type) in enumerate(pushdata_rows):
            pushdata_bytes = bytes.fromhex(pushdata_hash)
            tx_hash_bytes = bytes.fromhex(tx_hash)
            batch.add(
                "DELETE FROM pushdata WHERE pushdata_hash = %s AND tx_hash = %s AND idx = %s AND ref_type = %s",
                (pushdata_bytes, tx_hash_bytes, idx, ref_type),
            )
            if (i + 1) % BATCH_SIZE == 0 or i == len(pushdata_rows) - 1:
                self.session.execute(batch)
                batch.clear()
        t1 = time.time() - t0
        self.logger.log(
            PROFILING,
            f"elapsed time for bulk delete of pushdata rows = {t1} seconds for {len(pushdata_rows)}",
        )

    def delete_input_rows(self, input_rows: list[InputRow]) -> None:
        t0 = time.time()
        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        for i, (out_tx_hash, out_idx, in_tx_hash, in_idx) in enumerate(input_rows):
            out_tx_hash_bytes = bytes.fromhex(out_tx_hash)
            in_tx_hash_bytes = bytes.fromhex(in_tx_hash)
            batch.add(
                "DELETE FROM inputs_table WHERE out_tx_hash = %s AND out_idx = %s",
                (out_tx_hash_bytes, out_idx),
            )
            if (i + 1) % BATCH_SIZE == 0 or i == len(input_rows) - 1:
                self.session.execute(batch)
                batch.clear()
        t1 = time.time() - t0
        self.logger.log(
            PROFILING, f"elapsed time for bulk delete of input rows = {t1} seconds for {len(input_rows)}"
        )

    def delete_header_rows(self, block_hashes: list[bytes]) -> None:
        t0 = time.time()
        select_statement = self.session.prepare(
            "SELECT block_num FROM headers WHERE block_hash = ? ALLOW FILTERING;"
        )
        results = execute_concurrent_with_args(
            self.session, select_statement, [(block_hash,) for block_hash in block_hashes]
        )
        block_nums = []
        for success, result in results:
            if success:
                block_nums.extend([row.block_num for row in result])
            else:
                self.logger.error(f"Query failed: {result}")

        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        prepared_statement = self.session.prepare("DELETE FROM headers WHERE block_num = ?;")
        for block_num in block_nums:
            bound_statement = prepared_statement.bind([block_num])
            batch.add(bound_statement)
        self.session.execute(batch)

        t1 = time.time() - t0
        self.logger.log(PROFILING, f"elapsed time for delete of header row = {t1} seconds")

    def update_orphaned_headers(self, block_hashes: list[bytes]) -> None:
        select_statement = self.session.prepare(
            "SELECT block_num FROM headers WHERE block_hash = ? ALLOW FILTERING;"
        )
        results = execute_concurrent_with_args(
            self.session, select_statement, [(block_hash,) for block_hash in block_hashes]
        )
        block_nums = []
        for success, result in results:
            if success:
                block_nums.extend([row.block_num for row in result])
            else:
                self.logger.error(f"Query failed: {result}")

        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        for block_num in block_nums:
            query = "UPDATE headers SET is_orphaned = %s WHERE block_num = %s"
            batch.add(query, (1, block_num))
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
        return self.db.cache.r.scard(b"mempool")
