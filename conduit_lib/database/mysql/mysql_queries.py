import math
import os
import time
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Set

import logging

import MySQLdb
import bitcoinx

from .mysql_bulk_loads import MySQLBulkLoads
from .mysql_tables import MySQLTables
from ...constants import PROFILING
from ...types import ChainHashes


class MySQLQueries:

    def __init__(self, mysql_conn: MySQLdb.Connection, mysql_tables: MySQLTables, bulk_loads:
            MySQLBulkLoads, mysql_db):
        self.logger = logging.getLogger("mysql-queries")
        self.logger.setLevel(logging.DEBUG)
        self.mysql_conn = mysql_conn
        self.mysql_tables = mysql_tables
        self.bulk_loads = bulk_loads
        self.mysql_db = mysql_db

    def mysql_load_temp_mined_tx_hashes(self, mined_tx_hashes):
        """columns: tx_hashes, blk_num"""
        self.mysql_tables.mysql_create_temp_mined_tx_hashes_table()

        outfile = Path(str(uuid.uuid4()) + ".csv")
        try:
            string_rows = ["%s,%s\n" % (row) for row in mined_tx_hashes]
            column_names = ['mined_tx_hash', 'blk_num']
            self.bulk_loads._load_data_infile("temp_mined_tx_hashes", string_rows, column_names,
                binary_column_indices=[0])
        finally:
            if os.path.exists(outfile):
                os.remove(outfile)

        # self.mysql_conn.copy_records_to_table(
        #     "temp_mined_tx_hashes", columns=["mined_tx_hash"], records=mined_tx_hashes,
        # )

    def mysql_load_temp_inbound_tx_hashes(self, inbound_tx_hashes: list[tuple[str]],
            inbound_tx_table_name: str):
        """columns: tx_hashes, blk_height"""
        self.mysql_tables.mysql_create_temp_inbound_tx_hashes_table(inbound_tx_table_name)
        string_rows = ["%s\n" % (row) for row in inbound_tx_hashes]
        column_names = ['inbound_tx_hashes']
        self.bulk_loads._load_data_infile(f'{inbound_tx_table_name}', string_rows, column_names,
            binary_column_indices=[0])


    def mysql_get_unprocessed_txs(self, is_reorg: bool, new_tx_hashes: list[tuple[str]],
            inbound_tx_table_name: str) -> Set[bytes]:
        """
        NOTE: usually (if all mempool txs have been processed, this function will only return
        the coinbase tx)
        """
        self.mysql_load_temp_inbound_tx_hashes(new_tx_hashes, inbound_tx_table_name)
        try:
            self.mysql_conn.query(
                f"""
                -- tx_hash ISNULL implies no match therefore not previously processed yet
                SELECT *
                FROM {inbound_tx_table_name}
                LEFT OUTER JOIN mempool_transactions
                ON (mempool_transactions.mp_tx_hash = {inbound_tx_table_name}.inbound_tx_hashes)
                WHERE mempool_transactions.mp_tx_hash IS NULL
                ;"""
            )
            result = self.mysql_conn.store_result()
            final_result = set(x[0] for x in result.fetch_row(0))
        finally:
            self.mysql_db.commit_transaction()

        if not is_reorg:
            self.mysql_tables.mysql_drop_temp_inbound_tx_hashes(inbound_tx_table_name)
            return final_result
        else:
            # Todo - need the full set of orphaned transactions to be subtracted from end result
            try:
                self.mysql_conn.query(f"""SELECT * FROM temp_orphaned_txs;""")
                # Todo - Where tx_hash = inbound_tx_table_name.inbound_tx_hashes
                result = self.mysql_conn.store_result()
                orphaned_txs = set(x[0] for x in result.fetch_row(0))
            finally:
                self.mysql_db.commit_transaction()

            final_result = final_result - orphaned_txs
            self.mysql_tables.mysql_drop_temp_inbound_tx_hashes(inbound_tx_table_name)
            return set(final_result)

    # # Debugging
    # def get_temp_mined_tx_hashes(self):
    #     result: List[Record] = self.mysql_conn.fetch(
    #         """
    #         SELECT *
    #         FROM temp_mined_tx_hashes;"""
    #     )
    #     self.logger.debug(f"get_temp_mined_tx_hashes: {result}")

    def mysql_invalidate_mempool_rows(self):
        self.logger.debug(f"Deleting mined mempool txs")
        query = f"""
            DELETE FROM mempool_transactions
            WHERE mp_tx_hash in (
                SELECT mined_tx_hash 
                FROM temp_mined_tx_hashes 
            );
            """
        # NOTE: It would have been nice to count the rows that were deleted for accounting
        # purposes but MySQL makes this difficult.
        self.mysql_conn.query(query)
        self.mysql_conn.commit()

    def mysql_load_temp_mempool_removals(self, removals_from_mempool: List[bytes]) -> None:
        """i.e. newly mined transactions in a reorg context"""
        self.mysql_tables.mysql_create_temp_mempool_removals_table()

        outfile = Path(str(uuid.uuid4()) + ".csv")
        try:
            string_rows = ["%s\n" % (tx_hash.hex()) for tx_hash in removals_from_mempool]
            column_names = ['tx_hash']
            self.bulk_loads._load_data_infile("temp_mempool_removals", string_rows, column_names,
                binary_column_indices=[0])
        finally:
            if os.path.exists(outfile):
                os.remove(outfile)

    def mysql_load_temp_mempool_additions(self, additions_to_mempool: List[bytes]) -> None:
        self.mysql_tables.mysql_create_temp_mempool_additions_table()

        dt = datetime.utcnow()
        outfile = Path(str(uuid.uuid4()) + ".csv")
        try:
            string_rows = ["%s,%s\n" % (tx_hash.hex(), dt.isoformat()) for tx_hash in additions_to_mempool]
            column_names = ['tx_hash', 'tx_timestamp']
            self.bulk_loads._load_data_infile("temp_mempool_additions", string_rows, column_names,
                binary_column_indices=[0])
        finally:
            if os.path.exists(outfile):
                os.remove(outfile)

    def mysql_load_temp_orphaned_tx_hashes(self, orphaned_tx_hashes: List[bytes]) -> None:
        self.mysql_tables.mysql_create_temp_orphaned_txs_table()

        outfile = Path(str(uuid.uuid4()) + ".csv")
        try:
            string_rows = ["%s\n" % tx_hash.hex() for tx_hash in orphaned_tx_hashes]
            column_names = ['tx_hash']
            self.bulk_loads._load_data_infile("temp_orphaned_txs", string_rows, column_names,
                binary_column_indices=[0])
        finally:
            if os.path.exists(outfile):
                os.remove(outfile)

    def mysql_remove_from_mempool(self):
        self.logger.debug(f"Removing reorg differential from mempool")
        # Note the loading of the temp_mempool_removals is not done here
        query = f"""
            DELETE FROM mempool_transactions
            WHERE mempool_transactions.mp_tx_hash in (
                SELECT tx_hash 
                FROM temp_mempool_removals 
            );
            """
        self.mysql_conn.query(query)
        self.mysql_conn.commit()
        self.mysql_tables.mysql_drop_temp_mempool_removals()

    def mysql_add_to_mempool(self):
        self.logger.debug(f"Adding reorg differential to mempool")
        # Note the loading of the temp_mempool_additions is not done here
        query = f"""
            INSERT INTO mempool_transactions 
                SELECT tx_hash, tx_timestamp
                FROM temp_mempool_additions;
            """
        self.mysql_conn.query(query)
        self.mysql_conn.commit()
        self.mysql_tables.mysql_drop_temp_mempool_additions()

    def mysql_update_checkpoint_tip(self, checkpoint_tip: bitcoinx.Header):
        try:
            assert isinstance(checkpoint_tip, bitcoinx.Header)
            self.mysql_db.start_transaction()
            query = f"""
                UPDATE checkpoint_state
                SET id = 0, 
                    best_flushed_block_height = {checkpoint_tip.height}, 
                    best_flushed_block_hash = X'{checkpoint_tip.hash.hex()}'
                WHERE id = 0;
            """
            self.mysql_conn.query(query)
        finally:
            self.mysql_db.commit_transaction()

    def mysql_get_checkpoint_state(self) -> Optional[tuple[int, bytes, bool, bytes, bytes, bytes,
            bytes]]:
        """We 'allocate' work up to a given block hash and then we set the new checkpoint only
        after every block in the batch is successfully flushed"""
        try:
            query = f"""SELECT * FROM checkpoint_state;"""
            self.mysql_conn.query(query)
            result = self.mysql_conn.store_result()
            rows = result.fetch_row(0)
            if len(rows) != 0:
                row = rows[0]
                best_flushed_block_height = row[1]
                best_flushed_block_hash = row[2]
                reorg_was_allocated = row[3]
                first_allocated_block_hash = row[4]
                last_allocated_block_hash = row[5]
                old_hashes_array = row[6]
                new_hashes_array = row[7]
                return best_flushed_block_height, best_flushed_block_hash, reorg_was_allocated, \
                    first_allocated_block_hash, last_allocated_block_hash, old_hashes_array, \
                    new_hashes_array
            else:
                return None
        finally:
            self.mysql_db.commit_transaction()

    def mysql_get_txids_above_last_good_block_num(self, last_good_block_num: int):
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
            self.mysql_conn.query(query)
            result = self.mysql_conn.store_result()
            count = result.num_rows()
            if count != 0:
                self.logger.warning(f"The database was abruptly shutdown ({count} unsafe txs for "
                    f"rollback) - beginning repair process...")
            return result.fetch_row(0)
        finally:
            self.mysql_db.commit_transaction()

    def mysql_delete_transaction_rows(self, tx_hash_hexes: List[str]):
        # Deletion is very slow for large batch sizes
        t0 = time.time()
        BATCH_SIZE = 2000
        BATCHES_COUNT = math.ceil(len(tx_hash_hexes)/BATCH_SIZE)
        for i in range(BATCHES_COUNT):
            self.mysql_db.start_transaction()
            try:
                if i == BATCHES_COUNT - 1:
                    tx_hash_hexes_batch = tx_hash_hexes[i*BATCH_SIZE:]
                else:
                    tx_hash_hexes_batch = tx_hash_hexes[i*BATCH_SIZE:(i+1)*BATCH_SIZE]
                stringified_tx_hash_hexes = ','.join(
                    [f"X'{tx_hash_hex}'" for tx_hash_hex in tx_hash_hexes_batch])

                query = f"""
                    DELETE FROM confirmed_transactions
                    WHERE tx_hash in ({stringified_tx_hash_hexes})"""
                self.mysql_conn.query(query)
            finally:
                self.mysql_db.commit_transaction()
        t1 = time.time() - t0
        self.logger.log(PROFILING,
            f"elapsed time for bulk delete of transactions = {t1} seconds for "
            f"{len(tx_hash_hexes)}"
        )

    def mysql_delete_pushdata_rows(self, pushdata_rows):
        t0 = time.time()
        cursor: MySQLdb.cursors.Cursor = self.mysql_conn.cursor()
        cursor.execute("START TRANSACTION;")
        try:
            for pushdata_hash, tx_hash, idx, ref_type in pushdata_rows:
                query = f"""
                    DELETE FROM pushdata
                    WHERE pushdata_hash = X'{pushdata_hash}'
                        AND tx_hash = X'{tx_hash}'
                        AND idx = {idx}
                        AND ref_type = {ref_type}"""
                cursor.execute(query)
        finally:
            cursor.execute("COMMIT;")
        t1 = time.time() - t0
        self.logger.log(PROFILING,
            f"elapsed time for bulk delete of pushdata rows = {t1} seconds for "
            f"{len(pushdata_rows)}"
        )

    def mysql_delete_output_rows(self, output_rows):
        t0 = time.time()
        cursor: MySQLdb.cursors.Cursor = self.mysql_conn.cursor()
        cursor.execute("START TRANSACTION;")
        try:
            for out_tx_hash, out_idx, out_value in output_rows:
                query = f"""
                    DELETE FROM txo_table
                    WHERE out_tx_hash = X'{out_tx_hash}' AND out_idx = {out_idx}"""
                cursor.execute(query)
        finally:
            cursor.execute("COMMIT;")
        t1 = time.time() - t0
        self.logger.log(PROFILING,
            f"elapsed time for bulk delete of output rows = {t1} seconds for "
            f"{len(output_rows)}"
        )

    def mysql_delete_input_rows(self, input_rows):
        t0 = time.time()
        cursor: MySQLdb.cursors.Cursor = self.mysql_conn.cursor()
        cursor.execute("START TRANSACTION;")
        try:
            for out_tx_hash, out_idx, in_tx_hash, in_idx in input_rows:
                query = f"""
                    DELETE FROM inputs_table
                    WHERE out_tx_hash = X'{out_tx_hash}' AND out_idx = {out_idx}
                        AND in_tx_hash=X'{in_tx_hash}' and in_idx={in_idx}"""
                cursor.execute(query)
        finally:
            cursor.execute("COMMIT;")
        t1 = time.time() - t0

        self.logger.log(PROFILING,
            f"elapsed time for bulk delete of input rows = {t1} seconds for "
            f"{len(input_rows)}"
        )

    def mysql_delete_header_row(self, block_hash: bytes):
        t0 = time.time()
        self.mysql_db.start_transaction()
        try:
            query = f"""
                DELETE FROM headers
                WHERE block_hash = %s"""
            cursor: MySQLdb.cursors.Cursor = self.mysql_conn.cursor()
            cursor.execute(query, (block_hash,))
        finally:
            self.mysql_db.commit_transaction()
        t1 = time.time() - t0
        self.logger.log(PROFILING,
            f"elapsed time for delete of header row = {t1} seconds")

    def mysql_get_duplicate_tx_hashes(self, tx_rows):
        try:
            candidate_tx_hashes = [(row[0],) for row in tx_rows]

            t0 = time.time()
            BATCH_SIZE = 2000
            BATCHES_COUNT = math.ceil(len(candidate_tx_hashes)/BATCH_SIZE)
            results = []
            for i in range(BATCHES_COUNT):
                if i == BATCHES_COUNT - 1:
                    batched_unsafe_txs = candidate_tx_hashes[i*BATCH_SIZE:]
                else:
                    batched_unsafe_txs = candidate_tx_hashes[i*BATCH_SIZE:(i+1)*BATCH_SIZE]
                stringified_tx_hashes = ','.join([f"UNHEX('{(row[0])}')" for row in
                    batched_unsafe_txs])

                query = f"""
                    SELECT * FROM confirmed_transactions
                    WHERE tx_hash in ({stringified_tx_hashes})"""
                self.mysql_conn.query(query)
                result = self.mysql_conn.store_result()
                for row in result.fetch_row(0):
                    results.append(row)
            t1 = time.time() - t0
            self.logger.log(PROFILING,
                f"elapsed time for selecting duplicate tx_hashes = {t1} seconds for "
                f"{len(candidate_tx_hashes)}"
            )
            return results
        finally:
            self.mysql_db.commit_transaction()

    def mysql_update_oprhaned_headers(self, block_hashes: List[bytes]):
        """This allows us to filter out any query results that do not lie on the longest chain"""
        for block_hash in block_hashes:
            try:
                self.mysql_db.start_transaction()
                query = f"""
                    UPDATE headers
                    SET is_orphaned = 1
                    WHERE block_hash = X'{block_hash.hex()}'
                    """
                self.mysql_conn.query(query)
            finally:
                self.mysql_db.commit_transaction()

    def update_allocated_state(self, reorg_was_allocated: bool, first_allocated: bitcoinx.Header,
            last_allocated: bitcoinx.Header, old_hashes: Optional[ChainHashes],
            new_hashes: Optional[ChainHashes]) -> None:

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
            self.mysql_db.start_transaction()
            if old_hashes and new_hashes is not None:
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
            self.mysql_conn.query(query)
        finally:
            self.mysql_db.commit_transaction()
