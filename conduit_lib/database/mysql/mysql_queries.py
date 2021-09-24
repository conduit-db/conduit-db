import math
import os
import time
import uuid
from pathlib import Path
from typing import Optional, Tuple

import logging

import bitcoinx
from MySQLdb import _mysql

from .mysql_bulk_loads import MySQLBulkLoads
from .mysql_tables import MySQLTables
from ...constants import PROFILING


class MySQLQueries:

    def __init__(self, mysql_conn: _mysql.connection, mysql_tables: MySQLTables, bulk_loads:
            MySQLBulkLoads, mysql_db):
        self.logger = logging.getLogger("mysql-queries")
        self.logger.setLevel(logging.DEBUG)
        self.mysql_conn = mysql_conn
        self.mysql_tables = mysql_tables
        self.bulk_loads = bulk_loads
        self.mysql_db = mysql_db

    def mysql_load_temp_mined_tx_hashes(self, mined_tx_hashes):
        """columns: tx_hashes, blk_height"""
        self.mysql_tables.mysql_create_temp_mined_tx_hashes_table()

        outfile = Path(str(uuid.uuid4()) + ".csv")
        try:
            string_rows = ["%s,%s\n" % (row) for row in mined_tx_hashes]
            column_names = ['mined_tx_hash', 'blk_height']
            self.bulk_loads._load_data_infile("temp_mined_tx_hashes", string_rows, column_names,
                binary_column_indices=[0])
        finally:
            if os.path.exists(outfile):
                os.remove(outfile)

        # self.mysql_conn.copy_records_to_table(
        #     "temp_mined_tx_hashes", columns=["mined_tx_hash"], records=mined_tx_hashes,
        # )

    def mysql_load_temp_inbound_tx_hashes(self, inbound_tx_hashes):
        """columns: tx_hashes, blk_height"""
        self.mysql_tables.mysql_create_temp_inbound_tx_hashes_table()
        outfile = Path(str(uuid.uuid4()) + ".csv")
        try:
            string_rows = ["%s\n" % (row) for row in inbound_tx_hashes]
            column_names = ['inbound_tx_hashes']
            self.bulk_loads._load_data_infile('temp_inbound_tx_hashes', string_rows, column_names,
                binary_column_indices=[0])
        finally:
            if os.path.exists(outfile):
                os.remove(outfile)

    def mysql_get_unprocessed_txs(self, new_tx_hashes) -> Optional[Tuple[bytes, bytes, int, bytes]]:
        """
        NOTE: usually (if all mempool txs have been processed, this function will only return
        the coinbase tx)
        """
        self.mysql_load_temp_inbound_tx_hashes(new_tx_hashes)
        self.mysql_conn.query(
            """
            -- tx_hash ISNULL implies no match therefore not previously processed yet
            SELECT *
            FROM temp_inbound_tx_hashes
            LEFT OUTER JOIN mempool_transactions
            ON (mempool_transactions.mp_tx_hash = temp_inbound_tx_hashes.inbound_tx_hashes)
            WHERE mempool_transactions.mp_tx_hash IS NULL
            ;"""
        )
        result = self.mysql_conn.store_result()
        self.mysql_tables.mysql_drop_temp_inbound_tx_hashes()
        return result.fetch_row(0)

    # # Debugging
    # def get_temp_mined_tx_hashes(self):
    #     result: List[Record] = self.mysql_conn.fetch(
    #         """
    #         SELECT *
    #         FROM temp_mined_tx_hashes;"""
    #     )
    #     self.logger.debug(f"get_temp_mined_tx_hashes: {result}")

    def mysql_invalidate_mempool_rows(self, api_block_tip_height: int):
        """Need to deal with collisions here"""
        # Todo - block height is unreliable.. should be done based on block_hash
        self.logger.debug(f"Deleting mempool txs based on block height")
        query = f"""
            DELETE FROM mempool_transactions
            WHERE mp_tx_hash in (
                SELECT mined_tx_hash 
                FROM temp_mined_tx_hashes 
                WHERE blk_height <= {api_block_tip_height}
            );
            """
        # NOTE: It would have been nice to count the rows that were deleted for accounting
        # purposes but MySQL makes this difficult.
        self.mysql_conn.query(query)
        self.mysql_conn.commit()

    def mysql_update_api_tip_height_and_hash(self, api_tip_height: int, api_tip_hash: bytes):
        assert isinstance(api_tip_height, int)
        assert isinstance(api_tip_hash, bytes)
        api_tip_hash_hex = bitcoinx.hash_to_hex_str(api_tip_hash)
        self.mysql_db.start_transaction()
        query = f"""
            REPLACE INTO api_state(id, api_tip_height, api_tip_hash) 
            VALUES(0, {api_tip_height}, UNHEX('{api_tip_hash_hex}'))
        """
        self.mysql_conn.query(query)
        self.mysql_db.commit_transaction()

    def mysql_get_max_tx_height(self) -> Optional[int]:
        query = f"""
            SELECT MAX(tx_height) FROM confirmed_transactions;
            """
        self.mysql_conn.query(query)
        result = self.mysql_conn.store_result()

        result_unpacked = result.fetch_row(0)[0][0]
        if result_unpacked is not None:
            return int(result_unpacked)
        return

    def mysql_get_txids_above_last_good_height(self, last_good_height: int):
        """This query will do a full table scan and so will not scale - instead would need to
        pull txids by blockhash from merkleproof to get the set of txids..."""
        assert isinstance(last_good_height, int)
        query = f"""
            SELECT tx_hash 
            FROM confirmed_transactions 
            WHERE tx_height >{last_good_height} 
            ORDER BY tx_height DESC;
            """
        # self.logger.debug(query)
        self.mysql_conn.query(query)
        result = self.mysql_conn.store_result()
        count = result.num_rows()
        if count != 0:
            self.logger.warning(f"The database was abruptly shutdown ({count} unsafe txs for "
                f"rollback) - beginning repair process...")
        return result.fetch_row(0)

    def mysql_rollback_unsafe_txs(self, unsafe_tx_rows):
        """Todo(rollback) - Probably need to rollback the corresponding pushdata and io table
            entries too."""
        unsafe_tx_rows = [(row[0].hex(),) for row in unsafe_tx_rows]

        # Deletion is very slow for large batch sizes
        t0 = time.time()
        BATCH_SIZE = 2000
        BATCHES_COUNT = math.ceil(len(unsafe_tx_rows)/BATCH_SIZE)
        for i in range(BATCHES_COUNT):
            self.mysql_db.start_transaction()
            try:
                if i == BATCHES_COUNT - 1:
                    batched_unsafe_txs = unsafe_tx_rows[i*BATCH_SIZE:]
                else:
                    batched_unsafe_txs = unsafe_tx_rows[i*BATCH_SIZE:(i+1)*BATCH_SIZE]
                stringified_tx_hashes = ','.join([f"UNHEX('{(row[0])}')" for row in
                    batched_unsafe_txs])

                query = f"""
                    DELETE FROM confirmed_transactions
                    WHERE tx_hash in ({stringified_tx_hashes})"""
                self.mysql_conn.query(query)
            finally:
                self.mysql_db.commit_transaction()
        t1 = time.time() - t0
        self.logger.log(PROFILING,
            f"elapsed time for bulk delete all unsafe txs = {t1} seconds for "
            f"{len(unsafe_tx_rows)}"
        )
        self.logger.debug(f"Successfully completed database repair")

    def mysql_get_duplicate_tx_hashes(self, tx_rows):
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
