import math
import os
import shutil
import time
import uuid
from pathlib import Path
from typing import List, Optional

import logging

import bitcoinx
from MySQLdb import _mysql
from bitcoinx import hash_to_hex_str

from .mysql_bulk_loads import MySQLBulkLoads
from .mysql_tables import MySQLTables
from ...constants import PROFILING


class MySQLQueries:

    def __init__(self, mysql_conn: _mysql.connection, mysql_tables: MySQLTables, bulk_loads:
            MySQLBulkLoads, mysql_db):
        self.logger = logging.getLogger("mysql-tables")
        self.mysql_conn = mysql_conn
        self.mysql_tables = mysql_tables
        self.bulk_loads = bulk_loads
        self.mysql_db = mysql_db

    async def mysql_load_temp_mined_tx_hashes(self, mined_tx_hashes):
        """columns: tx_hashes, blk_height"""
        await self.mysql_tables.mysql_create_temp_mined_tx_hashes_table()

        outfile = Path(str(uuid.uuid4()) + ".csv")
        try:
            string_rows = ["%s,%s\n" % (row) for row in mined_tx_hashes]
            column_names = ['mined_tx_hash', 'blk_height']
            self.bulk_loads._load_data_infile("temp_mined_tx_hashes", string_rows, column_names,
                binary_column_indices=[0])
        finally:
            if os.path.exists(outfile):
                os.remove(outfile)

        # await self.mysql_conn.copy_records_to_table(
        #     "temp_mined_tx_hashes", columns=["mined_tx_hash"], records=mined_tx_hashes,
        # )

    async def mysql_load_temp_inbound_tx_hashes(self, inbound_tx_hashes):
        """columns: tx_hashes, blk_height"""
        await self.mysql_tables.mysql_create_temp_inbound_tx_hashes_table()
        outfile = Path(str(uuid.uuid4()) + ".csv")
        try:
            string_rows = ["%s\n" % (row) for row in inbound_tx_hashes]
            column_names = ['inbound_tx_hashes']
            self.bulk_loads._load_data_infile('temp_inbound_tx_hashes', string_rows, column_names,
                binary_column_indices=[0])
        finally:
            if os.path.exists(outfile):
                os.remove(outfile)

    async def mysql_get_unprocessed_txs(self, new_tx_hashes):
        """
        NOTE: usually (if all mempool txs have been processed, this function will only return
        the coinbase tx)

        Todo(collisions)
        - if there is a collision between local inbound tx shashes and mempool txs then it would
        cause a false negative in thinking it has already been processed
        - solution: use full hashes for correctness
        """
        await self.mysql_load_temp_inbound_tx_hashes(new_tx_hashes)
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
        await self.mysql_tables.mysql_drop_temp_inbound_tx_hashes()
        return result.fetch_row(0)

    # # Debugging
    # async def get_temp_mined_tx_hashes(self):
    #     result: List[Record] = await self.mysql_conn.fetch(
    #         """
    #         SELECT *
    #         FROM temp_mined_tx_hashes;"""
    #     )
    #     self.logger.debug(f"get_temp_mined_tx_hashes: {result}")

    async def check_for_mempool_inbound_collision(self):
        """Need to check for collisions between mempool and confirmed
        tx table (on tx_shashes) - can only do LBYL style here because no constraint
        violations would be raised from the pushdata or io tables.

        Todo(collisions) - If a collision is caught, need to act on it by marking all affected rows
         and inserting to the collision tables
        """
        self.mysql_conn.query(
            """           
            SELECT * 
            FROM mempool_transactions
            INNER JOIN confirmed_transactions
            ON confirmed_transactions.tx_shash = mempool_transactions.mp_tx_shash;
            """
        )
        result = self.mysql_conn.store_result()
        count = result.num_rows()
        assert count == 0, "Collision detected between inbound mempool tx and confirmed tx table!"

    async def mysql_invalidate_mempool_rows(self, api_block_tip_height: int):
        """Need to deal with collisions here"""
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

    async def mysql_update_api_tip_height_and_hash(self, api_tip_height: int, api_tip_hash: bytes):
        assert isinstance(api_tip_height, int)
        assert isinstance(api_tip_hash, bytes)
        api_tip_hash_hex = bitcoinx.hash_to_hex_str(api_tip_hash)
        query = f"""
            INSERT INTO api_state(id, api_tip_height, api_tip_hash) 
            VALUES(0, {api_tip_height}, UNHEX('{api_tip_hash_hex}'))
            ON DUPLICATE KEY UPDATE id=0, api_tip_height={api_tip_height}, 
                api_tip_hash=UNHEX('{api_tip_hash_hex}');
            """
        self.mysql_conn.query(query)

    async def mysql_get_max_tx_height(self) -> Optional[int]:
        query = f"""
            SELECT MAX(tx_height) FROM confirmed_transactions;
            """
        self.mysql_conn.query(query)
        result = self.mysql_conn.store_result()

        result_unpacked = result.fetch_row(0)[0][0]
        if result_unpacked is not None:
            return int(result_unpacked)
        return

    async def mysql_get_txids_above_last_good_height(self, last_good_height: int):
        assert isinstance(last_good_height, int)
        query = f"""
            SELECT tx_shash, tx_hash 
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

    async def mysql_rollback_unsafe_txs(self, unsafe_tx_rows):
        """Todo(rollback) - Probably need to rollback the corresponding pushdata and io table
            entries too."""
        await self.mysql_tables.mysql_create_temp_unsafe_txs_table()
        unsafe_tx_rows = [(int(row[0]), row[1].hex()) for row in unsafe_tx_rows]
        await self.bulk_loads.mysql_bulk_load_temp_unsafe_txs(unsafe_tx_rows)

        # Debugging
        query = f"""
            SELECT * 
            FROM confirmed_transactions
            INNER JOIN temp_unsafe_txs
            ON confirmed_transactions.tx_shash = temp_unsafe_txs.tx_shash
            WHERE tx_has_collided != 0;
            """
        self.mysql_conn.query(query)
        result = self.mysql_conn.store_result()
        count_rows_with_collisions = result.num_rows()
        assert count_rows_with_collisions == 0, "Some of the unsafe tx rows have collisions with " \
            "other rows - this has not been encountered before and needs a patch handle it"
        await self.mysql_tables.mysql_drop_temp_unsafe_txs()

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
                self.logger.debug(f"batch_size for deletion={len(batched_unsafe_txs)}")
                stringified_tx_shashes = ','.join([str(row[0]) for row in batched_unsafe_txs])
                query = f"""
                    DELETE FROM confirmed_transactions
                    WHERE tx_shash in ({stringified_tx_shashes})
                    """
                self.mysql_conn.query(query)
            finally:
                self.mysql_db.commit_transaction()
        t1 = time.time() - t0
        self.logger.log(PROFILING,
            f"elapsed time for bulk delete all unsafe txs = {t1} seconds for "
            f"{len(unsafe_tx_rows)}"
        )
        self.logger.debug("Successfully completed database repair for ")
