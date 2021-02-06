import os
import shutil
import time
import uuid
from pathlib import Path
from typing import List

import logging

import bitcoinx
from MySQLdb import _mysql

from .mysql_bulk_loads import MySQLBulkLoads
from .mysql_tables import MySQLTables


class MySQLQueries:

    def __init__(self, mysql_conn: _mysql.connection, mysql_tables: MySQLTables, bulk_loads:
            MySQLBulkLoads):
        self.logger = logging.getLogger("mysql-tables")
        self.mysql_conn = mysql_conn
        self.mysql_tables = mysql_tables
        self.bulk_loads = bulk_loads

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
