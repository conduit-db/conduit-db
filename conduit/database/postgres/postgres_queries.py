from typing import List

import asyncpg
import logging

from asyncpg import Record

from .postgres_tables import PostgresTables


class PostgresQueries:

    def __init__(self, pg_conn: asyncpg.Connection, pg_tables: PostgresTables):
        self.logger = logging.getLogger("pg-tables")
        self.pg_conn = pg_conn
        self.pg_tables = pg_tables

    async def pg_load_temp_mined_tx_hashes(self, mined_tx_hashes):
        """columns: tx_hashes, blk_height"""
        await self.pg_tables.pg_create_temp_mined_tx_hashes_table()
        await self.pg_conn.copy_records_to_table(
            "temp_mined_tx_hashes", columns=["mined_tx_hash"], records=mined_tx_hashes,
        )

    async def pg_load_temp_inbound_tx_hashes(self, inbound_tx_hashes):
        """columns: tx_hashes, blk_height"""
        await self.pg_tables.pg_create_temp_inbound_tx_hashes_table()
        await self.pg_conn.copy_records_to_table(
            "temp_inbound_tx_hashes", columns=["inbound_tx_hashes"], records=inbound_tx_hashes,
        )

    async def pg_get_unprocessed_txs(self, new_tx_hashes):
        """
        NOTE: usually (if all mempool txs have been processed, this function will only return
        the coinbase tx)

        Todo(collisions)
        - if there is a collision between local inbound tx shashes and mempool txs then it would
        cause a false negative in thinking it has already been processed
        - solution: use full hashes for correctness
        """
        await self.pg_load_temp_inbound_tx_hashes(new_tx_hashes)
        result: List[Record] = await self.pg_conn.fetch(
            """
            -- tx_hash ISNULL implies no match therefore not previously processed yet
            SELECT *
            FROM temp_inbound_tx_hashes
            LEFT OUTER JOIN mempool_transactions
            ON (mempool_transactions.mp_tx_hash = temp_inbound_tx_hashes.inbound_tx_hashes)
            WHERE mempool_transactions.mp_tx_hash ISNULL
            ;"""
        )
        await self.pg_tables.pg_drop_temp_inbound_tx_hashes()
        return result

    # # Debugging
    # async def get_temp_mined_tx_hashes(self):
    #     result: List[Record] = await self.pg_conn.fetch(
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
        result = await self.pg_conn.execute(
            """           
            SELECT * 
            FROM mempool_transactions
            INNER JOIN confirmed_transactions
            ON confirmed_transactions.tx_shash = mempool_transactions.mp_tx_shash;
            """
        )
        count = int(result.split(" ")[1])
        assert count == 0, "Collision detected between inbound mempool tx and confirmed tx table!"

    async def pg_invalidate_mempool_rows(self, api_block_tip_height: int):
        """Need to deal with collisions here"""
        result = await self.pg_conn.execute(
            """
            DELETE FROM mempool_transactions
            WHERE mp_tx_hash in (
                SELECT mined_tx_hash 
                FROM temp_mined_tx_hashes 
                WHERE temp_mined_tx_hashes.blk_height <= $1
            )
            """,
            api_block_tip_height
        )
        deleted_count = result.split(" ")[1]
        self.logger.debug(f"deleted {deleted_count} mempool rows (now included in a block)")

    async def pg_update_api_tip_height_and_hash(self, api_tip_height: int, api_tip_hash: bytes):
        assert isinstance(api_tip_height, int)
        assert isinstance(api_tip_hash, bytes)
        await self.pg_conn.execute(
            """
            INSERT INTO api_state(id, api_tip_height, api_tip_hash) 
            VALUES(0, $1, $2)
            ON CONFLICT (id)
            DO UPDATE SET id=0, api_tip_height = $1, api_tip_hash=$2;
            """, api_tip_height, api_tip_hash
        )
