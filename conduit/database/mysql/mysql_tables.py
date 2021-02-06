import asyncpg
import logging

from MySQLdb import _mysql


class MySQLTables:

    def __init__(self, mysql_conn: _mysql.connection):
        self.logger = logging.getLogger("mysql-tables")
        self.mysql_conn = mysql_conn

    async def mysql_drop_tables(self):
        try:
            queries = [
                "DROP TABLE confirmed_transactions;",
                "DROP TABLE mempool_transactions;",
                "DROP TABLE io_table;",
                "DROP TABLE pushdata;",
                "DROP TABLE api_state",
            ]
            for query in queries:
                self.mysql_conn.query(query)
        except asyncpg.exceptions.UndefinedTableError as e:
            self.logger.exception(e)

    async def mysql_drop_temp_inputs(self):
        try:
            self.mysql_conn.query("""
                DROP TABLE temp_inputs;
            """)
        except Exception as e:
            self.logger.exception("mysql_drop_temp_inbound_tx_hashes failed unexpectedly")

    async def mysql_drop_temp_mined_tx_hashes(self):
        try:
            self.mysql_conn.query("""
                DROP TABLE IF EXISTS temp_mined_tx_hashes;
            """)
        except Exception:
            self.logger.exception("mysql_drop_temp_inbound_tx_hashes failed unexpectedly")

    async def mysql_drop_temp_inbound_tx_hashes(self):
        try:
            self.mysql_conn.query("""
                DROP TABLE IF EXISTS temp_inbound_tx_hashes;
            """)
        except Exception:
            self.logger.exception("mysql_drop_temp_inbound_tx_hashes failed unexpectedly")

    async def mysql_create_permanent_tables(self):
        self.mysql_conn.query("""
            CREATE TABLE IF NOT EXISTS confirmed_transactions (
                tx_shash BIGINT PRIMARY KEY,
                tx_hash BINARY(32),
                tx_height INT,
                tx_position BIGINT,
                tx_offset_start BIGINT,
                tx_offset_end BIGINT,
                tx_has_collided INT
            );
            """)

        self.mysql_conn.query("""
            CREATE TABLE IF NOT EXISTS io_table (
                out_tx_shash BIGINT,
                out_idx INT,
                out_value BIGINT,
                out_has_collided INT,
                in_tx_shash BIGINT,
                in_idx INT,
                in_has_collided INT
            );
            """)

        self.mysql_conn.query("""
            CREATE INDEX IF NOT EXISTS io_idx ON io_table (out_tx_shash, out_idx);
            """)

        # I think I can get away with not storing full pushdata hashes
        # unless they collide because the client provides the full pushdata_hash
        self.mysql_conn.query("""
            CREATE TABLE IF NOT EXISTS pushdata (
                pushdata_shash BIGINT,
                pushdata_hash BINARY(32),
                tx_shash BIGINT,
                idx INT,
                ref_type SMALLINT,
                pd_tx_has_collided INT
            );
            """)

        # NOTE - parsing stage ensures there are no duplicates otherwise would need
        # to do UPSERT which is slow...
        # dropped the tx_shash index and can instead do range scans (for a given
        # pushdata_hash / key history) at lookup time...
        # Things like B:// maybe could be dealt with as special cases perhaps?

        self.mysql_conn.query("""
            CREATE INDEX IF NOT EXISTS pushdata_multi_idx ON pushdata (pushdata_shash);
        """)

        # ?? should this be an in-memory only table?
        # need to store the full tx_hash (albeit non-indexed) because the client
        # may not be providing the tx_hash in their query (e.g. for key history).
        self.mysql_conn.query("""
            CREATE TABLE IF NOT EXISTS mempool_transactions (
                mp_tx_shash BIGINT PRIMARY KEY,
                mp_tx_hash BINARY(32),
                mp_tx_timestamp TIMESTAMP,
                mp_tx_has_collided INT,
                mp_rawtx LONGBLOB
            );
            """)

        self.mysql_conn.query("""
            CREATE TABLE IF NOT EXISTS api_state (
                id INT,
                api_tip_height INT,
                api_tip_hash BINARY(32),
                PRIMARY KEY (id)
            );
            """)

    async def mysql_create_temp_inputs_table(self):
      # Todo - prefix the table name with the worker_id to avoid clashes
      #  currently there is only a single worker
      self.mysql_conn.query("""           
        CREATE TEMPORARY TABLE IF NOT EXISTS temp_inputs (
            in_prevout_shash BIGINT,
            out_idx INT,
            in_tx_shash BIGINT,
            in_idx INT,
            in_has_collided INT
        );
        """)

    async def mysql_create_temp_mined_tx_hashes_table(self):
        self.mysql_conn.query("""
            CREATE TEMPORARY TABLE IF NOT EXISTS temp_mined_tx_hashes (
                mined_tx_hash BINARY(32),
                blk_height BIGINT
            );
            """)

    async def mysql_create_temp_inbound_tx_hashes_table(self):
        self.mysql_conn.query("""
            CREATE TEMPORARY TABLE IF NOT EXISTS temp_inbound_tx_hashes (
                inbound_tx_hashes BINARY(32)
            );
            """)
