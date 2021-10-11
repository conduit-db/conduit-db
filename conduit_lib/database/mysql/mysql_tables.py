import logging

from MySQLdb import _mysql


class MySQLTables:

    def __init__(self, mysql_conn: _mysql.connection):
        self.logger = logging.getLogger("mysql-tables")
        self.mysql_conn = mysql_conn

    def get_tables(self):
        try:
            self.mysql_conn.query("""SHOW TABLES""")
            result = self.mysql_conn.store_result()
            return result.fetch_row(0)
        except Exception as e:
            self.logger.exception("mysql_drop_temp_inputs failed unexpectedly")

    def mysql_drop_tables(self):
        try:
            result = self.get_tables()
            queries = []
            for row in result:
                # table = row[0].decode()
                table = row[0]
                queries.append(f"DROP TABLE {table};")
            for query in queries:
                self.mysql_conn.query(query)
        except Exception as e:
            self.logger.exception("mysql_drop_temp_inputs failed unexpectedly")

    def mysql_drop_mempool_table(self):
        try:
            result = self.get_tables()
            for row in result:
                # table = row[0].decode()
                table = row[0]
                if table == "mempool_transactions":
                    break
            else:
                return

            self.mysql_conn.query("""DROP TABLE mempool_transactions;""")
        except Exception as e:
            self.logger.exception(e)

    def mysql_drop_temp_mined_tx_hashes(self):
        try:
            self.mysql_conn.query("""
                DROP TABLE IF EXISTS temp_mined_tx_hashes;
            """)
        except Exception:
            self.logger.exception("mysql_drop_temp_mined_tx_hashes failed unexpectedly")

    def mysql_drop_temp_inbound_tx_hashes(self):
        try:
            self.mysql_conn.query("""
                DROP TABLE IF EXISTS temp_inbound_tx_hashes;
            """)
        except Exception:
            self.logger.exception("mysql_drop_temp_inbound_tx_hashes failed unexpectedly")

    def mysql_drop_mysql_afe_txs(self):
        try:
            self.mysql_conn.query("""
                DROP TABLE IF EXISTS temp_unsafe_txs;
            """)
        except Exception:
            self.logger.exception("mysql_drop_temp_unsafe_txs failed unexpectedly")

    # Todo - make all offsets BINARY(5) and tx_position BINARY(5) because this gives enough capacity
    #  for 1 TB block sizes.
    def mysql_create_permanent_tables(self):
        # tx_offset_start is relative to start of the raw block
        self.mysql_conn.query("""
            CREATE TABLE IF NOT EXISTS confirmed_transactions (
                tx_hash BINARY(32) PRIMARY KEY,
                tx_height INT UNSIGNED,
                tx_position BIGINT UNSIGNED,
                tx_offset_start BIGINT UNSIGNED,
                tx_offset_end BIGINT UNSIGNED
            ) ENGINE=RocksDB DEFAULT COLLATE=latin1_bin;
            """)

        # block_offset is relative to start of rawtx
        self.mysql_conn.query("""
            CREATE TABLE IF NOT EXISTS txo_table (
                out_tx_hash BINARY(32),
                out_idx INT UNSIGNED,
                out_value BIGINT UNSIGNED,
                out_offset_start INT UNSIGNED,
                out_offset_end INT UNSIGNED
            ) ENGINE=RocksDB DEFAULT COLLATE=latin1_bin;
            """)

        self.mysql_conn.query("""
            CREATE INDEX IF NOT EXISTS io_idx ON txo_table (out_tx_hash, out_idx);
            """)

        # block_offset is relative to start of rawtx
        # this table may look wasteful (due to repetition of the out_tx_hash but the
        # write throughput advantage is considerable (as it avoids the random io burden of
        # updating each row of the combined inputs and outputs table one at a time...)
        self.mysql_conn.query("""
            CREATE TABLE IF NOT EXISTS inputs_table (
                out_tx_hash BINARY(32),
                out_idx INT UNSIGNED,
                in_tx_hash BINARY(32),
                in_idx INT UNSIGNED,
                in_offset_start INT UNSIGNED,
                in_offset_end INT UNSIGNED
            ) ENGINE=RocksDB DEFAULT COLLATE=latin1_bin;
            """)

        self.mysql_conn.query("""
            CREATE INDEX IF NOT EXISTS io_idx ON inputs_table (out_tx_hash, out_idx);
            """)

        # I think I can get away with not storing full pushdata hashes
        # unless they collide because the client provides the full pushdata_hash
        self.mysql_conn.query("""
            CREATE TABLE IF NOT EXISTS pushdata (
                pushdata_hash BINARY(32),
                tx_hash BINARY (32),
                idx INT,
                ref_type SMALLINT
            ) ENGINE=RocksDB DEFAULT COLLATE=latin1_bin;
            """)

        # NOTE - parsing stage ensures there are no duplicates otherwise would need
        # to do UPSERT which is slow...
        # dropped the tx_shash index and can instead do range scans (for a given
        # pushdata_hash / key history) at lookup time...
        # Things like B:// or Tokens could be dealt with as special cases in their own dedicated
        # table.

        self.mysql_conn.query("""
            CREATE INDEX IF NOT EXISTS pushdata_idx ON pushdata (pushdata_hash);
        """)

        # ?? should this be an in-memory only table?
        # need to store the full tx_hash (albeit non-indexed) because the client
        # may not be providing the tx_hash in their query (e.g. for key history).

        # Note: removed "mp_rawtx LONGBLOB," field because of an error:
        #  MySQLdb._exceptions.OperationalError:
        #  (1163, "Storage engine MEMORY doesn't support BLOB/TEXT columns")
        #  But come to think of it, it may be for the best to fit more txs in memory and the
        #  node (even if pruned) will have the full rawtx for requesting on-demand anyway.
        #  LSM databases are not designed for millions of random deletes every 10 mins!
        #  40,000 deletes takes 3mins 25 seconds with ENGINE=MyRocks but is instant with
        #  ENGINE=MEMORY and USING HASH index.
        self.mysql_conn.query("""
            CREATE TABLE IF NOT EXISTS mempool_transactions (
                mp_tx_hash BINARY(32),
                mp_tx_timestamp TIMESTAMP,
                INDEX USING HASH (mp_tx_hash)
            ) ENGINE=MEMORY DEFAULT CHARSET=latin1;
            """)

        self.mysql_conn.query("""
            CREATE TABLE IF NOT EXISTS api_state (
                id INT PRIMARY KEY,
                api_tip_height INT,
                api_tip_hash BINARY(32)
            );
            """)

    def mysql_create_temp_mined_tx_hashes_table(self):
        self.mysql_conn.query("""
            CREATE TEMPORARY TABLE IF NOT EXISTS temp_mined_tx_hashes (
                mined_tx_hash BINARY(32),
                blk_height BIGINT,
                INDEX USING HASH (mined_tx_hash)
            ) ENGINE=MEMORY DEFAULT CHARSET=latin1;
            """)

    def mysql_create_temp_inbound_tx_hashes_table(self):
        self.mysql_conn.query("""
            CREATE TEMPORARY TABLE IF NOT EXISTS temp_inbound_tx_hashes (
                inbound_tx_hashes BINARY(32),
                INDEX USING HASH (inbound_tx_hashes)
            ) ENGINE=MEMORY DEFAULT CHARSET=latin1;
            """)
