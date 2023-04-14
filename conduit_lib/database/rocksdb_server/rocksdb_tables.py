import logging
from typing import cast, Sequence

from rocksdict import Rdict

from conduit_lib.constants import HashXLength


class RocksDbTables:

    def __init__(self, db: Rdict) -> None:
        self.logger = logging.getLogger("rocksdb-tables")
        self.db = db

    def start_transaction(self) -> None:
        self.db.query(
            """START TRANSACTION;"""
        )

    def commit_transaction(self) -> None:
        self.db.query(
            """COMMIT;"""
        )

    def get_tables(self) -> Sequence[tuple[str]]:
        try:
            self.db.query("""SHOW TABLES""")
            result = self.db.store_result()
            return cast(Sequence[tuple[str]], result.fetch_row(0))
        except Exception as e:
            self.logger.exception("rocksdb_drop_temp_inputs failed unexpectedly")
            raise
        finally:
            self.commit_transaction()

    def rocksdb_drop_tables(self) -> None:
        try:
            result = ['checkpoint_state', 'confirmed_transactions', 'headers', 'inputs_table',
                'mempool_transactions', 'pushdata', 'txo_table']
            queries = []
            for table_name in result:
                # table = row[0].decode()
                queries.append(f"DROP TABLE IF EXISTS {table_name}")
            for query in queries:
                self.db.query(query)
        except Exception as e:
            self.logger.exception("rocksdb_drop_tables failed unexpectedly")
        finally:
            self.commit_transaction()

    def rocksdb_drop_indices(self) -> None:
        try:
            tables = [row[0] for row in self.get_tables()]
            if "confirmed_transactions" in tables:
                self.db.query("DROP INDEX IF EXISTS tx_idx ON confirmed_transactions")
            if "headers" in tables:
                self.db.query("DROP INDEX IF EXISTS headers_idx ON headers;")
                self.db.query("DROP INDEX IF EXISTS headers_idx_height ON headers;")
            if "txo_table" in tables:
                self.db.query("DROP INDEX IF EXISTS txo_idx ON txo_table;")
            if "inputs_table" in tables:
                self.db.query("DROP INDEX IF EXISTS input_idx ON inputs_table;")
            if "pushdata" in tables:
                self.db.query("DROP INDEX IF EXISTS pushdata_idx ON pushdata;")
        except Exception as e:
            self.logger.exception("rocksdb_drop_tables failed unexpectedly")
        finally:
            self.commit_transaction()

    def rocksdb_drop_mempool_table(self) -> None:
        try:
            result = self.get_tables()
            for row in result:
                # table = row[0].decode()
                table = row[0]
                if table == "mempool_transactions":
                    break
            else:
                return

            self.db.query("""DROP TABLE mempool_transactions;""")
        except Exception as e:
            self.logger.exception("Caught exception")
        finally:
            self.commit_transaction()

    def rocksdb_drop_temp_mined_tx_hashes(self) -> None:
        try:
            self.db.query("""
                DROP TABLE IF EXISTS temp_mined_tx_hashes;
            """)
        except Exception:
            self.logger.exception("rocksdb_drop_temp_mined_tx_hashes failed unexpectedly")
        finally:
            self.commit_transaction()

    def rocksdb_drop_temp_inbound_tx_hashes(self, inbound_tx_table_name: str) -> None:
        try:
            self.start_transaction()
            self.db.query(f"""
                DROP TABLE IF EXISTS {inbound_tx_table_name};
            """)
        except Exception:
            self.logger.exception("rocksdb_drop_temp_inbound_tx_hashes failed unexpectedly")
        finally:
            self.commit_transaction()

    def rocksdb_drop_temp_mempool_removals(self) -> None:
        try:
            self.start_transaction()
            self.db.query(f"""
                DROP TABLE IF EXISTS temp_mempool_removals;
            """)
        except Exception:
            self.logger.exception("rocksdb_drop_temp_mempool_removals failed unexpectedly")
        finally:
            self.commit_transaction()

    def rocksdb_drop_temp_mempool_additions(self) -> None:
        try:
            self.start_transaction()
            self.db.query(f"""
                DROP TABLE IF EXISTS temp_mempool_additions;
            """)
        except Exception:
            self.logger.exception("rocksdb_drop_temp_mempool_additions failed unexpectedly")
        finally:
            self.commit_transaction()

    def rocksdb_drop_temp_orphaned_txs(self) -> None:
        try:
            self.start_transaction()
            self.db.query(f"""
                DROP TABLE IF EXISTS temp_orphaned_txs;
            """)
        except Exception:
            self.logger.exception("rocksdb_drop_temp_orphaned_txs failed unexpectedly")
        finally:
            self.commit_transaction()

    def rocksdb_drop_rocksdb_unsafe_txs(self) -> None:
        try:
            self.db.query("""
                DROP TABLE IF EXISTS temp_unsafe_txs;
            """)
        except Exception:
            self.logger.exception("rocksdb_drop_temp_unsafe_txs failed unexpectedly")
        finally:
            self.commit_transaction()

    def rocksdb_create_mempool_table(self) -> None:
        try:
            # Note: MEMORY table doesn't support BLOB/TEXT columns - will need to find a different
            # way if we want to cache the mempool full rawtxs.
            self.db.query(f"""
                CREATE TABLE IF NOT EXISTS mempool_transactions (
                    mp_tx_hash BINARY(32) PRIMARY KEY,
                    mp_tx_timestamp INTEGER
                ) ENGINE=MEMORY DEFAULT CHARSET=latin1;"""
            )
        except Exception:
            self.logger.exception("rocksdb_create_mempool_table failed unexpectedly")
        finally:
            self.commit_transaction()

    # Todo - make all offsets BINARY(5) and tx_position BINARY(5) because this gives enough capacity
    #  for 1 TB block sizes.
    def rocksdb_create_permanent_tables(self) -> None:
        self.rocksdb_create_mempool_table()
        try:
            # tx_offset_start is relative to start of the raw block
            self.db.query(f"""
                CREATE TABLE IF NOT EXISTS confirmed_transactions (
                    tx_hash BINARY({HashXLength}),
                    tx_block_num INT UNSIGNED,
                    tx_position BIGINT UNSIGNED
                ) ENGINE=RocksDB DEFAULT COLLATE=latin1_bin;
                """)

            self.db.query("""
                CREATE INDEX IF NOT EXISTS tx_idx ON confirmed_transactions (tx_hash);
                """)

            # block_offset is relative to start of rawtx
            self.db.query(f"""
                CREATE TABLE IF NOT EXISTS txo_table (
                    out_tx_hash BINARY({HashXLength}),
                    out_idx INT UNSIGNED,
                    out_value BIGINT UNSIGNED
                ) ENGINE=RocksDB DEFAULT COLLATE=latin1_bin;
                """)

            self.db.query("""
                CREATE INDEX IF NOT EXISTS txo_idx ON txo_table (out_tx_hash, out_idx);
                """)

            # block_offset is relative to start of rawtx
            # this table may look wasteful (due to repetition of the out_tx_hash but the
            # write throughput advantage is considerable (as it avoids the random io burden of
            # updating each row of the combined inputs and outputs table one at a time...)
            # Todo: Maybe I need an autoincrement PK for uniqueness (append-only) + the secondary index on
            #  out_tx_hash... maybe this is why the db size is so huge?
            self.db.query(f"""
                CREATE TABLE IF NOT EXISTS inputs_table (
                    out_tx_hash BINARY({HashXLength}),
                    out_idx INT UNSIGNED,
                    in_tx_hash BINARY({HashXLength}),
                    in_idx INT UNSIGNED
                ) ENGINE=RocksDB DEFAULT COLLATE=latin1_bin;
                """)

            self.db.query("""
                CREATE INDEX IF NOT EXISTS input_idx ON inputs_table (out_tx_hash, out_idx);
                """)

            # I think I can get away with not storing full pushdata hashes
            # unless they collide because the client provides the full pushdata_hash
            # TODO: If rocksdb_client were used directly could have all of this as the key, value as null
            #  and use the fixed slice length prefix extractor to get all of the entries for a given
            #  pushdata_hash.
            self.db.query(f"""
                CREATE TABLE IF NOT EXISTS pushdata (
                    pushdata_hash BINARY({HashXLength}),
                    tx_hash BINARY ({HashXLength}),
                    idx INT,
                    ref_type SMALLINT
                ) ENGINE=RocksDB DEFAULT COLLATE=latin1_bin;
                """)

            self.db.query("""
                CREATE INDEX IF NOT EXISTS pushdata_idx ON pushdata (pushdata_hash);
            """)

            self.db.query("""
                CREATE TABLE IF NOT EXISTS checkpoint_state (
                    id INT PRIMARY KEY,
                    best_flushed_block_height INT,
                    best_flushed_block_hash BINARY(32),
                    reorg_was_allocated SMALLINT,
                    first_allocated_block_hash BINARY(32),
                    last_allocated_block_hash BINARY(32),
                    old_hashes_array BLOB,
                    new_hashes_array BLOB
                ) ENGINE=RocksDB DEFAULT COLLATE=latin1_bin;
                """)

            self.db.query(f"""
                CREATE TABLE IF NOT EXISTS headers (
                    block_num INT PRIMARY KEY,
                    block_hash BINARY(32),
                    block_height INT,
                    block_header BINARY(80),
                    block_tx_count BIGINT UNSIGNED,
                    block_size BIGINT UNSIGNED,
                    is_orphaned TINYINT
                ) ENGINE=RocksDB DEFAULT COLLATE=latin1_bin;
                """)

            self.db.query("""
                CREATE INDEX IF NOT EXISTS headers_idx ON headers (block_hash);
            """)
            self.db.query("""
                CREATE INDEX IF NOT EXISTS headers_idx_height ON headers (block_height);
            """)
        finally:
            self.commit_transaction()

    def rocksdb_create_temp_mined_tx_hashes_table(self) -> None:
        try:
            self.db.query(f"""
                CREATE TABLE IF NOT EXISTS temp_mined_tx_hashes (
                    mined_tx_hash BINARY(32) PRIMARY KEY,
                    blk_num BIGINT
                ) ENGINE=MEMORY DEFAULT CHARSET=latin1;
                """)
        finally:
            self.commit_transaction()

    def rocksdb_create_temp_mempool_removals_table(self) -> None:
        try:
            self.db.query(f"""
                CREATE TABLE IF NOT EXISTS temp_mempool_removals (
                    tx_hash BINARY({HashXLength}) PRIMARY KEY
                ) ENGINE=MEMORY DEFAULT CHARSET=latin1;
                """)
        finally:
            self.commit_transaction()

    def rocksdb_create_temp_mempool_additions_table(self) -> None:
        try:
            self.db.query(f"""
                CREATE TABLE IF NOT EXISTS temp_mempool_additions (
                    tx_hash BINARY({HashXLength}) PRIMARY KEY,
                    tx_timestamp TIMESTAMP
                ) ENGINE=MEMORY DEFAULT CHARSET=latin1;
                """)
        finally:
            self.commit_transaction()

    def rocksdb_create_temp_orphaned_txs_table(self) -> None:
        try:
            self.db.query(f"""
                CREATE TABLE IF NOT EXISTS temp_orphaned_txs (
                    tx_hash BINARY(32) PRIMARY KEY
                ) ENGINE=MEMORY DEFAULT CHARSET=latin1;
                """)
        finally:
            self.commit_transaction()

    def rocksdb_create_temp_inbound_tx_hashes_table(self, inbound_tx_table_name: str) -> None:
        try:
            self.db.query(f"""
                CREATE TABLE IF NOT EXISTS {inbound_tx_table_name} (
                    inbound_tx_hashes BINARY(32) PRIMARY KEY
                ) ENGINE=MEMORY DEFAULT CHARSET=latin1;
                """)
        finally:
            self.commit_transaction()

    def initialise_checkpoint_state(self) -> None:
        self.start_transaction()
        try:
            # Initialise checkpoint_state if needed
            self.db.query("""SELECT * FROM checkpoint_state""")
            result = self.db.store_result()
            rows = result.fetch_row(0)
            if len(rows) == 0:
                self.db.query(f"""
                    INSERT INTO checkpoint_state VALUES(0, 0, NULL, NULL, false, NULL, NULL, NULL)
                """)
        finally:
            self.commit_transaction()
