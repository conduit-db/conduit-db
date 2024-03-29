# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.

import logging
import os
from typing import cast, Sequence

import MySQLdb

from conduit_lib.constants import HashXLength
from conduit_lib.utils import index_exists


class MySQLTables:
    def __init__(self, conn: MySQLdb.Connection) -> None:
        self.logger = logging.getLogger("mysql-tables")
        self.conn = conn

    def start_transaction(self) -> None:
        self.conn.query("""START TRANSACTION;""")

    def commit_transaction(self) -> None:
        self.conn.query("""COMMIT;""")

    def get_tables(self) -> Sequence[tuple[str]]:
        try:
            self.conn.query("""SHOW TABLES""")
            result = self.conn.store_result()
            return cast(Sequence[tuple[str]], result.fetch_row(0))
        except Exception as e:
            self.logger.exception("drop_temp_inputs failed unexpectedly")
            raise
        finally:
            self.commit_transaction()

    def drop_tables(self) -> None:
        try:
            tables = [
                "checkpoint_state",
                "confirmed_transactions",
                "headers",
                "inputs_table",
                "mempool_transactions",
                "pushdata",
                "temp_mempool_additions",
                "temp_mempool_removals",
                "temp_mined_tx_hashXes",
            ]
            WORKER_COUNT_TX_PARSERS = int(os.getenv("WORKER_COUNT_TX_PARSERS", "4"))
            for worker_id in range(1, WORKER_COUNT_TX_PARSERS + 1):
                tables.append(f'inbound_tx_table_{worker_id}')
            queries = []
            for table_name in tables:
                # table = row[0].decode()
                queries.append(f"DROP TABLE IF EXISTS {table_name}")
            for query in queries:
                self.conn.query(query)
        except Exception as e:
            self.logger.exception("drop_tables failed unexpectedly")
        finally:
            self.commit_transaction()

    def drop_indices(self) -> None:
        try:
            tables = [row[0] for row in self.get_tables()]
            if "confirmed_transactions" in tables:
                self.conn.query("DROP INDEX tx_idx ON confirmed_transactions")
            if "headers" in tables:
                self.conn.query("DROP INDEX headers_idx ON headers;")
                self.conn.query("DROP INDEX headers_idx_height ON headers;")
            if "inputs_table" in tables:
                self.conn.query("DROP INDEX input_idx ON inputs_table;")
            if "pushdata" in tables:
                self.conn.query("DROP INDEX pushdata_idx ON pushdata;")
        except Exception as e:
            self.logger.exception("drop_tables failed unexpectedly")
        finally:
            self.commit_transaction()

    def drop_mempool_table(self) -> None:
        try:
            result = self.get_tables()
            for row in result:
                # table = row[0].decode()
                table = row[0]
                if table == "mempool_transactions":
                    break
            else:
                return

            self.conn.query("""DROP TABLE mempool_transactions;""")
        except Exception as e:
            self.logger.exception("Caught exception")
        finally:
            self.commit_transaction()

    def drop_temp_mined_tx_hashXes(self) -> None:
        try:
            self.conn.query(
                """
                DROP TABLE IF EXISTS temp_mined_tx_hashXes;
            """
            )
        except Exception:
            self.logger.exception("drop_temp_mined_tx_hashXes failed unexpectedly")
        finally:
            self.commit_transaction()

    def drop_temp_inbound_tx_hashXes(self, inbound_tx_table_name: str) -> None:
        try:
            self.start_transaction()
            self.conn.query(
                f"""
                DROP TABLE IF EXISTS {inbound_tx_table_name};
            """
            )
        except Exception:
            self.logger.exception("drop_temp_inbound_tx_hashXes failed unexpectedly")
        finally:
            self.commit_transaction()

    def drop_temp_mempool_removals(self) -> None:
        try:
            self.start_transaction()
            self.conn.query(
                f"""
                DROP TABLE IF EXISTS temp_mempool_removals;
            """
            )
        except Exception:
            self.logger.exception("drop_temp_mempool_removals failed unexpectedly")
        finally:
            self.commit_transaction()

    def drop_temp_mempool_additions(self) -> None:
        try:
            self.start_transaction()
            self.conn.query(
                f"""
                DROP TABLE IF EXISTS temp_mempool_additions;
            """
            )
        except Exception:
            self.logger.exception("drop_temp_mempool_additions failed unexpectedly")
        finally:
            self.commit_transaction()

    def drop_temp_orphaned_txs(self) -> None:
        try:
            self.start_transaction()
            self.conn.query(
                f"""
                DROP TABLE IF EXISTS temp_orphaned_txs;
            """
            )
        except Exception:
            self.logger.exception("drop_temp_orphaned_txs failed unexpectedly")
        finally:
            self.commit_transaction()

    def create_mempool_table(self) -> None:
        try:
            # Note: MEMORY table doesn't support BLOB/TEXT columns - will need to find a different
            # way if we want to cache the mempool full rawtxs.
            self.conn.query(
                f"""
                CREATE TABLE IF NOT EXISTS mempool_transactions (
                    mp_tx_hash BINARY({HashXLength}) PRIMARY KEY
                ) ENGINE=MEMORY DEFAULT CHARSET=latin1;"""
            )
        except Exception:
            self.logger.exception("create_mempool_table failed unexpectedly")
        finally:
            self.commit_transaction()

    # Todo - make all offsets BINARY(5) and tx_position BINARY(5) because this gives enough capacity
    #  for 1 TB block sizes.
    def create_permanent_tables(self) -> None:
        self.create_mempool_table()
        try:
            # tx_offset_start is relative to start of the raw block
            self.conn.query(
                f"""
                CREATE TABLE IF NOT EXISTS confirmed_transactions (
                    tx_hash BINARY({HashXLength}),
                    tx_block_num INT UNSIGNED,
                    tx_position BIGINT UNSIGNED,
                    PRIMARY KEY (tx_hash, tx_block_num)
                ) ENGINE=RocksDB DEFAULT COLLATE=latin1_bin;
                """
            )

            # if not index_exists(self.conn, "tx_idx", "confirmed_transactions"):
            #     self.conn.query("""CREATE INDEX tx_idx ON confirmed_transactions (tx_hash);""")

            # block_offset is relative to start of rawtx
            # this table may look wasteful (due to repetition of the out_tx_hash but the
            # write throughput advantage is considerable (as it avoids the random io burden of
            # updating each row of the combined inputs and outputs table one at a time...)
            # Todo: Maybe I need an autoincrement PK for uniqueness (append-only) + the secondary index on
            #  out_tx_hash... maybe this is why the db size is so huge?
            self.conn.query(
                f"""
                CREATE TABLE IF NOT EXISTS inputs_table (
                    out_tx_hash BINARY({HashXLength}),
                    out_idx INT UNSIGNED,
                    in_tx_hash BINARY({HashXLength}),
                    in_idx INT UNSIGNED
                ) ENGINE=RocksDB DEFAULT COLLATE=latin1_bin;
                """
            )

            if not index_exists(self.conn, "input_idx", "inputs_table"):
                self.conn.query(
                    """
                    CREATE INDEX input_idx ON inputs_table (out_tx_hash, out_idx);
                    """
                )

            # I think I can get away with not storing full pushdata hashes
            # unless they collide because the client provides the full pushdata_hash
            # TODO: If rocksdb were used directly could have all of this as the key, value as null
            #  and use the fixed slice length prefix extractor to get all of the entries for a given
            #  pushdata_hash.
            self.conn.query(
                f"""
                CREATE TABLE IF NOT EXISTS pushdata (
                    pushdata_hash BINARY({HashXLength}),
                    tx_hash BINARY ({HashXLength}),
                    idx INT,
                    ref_type SMALLINT
                ) ENGINE=RocksDB DEFAULT COLLATE=latin1_bin;
                """
            )

            if not index_exists(self.conn, "pushdata_idx", "pushdata"):
                self.conn.query(
                    """
                    CREATE INDEX pushdata_idx ON pushdata (pushdata_hash);
                """
                )

            self.conn.query(
                """
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
                """
            )

            self.conn.query(
                f"""
                CREATE TABLE IF NOT EXISTS headers (
                    block_num INT PRIMARY KEY,
                    block_hash BINARY(32),
                    block_height INT,
                    block_header BINARY(80),
                    block_tx_count BIGINT UNSIGNED,
                    block_size BIGINT UNSIGNED,
                    is_orphaned TINYINT
                ) ENGINE=RocksDB DEFAULT COLLATE=latin1_bin;
                """
            )

            if not index_exists(self.conn, "headers_idx", "headers"):
                self.conn.query(
                    """
                    CREATE INDEX headers_idx ON headers (block_hash);
                    """
                )

            if not index_exists(self.conn, "headers_height_idx", "headers"):
                self.conn.query(
                    """
                    CREATE INDEX headers_height_idx ON headers (block_height);
                    """
                )
        finally:
            self.commit_transaction()

    def create_temp_mined_tx_hashXes_table(self) -> None:
        # TODO(db): Should be able to remove the blk_num field
        try:
            self.conn.query(
                f"""
                CREATE TABLE IF NOT EXISTS temp_mined_tx_hashXes (
                    mined_tx_hash BINARY({HashXLength}) PRIMARY KEY,
                    blk_num BIGINT
                ) ENGINE=MEMORY DEFAULT CHARSET=latin1;
                """
            )
        finally:
            self.commit_transaction()

    def create_temp_mempool_removals_table(self) -> None:
        try:
            self.conn.query(
                f"""
                CREATE TABLE IF NOT EXISTS temp_mempool_removals (
                    tx_hash BINARY({HashXLength}) PRIMARY KEY
                ) ENGINE=MEMORY DEFAULT CHARSET=latin1;
                """
            )
        finally:
            self.commit_transaction()

    def create_temp_mempool_additions_table(self) -> None:
        try:
            self.conn.query(
                f"""
                CREATE TABLE IF NOT EXISTS temp_mempool_additions (
                    tx_hash BINARY({HashXLength}) PRIMARY KEY
                ) ENGINE=MEMORY DEFAULT CHARSET=latin1;
                """
            )
        finally:
            self.commit_transaction()

    def create_temp_orphaned_txs_table(self) -> None:
        try:
            self.conn.query(
                f"""
                CREATE TABLE IF NOT EXISTS temp_orphaned_txs (
                    tx_hash BINARY({HashXLength}) PRIMARY KEY
                ) ENGINE=MEMORY DEFAULT CHARSET=latin1;
                """
            )
        finally:
            self.commit_transaction()

    def create_temp_inbound_tx_hashes_table(self, inbound_tx_table_name: str) -> None:
        try:
            self.conn.query(
                f"""
                CREATE TABLE IF NOT EXISTS {inbound_tx_table_name} (
                    inbound_tx_hash BINARY({HashXLength}) PRIMARY KEY
                ) ENGINE=MEMORY DEFAULT CHARSET=latin1;
                """
            )
        finally:
            self.commit_transaction()

    def initialise_checkpoint_state(self) -> None:
        self.start_transaction()
        try:
            # Initialise checkpoint_state if needed
            self.conn.query("""SELECT * FROM checkpoint_state""")
            result = self.conn.store_result()
            rows = result.fetch_row(0)
            if len(rows) == 0:
                self.conn.query(
                    f"""
                    INSERT INTO checkpoint_state VALUES(0, 0, NULL, false, NULL, NULL, NULL, NULL)
                """
                )
        finally:
            self.commit_transaction()
