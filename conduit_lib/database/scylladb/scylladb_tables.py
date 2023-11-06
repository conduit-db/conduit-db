import logging
import os
from typing import cast, Sequence

import typing

from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from conduit_lib.constants import HashXLength
from conduit_lib.utils import index_exists, get_log_level
from .exceptions import FailedScyllaOperation

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))

if typing.TYPE_CHECKING:
    from .scylladb import ScyllaDatabase


class ScyllaDBTables:
    def __init__(self, scylla_db: "ScyllaDatabase") -> None:
        self.scylla_db = scylla_db
        self.worker_id = self.scylla_db.worker_id
        if self.worker_id:
            self.logger = logging.getLogger(f"scylla-tables-{self.worker_id}")
        else:
            self.logger = logging.getLogger(f"scylla-tables")
        self.scylla_session = self.scylla_db.session
        self.logger.setLevel(get_log_level("conduit_index"))

    def execute_query(self, query: str) -> None:
        """Helper method to execute a query with error handling."""
        try:
            statement = SimpleStatement(query, consistency_level=ConsistencyLevel.LOCAL_QUORUM)
            self.scylla_session.execute(statement)
        except Exception as e:
            self.logger.exception("Failed to execute query: %s", query)
            raise FailedScyllaOperation from e

    def get_tables(self) -> Sequence[tuple[str]]:
        try:
            keyspace = self.scylla_session.keyspace
            rows = self.scylla_session.execute(
                f"SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{keyspace}'"
            )
            return cast(Sequence[tuple[str]], [(row.table_name,) for row in rows])
        except Exception as e:
            self.logger.exception("Failed to get tables from ScyllaDB")
            raise FailedScyllaOperation from e

    def scylla_drop_tables(self) -> None:
        try:
            tables_to_drop = [
                "checkpoint_state",
                "confirmed_transactions",
                "headers",
                "inputs_table",
                "mempool_transactions",
                "pushdata",
                "txo_table",
            ]
            for table_name in tables_to_drop:
                self.execute_query(f"DROP TABLE IF EXISTS {table_name};")
        except Exception as e:
            self.logger.exception("scylla_drop_tables failed unexpectedly")
            raise FailedScyllaOperation from e

    def scylla_drop_indices(self) -> None:
        try:
            # In Scylla, indexes are dropped by name, not by table.column.
            # You need to know the index names beforehand or query system_schema.indexes.
            indexes_to_drop = [
                # "index_name_1",
                # "index_name_2",
                # ... add index names
            ]
            for index_name in indexes_to_drop:
                self.execute_query(f"DROP INDEX IF EXISTS {index_name};")
        except Exception as e:
            self.logger.exception("scylla_drop_indices failed unexpectedly")
            raise FailedScyllaOperation from e

    # ... Add more methods for table and index creation as needed

    # Example method to create a table in Scylla
    def scylla_create_table(self, table_name: str, schema: str) -> None:
        try:
            self.execute_query(f"CREATE TABLE IF NOT EXISTS {table_name} ({schema});")
        except Exception as e:
            self.logger.exception("Failed to create table %s", table_name)
            raise FailedScyllaOperation from e

    def scylla_drop_mempool_table(self) -> None:
        try:
            result = self.get_tables()
            for row in result:
                # table = row[0].decode()
                table = row[0]
                if table == "mempool_transactions":
                    break
            else:
                return

            self.scylla_session.execute("DROP TABLE IF EXISTS mempool_transactions;")
        except Exception as e:
            self.logger.exception("Caught exception")
        finally:
            self.commit_transaction()

    def scylla_drop_temp_mined_tx_hashes(self) -> None:
        try:
            self.scylla_session.execute(
                """
                DROP TABLE IF EXISTS temp_mined_tx_hashes;
                """
            )
        except Exception:
            self.logger.exception("scylla_drop_temp_mined_tx_hashes failed unexpectedly")

    def scylla_drop_temp_inbound_tx_hashes(self, inbound_tx_table_name: str) -> None:
        try:
            self.start_transaction()
            self.scylla_session.execute(
                f"""
                DROP TABLE IF EXISTS {inbound_tx_table_name};
                """
            )
        except Exception:
            self.logger.exception("scylla_drop_temp_inbound_tx_hashes failed unexpectedly")

    def scylla_create_mempool_table(self) -> None:
        try:
            self.scylla_session.execute(
                """
                CREATE TABLE IF NOT EXISTS mempool_transactions (
                    mp_tx_hash BLOB PRIMARY KEY,
                    mp_tx_timestamp INT
                );
                """
            )
        except Exception:
            self.logger.exception("scylla_create_mempool_table failed unexpectedly")

    def scylla_create_permanent_tables(self) -> None:
        # Assuming you have a function scylla_create_mempool_table similar to mysql_create_mempool_table
        self.scylla_create_mempool_table()

        try:
            self.scylla_session.execute(
                f"""
                CREATE TABLE IF NOT EXISTS confirmed_transactions (
                    tx_hash blob,
                    tx_block_num int,
                    tx_position bigint,
                    PRIMARY KEY (tx_hash)
                );
                """
            )

            # Creating secondary index in Scylla is similar to MySQL
            self.scylla_session.execute(
                """
                CREATE INDEX IF NOT EXISTS tx_idx ON confirmed_transactions (tx_block_num);
                """
            )

            self.scylla_session.execute(
                f"""
                CREATE TABLE IF NOT EXISTS txo_table (
                    out_tx_hash blob,
                    out_idx int,
                    out_value bigint,
                    PRIMARY KEY (out_tx_hash, out_idx)
                );
                """
            )

            self.scylla_session.execute(
                f"""
                CREATE TABLE IF NOT EXISTS inputs_table (
                    out_tx_hash blob,
                    out_idx int,
                    in_tx_hash blob,
                    in_idx int,
                    PRIMARY KEY ((out_tx_hash, out_idx), in_tx_hash, in_idx)
                );
                """
            )

            self.scylla_session.execute(
                f"""
                CREATE TABLE IF NOT EXISTS pushdata (
                    pushdata_hash blob,
                    tx_hash blob,
                    idx int,
                    ref_type smallint,
                    PRIMARY KEY (pushdata_hash, tx_hash)
                );
                """
            )

            self.scylla_session.execute(
                """
                CREATE TABLE IF NOT EXISTS checkpoint_state (
                    id int PRIMARY KEY,
                    best_flushed_block_height int,
                    best_flushed_block_hash blob,
                    reorg_was_allocated smallint,
                    first_allocated_block_hash blob,
                    last_allocated_block_hash blob,
                    old_hashes_array blob,
                    new_hashes_array blob
                );
                """
            )

            self.scylla_session.execute(
                f"""
                CREATE TABLE IF NOT EXISTS headers (
                    block_num int PRIMARY KEY,
                    block_hash blob,
                    block_height int,
                    block_header blob,
                    block_tx_count bigint,
                    block_size bigint,
                    is_orphaned tinyint
                );
                """
            )

            # Creating secondary index for block_hash and block_height
            self.scylla_session.execute(
                """
                CREATE INDEX IF NOT EXISTS headers_idx ON headers (block_hash);
                """
            )

            self.scylla_session.execute(
                """
                CREATE INDEX IF NOT EXISTS headers_height_idx ON headers (block_height);
                """
            )
        except Exception:
            self.logger.debug(f"Exception creating tables")

    def scylla_create_temp_mined_tx_hashes_table(self) -> None:
        try:
            self.scylla_session.execute(
                """
                CREATE TABLE IF NOT EXISTS temp_mined_tx_hashes (
                    mined_tx_hash BINARY(32) PRIMARY KEY,
                    blk_num BIGINT
                )
                WITH compression = {}
                AND read_repair_chance = '0'
                AND speculative_retry = 'ALWAYS'
                AND in_memory = 'true'
                AND compaction = { 'class' : 'InMemoryCompactionStrategy' }
                """
            )
        except Exception:
            self.logger.exception("scylla_create_temp_mined_tx_hashes_table failed unexpectedly")

    def scylla_create_temp_inbound_tx_hashes_table(self, inbound_tx_table_name: str):
        try:
            self.scylla_session.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {inbound_tx_table_name} (
                    inbound_tx_hashes BINARY(32) PRIMARY KEY
                )"""
                + """
                WITH compression = {}
                AND read_repair_chance = '0'
                AND speculative_retry = 'ALWAYS'
                AND in_memory = 'true'
                AND compaction = { 'class' : 'InMemoryCompactionStrategy' }
                """
            )
        except Exception:
            self.logger.exception("scylla_create_temp_inbound_tx_hashes_table failed unexpectedly")
