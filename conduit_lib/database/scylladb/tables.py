import logging
import os
from typing import cast, Sequence
import typing

from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

from conduit_lib.utils import get_log_level
from .exceptions import FailedScyllaOperation

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))

if typing.TYPE_CHECKING:
    from .db import ScyllaDB


KEYSPACE = 'conduitdb'


class ScyllaDBTables:
    def __init__(self, db: "ScyllaDB") -> None:
        self.db = db
        self.worker_id = self.db.worker_id
        if self.worker_id:
            self.logger = logging.getLogger(f"scylla-tables-{self.worker_id}")
        else:
            self.logger = logging.getLogger(f"scylla-tables")
        self.session = db.session
        self.logger.setLevel(get_log_level("conduit_index"))

    def execute_query(self, query: str) -> None:
        """Helper method to execute a query with error handling."""
        try:
            statement = SimpleStatement(query, consistency_level=ConsistencyLevel.LOCAL_QUORUM)
            self.db.session.execute(statement)
        except Exception as e:
            self.logger.exception("Failed to execute query: %s", query)
            raise FailedScyllaOperation from e

    def get_tables(self) -> Sequence[tuple[str]]:
        try:
            keyspace = self.db.session.keyspace
            rows = self.db.session.execute(
                f"SELECT table_name FROM system_schema.tables WHERE keyspace_name = '{keyspace}'"
            )
            return cast(Sequence[tuple[str]], [(row.table_name,) for row in rows])
        except Exception as e:
            self.logger.exception("Failed to get tables from ScyllaDB")
            raise FailedScyllaOperation from e

    def drop_tables(self) -> None:
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
            self.logger.exception("db.drop_tables failed unexpectedly")
            raise FailedScyllaOperation from e

    def drop_indices(self) -> None:
        try:
            # Query ScyllaDB's system schema to get the names of all indices
            index_query_result = self.session.execute(
                "SELECT index_name FROM system_schema.indexes WHERE keyspace_name = 'your_keyspace_name';"
            )

            indexes_to_drop = [row.index_name for row in index_query_result if
                row.index_name is not None]

            for index_name in indexes_to_drop:
                self.execute_query(f"DROP INDEX IF EXISTS {index_name};")

        except Exception as e:
            self.logger.exception("drop_indices failed unexpectedly")
            raise FailedScyllaOperation from e

    def create_table(self, table_name: str, schema: str) -> None:
        try:
            self.execute_query(f"CREATE TABLE IF NOT EXISTS {table_name} ({schema});")
        except Exception as e:
            self.logger.exception("Failed to create table %s", table_name)
            raise FailedScyllaOperation from e

    def drop_temp_mined_tx_hashes(self) -> None:
        self.db.cache.bulk_delete_in_namespace(b"temp_mined_tx_hashes")

    def drop_temp_inbound_tx_hashes(self, inbound_tx_table_name: str) -> None:
        self.db.cache.bulk_delete_in_namespace(b"temp_inbound_tx_hashes" + f"_{inbound_tx_table_name}".encode())

    def drop_temp_mempool_removals(self) -> None:
        self.db.cache.bulk_delete_in_namespace(b"temp_mempool_removals")

    def drop_temp_mempool_additions(self) -> None:
        self.db.cache.bulk_delete_in_namespace(b"temp_mempool_additions")

    def drop_temp_orphaned_txs(self) -> None:
        self.db.cache.bulk_delete_in_namespace(b"temp_orphaned_txs")

    def create_permanent_tables(self) -> None:
        self.session.execute(f"CREATE KEYSPACE IF NOT EXISTS {KEYSPACE} "
                             "WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }")
        self.session.set_keyspace(KEYSPACE)

        try:
            self.session.execute(
                f"""
                CREATE TABLE IF NOT EXISTS confirmed_transactions (
                    tx_hash blob,
                    tx_block_num int,
                    tx_position bigint,
                    PRIMARY KEY (tx_hash, tx_block_num)
                );
                """
            )

            self.session.execute(
                f"""
                CREATE TABLE IF NOT EXISTS txo_table (
                    out_tx_hash blob,
                    out_idx int,
                    out_value bigint,
                    PRIMARY KEY (out_tx_hash, out_idx)
                );
                """
            )

            self.session.execute(
                f"""
                CREATE TABLE IF NOT EXISTS inputs_table (
                    out_tx_hash blob,
                    out_idx int,
                    in_tx_hash blob,
                    in_idx int,
                    PRIMARY KEY (out_tx_hash, out_idx)
                );
                """
            )

            self.session.execute(
                f"""
                CREATE TABLE IF NOT EXISTS pushdata (
                    pushdata_hash blob,
                    tx_hash blob,
                    idx int,
                    ref_type smallint,
                    PRIMARY KEY (pushdata_hash, tx_hash, idx, ref_type)
                );
                """
            )

            self.session.execute(
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

            self.session.execute(
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
            self.session.execute(
                """
                CREATE INDEX IF NOT EXISTS headers_idx ON headers (block_hash);
                """
            )

            self.session.execute(
                """
                CREATE INDEX IF NOT EXISTS headers_height_idx ON headers (block_height);
                """
            )
        except Exception:
            self.logger.debug(f"Exception creating tables")

    def initialise_checkpoint_state(self) -> None:
        try:
            rows = list(self.session.execute("SELECT * FROM checkpoint_state"))
            if len(rows) == 0:
                self.session.execute(
                    """
                    INSERT INTO checkpoint_state (
                        id,
                        best_flushed_block_height,
                        best_flushed_block_hash,
                        reorg_was_allocated,
                        first_allocated_block_hash,
                        last_allocated_block_hash,
                        old_hashes_array,
                        new_hashes_array
                    ) VALUES (
                        0, 0, NULL, 0, NULL, NULL, NULL, NULL
                    )
                    """
                )
        except Exception:
            self.logger.exception("initialise_checkpoint_state failed unexpectedly")

