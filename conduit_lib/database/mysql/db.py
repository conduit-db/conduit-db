import bitcoinx
import logging
import os
from concurrent.futures.thread import ThreadPoolExecutor
from typing import TypeVar

import MySQLdb
from MySQLdb.connections import Connection

from .api_queries import MySQLAPIQueries
from .bulk_loads import MySQLBulkLoads
from .queries import MySQLQueries
from .tables import MySQLTables
from .types import (
    PushdataRow,
    InputRow,
    OutputRow,
    ConfirmedTransactionRow,
    MempoolTransactionRow,
    MinedTxHashes,
)
from ...utils import get_log_level

T1 = TypeVar("T1")


class MySQLDatabase:
    def __init__(self, conn: MySQLdb.Connection, worker_id: int | None = None) -> None:
        self.conn = conn  # passed into constructor for easier unit-testing
        self.worker_id = worker_id
        self.tables = MySQLTables(self.conn)
        self.bulk_loads = MySQLBulkLoads(self.conn, self)
        self.queries = MySQLQueries(self.conn, self.tables, self.bulk_loads, self)
        self.api_queries = MySQLAPIQueries(self.conn, self.tables, self)

        self.start_transaction()
        # self.bulk_loads.set_rocks_db_bulk_load_off()
        self._set_myrocks_settings()
        self.commit_transaction()

        self.executor = ThreadPoolExecutor(max_workers=1)
        self.logger = logging.getLogger("mysql-database")
        self.logger.setLevel(get_log_level("conduit_index"))

    def _set_myrocks_settings(self) -> None:
        # SET global rocksdb_max_subcompactions=8
        settings = f"""SET global rocksdb_max_row_locks={100_000_000};
            SET global rocksdb_write_disable_wal=0;
            SET global rocksdb_wal_recovery_mode=0;
            SET global rocksdb_max_background_jobs=16;
            SET global rocksdb_block_cache_size={1024 ** 3 * 8};"""
        for sql in settings.splitlines(keepends=False):
            self.conn.query(sql)

    def close(self) -> None:
        self.conn.close()

    def start_transaction(self) -> None:
        self.conn.query("""START TRANSACTION;""")

    def commit_transaction(self) -> None:
        self.conn.query("""COMMIT;""")

    def rollback_transaction(self) -> None:
        self.conn.query("""ROLLBACK;""")

    # TABLES
    def drop_tables(self) -> None:
        self.tables.drop_tables()

    def drop_temp_mined_tx_hashes(self) -> None:
        self.tables.drop_temp_mined_tx_hashes()

    def drop_temp_inbound_tx_hashes(self, inbound_tx_table_name: str) -> None:
        self.tables.drop_temp_inbound_tx_hashes(inbound_tx_table_name)

    def create_temp_mined_tx_hashes_table(self) -> None:
        self.tables.create_temp_mined_tx_hashes_table()

    def create_temp_inbound_tx_hashes_table(self, inbound_tx_table_name: str) -> None:
        self.tables.create_temp_inbound_tx_hashes_table(inbound_tx_table_name)

    # QUERIES
    def load_temp_mined_tx_hashes(self, mined_tx_hashes: list[MinedTxHashes]) -> None:
        self.queries.load_temp_mined_tx_hashes(mined_tx_hashes)

    def load_temp_inbound_tx_hashes(
        self, inbound_tx_hashes: list[tuple[str]], inbound_tx_table_name: str
    ) -> None:
        self.queries.load_temp_inbound_tx_hashes(inbound_tx_hashes, inbound_tx_table_name)

    def get_unprocessed_txs(
        self,
        is_reorg: bool,
        new_tx_hashes: list[tuple[str]],
        inbound_tx_table_name: str,
    ) -> set[bytes]:
        return self.queries.get_unprocessed_txs(is_reorg, new_tx_hashes, inbound_tx_table_name)

    def invalidate_mempool_rows(self) -> None:
        self.queries.invalidate_mempool_rows()

    def update_checkpoint_tip(self, checkpoint_tip: bitcoinx.Header) -> None:
        self.queries.update_checkpoint_tip(checkpoint_tip)

    # BULK LOADS
    def bulk_load_confirmed_tx_rows(self, tx_rows: list[ConfirmedTransactionRow]) -> None:
        self.bulk_loads.bulk_load_confirmed_tx_rows(tx_rows)

    def bulk_load_mempool_tx_rows(self, tx_rows: list[MempoolTransactionRow]) -> None:
        self.bulk_loads.bulk_load_mempool_tx_rows(tx_rows)

    def bulk_load_output_rows(self, out_rows: list[OutputRow]) -> None:
        self.bulk_loads.bulk_load_output_rows(out_rows)

    def bulk_load_input_rows(self, in_rows: list[InputRow]) -> None:
        self.bulk_loads.bulk_load_input_rows(in_rows)

    def bulk_load_pushdata_rows(self, pd_rows: list[PushdataRow]) -> None:
        self.bulk_loads.bulk_load_pushdata_rows(pd_rows)


def get_connection() -> Connection:
    host = os.environ.get("MYSQL_HOST", "127.0.0.1")
    port = int(os.environ.get("MYSQL_PORT", 52525))  # not 3306 because of docker issues
    user = os.getenv("MYSQL_USER", "root")
    password = os.getenv("MYSQL_PASSWORD", "conduitpass")
    database = os.getenv("MYSQL_DATABASE", "conduitdb")

    conn: MySQLdb.Connection = MySQLdb.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        local_infile=1,
    )
    return conn


def connect(worker_id: int | None = None) -> MySQLDatabase:
    conn = get_connection()
    return MySQLDatabase(conn, worker_id=worker_id)


load_database = connect
