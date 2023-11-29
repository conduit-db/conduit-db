import time

import bitcoinx
import logging
import os
from concurrent.futures.thread import ThreadPoolExecutor
from typing import TypeVar, Sequence, Iterator

import MySQLdb
from MySQLdb.connections import Connection

from .api_queries import MySQLAPIQueries
from .bulk_loads import MySQLBulkLoads
from .queries import MySQLQueries
from .tables import MySQLTables
from .tip_filtering import MySQLTipFilterQueries
from ..db_interface.db import DBInterface, DatabaseType
from ..db_interface.tip_filter_types import OutputSpendRow
from ..db_interface.types import (
    MinedTxHashes,
    ConfirmedTransactionRow,
    MempoolTransactionRow,
    InputRow,
    PushdataRow,
)
from ... import LMDB_Database
from ...types import ChainHashes, BlockHeaderRow, TxMetadata, RestorationFilterQueryResult, OutpointType
from ...utils import get_log_level

T1 = TypeVar("T1")


class MySQLDatabase(DBInterface):
    def __init__(self, conn: MySQLdb.Connection, worker_id: int | str | None = None) -> None:
        self.conn = conn  # passed into constructor for easier unit-testing
        assert self.conn is not None
        self.worker_id = worker_id
        self.db_type = DatabaseType.MySQL
        self.tables = MySQLTables(self.conn)
        self.bulk_loads = MySQLBulkLoads(self.conn, self)
        self.queries = MySQLQueries(self.conn, self.tables, self.bulk_loads, self)
        self.api_queries = MySQLAPIQueries(self.conn, self.tables, self)
        self.tip_filter_api = MySQLTipFilterQueries(self)

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
        assert self.conn is not None
        for sql in settings.splitlines(keepends=False):
            self.conn.query(sql)

    def close(self) -> None:
        assert self.conn is not None
        self.conn.close()

    def start_transaction(self) -> None:
        assert self.conn is not None
        self.conn.query("""START TRANSACTION;""")

    def commit_transaction(self) -> None:
        assert self.conn is not None
        self.conn.query("""COMMIT;""")

    def rollback_transaction(self) -> None:
        assert self.conn is not None
        self.conn.query("""ROLLBACK;""")

    def ping(self) -> None:
        assert self.conn is not None
        self.conn.ping()

    def maybe_refresh_connection(self, last_activity: int, logger: logging.Logger) -> tuple[DBInterface, int]:
        REFRESH_TIMEOUT = 600
        if int(time.time()) - last_activity > REFRESH_TIMEOUT:
            logger.info(f"Refreshing DB connection due to {REFRESH_TIMEOUT} " f"second refresh timeout")
            self.ping()
            last_activity = int(time.time())
            return self, last_activity
        else:
            return self, last_activity

    # TABLES
    def drop_tables(self) -> None:
        self.tables.drop_tables()

    def create_keyspace(self) -> None:
        pass  # purely for compatibility with DBInterface

    def drop_keyspace(self) -> None:
        pass  # purely for compatibility with DBInterface

    def create_permanent_tables(self) -> None:
        self.tables.create_permanent_tables()

    def drop_temp_mined_tx_hashes(self) -> None:
        self.tables.drop_temp_mined_tx_hashes()

    def drop_temp_inbound_tx_hashes(self, inbound_tx_table_name: str) -> None:
        self.tables.drop_temp_inbound_tx_hashes(inbound_tx_table_name)

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
    def bulk_load_confirmed_tx_rows(
        self, tx_rows: list[ConfirmedTransactionRow], check_duplicates: bool = False
    ) -> None:
        self.bulk_loads.bulk_load_confirmed_tx_rows(tx_rows, check_duplicates)

    def bulk_load_mempool_tx_rows(self, tx_rows: list[MempoolTransactionRow]) -> None:
        self.bulk_loads.bulk_load_mempool_tx_rows(tx_rows)

    def bulk_load_input_rows(self, in_rows: list[InputRow]) -> None:
        self.bulk_loads.bulk_load_input_rows(in_rows)

    def bulk_load_pushdata_rows(self, pd_rows: list[PushdataRow]) -> None:
        self.bulk_loads.bulk_load_pushdata_rows(pd_rows)

    def get_tables(self) -> Sequence[tuple[str]]:
        return self.tables.get_tables()

    def drop_mempool_table(self) -> None:
        self.tables.drop_mempool_table()

    def create_mempool_table(self) -> None:
        self.tables.create_mempool_table()

    def get_checkpoint_state(
        self,
    ) -> tuple[int, bytes, bool, bytes, bytes, bytes, bytes] | None:
        return self.queries.get_checkpoint_state()

    def initialise_checkpoint_state(self) -> None:
        self.tables.initialise_checkpoint_state()

    def delete_transaction_rows(self, tx_hash_hexes: list[str]) -> None:
        self.queries.delete_transaction_rows(tx_hash_hexes)

    def update_orphaned_headers(self, block_hashes: list[bytes]) -> None:
        self.queries.update_orphaned_headers(block_hashes)

    def load_temp_mempool_additions(self, additions_to_mempool: set[bytes]) -> None:
        self.queries.load_temp_mempool_additions(additions_to_mempool)

    def load_temp_mempool_removals(self, removals_from_mempool: set[bytes]) -> None:
        self.queries.load_temp_mempool_removals(removals_from_mempool)

    def load_temp_orphaned_tx_hashes(self, orphaned_tx_hashes: set[bytes]) -> None:
        self.queries.load_temp_orphaned_tx_hashes(orphaned_tx_hashes)

    def update_allocated_state(
        self,
        reorg_was_allocated: bool,
        first_allocated: bitcoinx.Header,
        last_allocated: bitcoinx.Header,
        old_hashes: ChainHashes | None,
        new_hashes: ChainHashes | None,
    ) -> None:
        self.queries.update_allocated_state(
            reorg_was_allocated, first_allocated, last_allocated, old_hashes, new_hashes
        )

    def drop_temp_orphaned_txs(self) -> None:
        self.tables.drop_temp_orphaned_txs()

    def get_mempool_size(self) -> int:
        return self.queries.get_mempool_size()

    def remove_from_mempool(self) -> None:
        self.queries.remove_from_mempool()

    def add_to_mempool(self) -> None:
        self.queries.add_to_mempool()

    def delete_pushdata_rows(self, pushdata_rows: list[PushdataRow]) -> None:
        pass

    def delete_input_rows(self, input_rows: list[InputRow]) -> None:
        pass

    def delete_header_rows(self, block_hashes: list[bytes]) -> None:
        pass

    def get_header_data(self, block_hash: bytes, raw_header_data: bool = True) -> BlockHeaderRow | None:
        return self.api_queries.get_header_data(block_hash, raw_header_data)

    def get_transaction_metadata_hashX(self, tx_hashX: bytes) -> TxMetadata | None:
        return self.api_queries.get_transaction_metadata_hashX(tx_hashX)

    def get_pushdata_filter_matches(
        self, pushdata_hashXes: list[str]
    ) -> Iterator[RestorationFilterQueryResult]:
        return self.api_queries.get_pushdata_filter_matches(pushdata_hashXes)

    def bulk_load_headers(self, block_header_rows: list[BlockHeaderRow]) -> None:
        return self.bulk_loads.bulk_load_headers(block_header_rows)

    def get_spent_outpoints(self, entries: list[OutpointType], lmdb: LMDB_Database) -> list[OutputSpendRow]:
        return self.api_queries.get_spent_outpoints(entries, lmdb)

    def create_temp_mempool_removals_table(self) -> None:
        self.tables.create_temp_mempool_removals_table()

    def create_temp_mempool_additions_table(self) -> None:
        self.tables.create_temp_mempool_additions_table()

    def create_temp_orphaned_txs_table(self) -> None:
        self.tables.create_temp_orphaned_txs_table()

    def drop_temp_mempool_removals(self) -> None:
        self.tables.drop_temp_mempool_removals()

    def drop_temp_mempool_additions(self) -> None:
        self.tables.drop_temp_mempool_additions()


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


def load_mysql_database(worker_id: int | str | None = None) -> DBInterface:
    logger = logging.getLogger('load_mysql_database')
    while True:
        try:
            conn = get_connection()
            logger.info("MySQL is now available")
            return MySQLDatabase(conn, worker_id=worker_id)
        except MySQLdb.OperationalError:
            logger.warning("MySQL is not available yet")
            time.sleep(2)
            pass
