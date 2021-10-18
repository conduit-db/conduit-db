import asyncio
import logging
import os
from concurrent.futures.thread import ThreadPoolExecutor
from functools import partial

import MySQLdb
from MySQLdb import _mysql

from .mysql_bulk_loads import MySQLBulkLoads
from .mysql_queries import MySQLQueries
from .mysql_tables import MySQLTables

from ...constants import PROFILING
from ...utils import is_docker, get_log_level


class MySQLDatabase:

    def __init__(self, mysql_conn: _mysql.connection, worker_id=None):
        self.mysql_conn = mysql_conn
        self.worker_id = worker_id
        self.tables = MySQLTables(self.mysql_conn)
        self.bulk_loads = MySQLBulkLoads(self.mysql_conn, self)
        self.queries = MySQLQueries(self.mysql_conn, self.tables, self.bulk_loads, self)

        self.start_transaction()
        # self.bulk_loads.set_rocks_db_bulk_load_off()
        self.set_myrocks_settings()
        self.commit_transaction()

        self.executor = ThreadPoolExecutor(max_workers=1)
        self.logger = logging.getLogger("mysql-database")
        self.logger.setLevel(get_log_level('conduit_index'))

    def set_myrocks_settings(self):
        # SET global rocksdb_max_subcompactions=8
        settings = f"""SET session rocksdb_max_row_locks={100_000_000};
            SET global rocksdb_write_disable_wal=0;
            SET global rocksdb_wal_recovery_mode=0;
            SET global rocksdb_max_background_jobs=16;
            SET global rocksdb_block_cache_size={1024 ** 3 * 8};"""
        for sql in settings.splitlines(keepends=False):
            self.mysql_conn.query(sql)

    def run_in_executor(self, func, *args, **kwargs):
        # Note: One must be very careful with combining remote logging over TCP and spinning up
        # Executor functions that initialise a new logger instance... it is very costly and slow.
        loop = asyncio.get_running_loop()
        func = partial(func, *args, **kwargs)
        return loop.run_in_executor(self.executor, func)

    def close(self):
        self.mysql_conn.close()

    def start_transaction(self):
        self.mysql_conn.query(
            """START TRANSACTION;"""
        )

    def commit_transaction(self):
        self.mysql_conn.query(
            """COMMIT;"""
        )

    def rollback_transaction(self):
        self.mysql_conn.query(
            """ROLLBACK;"""
        )

    # TABLES
    def mysql_drop_tables(self):
        self.tables.mysql_drop_tables()

    def mysql_drop_temp_mined_tx_hashes(self):
        self.tables.mysql_drop_temp_mined_tx_hashes()

    def mysql_drop_temp_inbound_tx_hashes(self, inbound_tx_table_name):
        self.tables.mysql_drop_temp_inbound_tx_hashes(inbound_tx_table_name)

    def mysql_create_temp_mined_tx_hashes_table(self):
        self.tables.mysql_create_temp_mined_tx_hashes_table()

    def mysql_create_temp_inbound_tx_hashes_table(self, inbound_tx_table_name):
        self.tables.mysql_create_temp_inbound_tx_hashes_table(inbound_tx_table_name)

    # QUERIES
    def mysql_load_temp_mined_tx_hashes(self, mined_tx_hashes):
        self.queries.mysql_load_temp_mined_tx_hashes(mined_tx_hashes)

    def mysql_load_temp_inbound_tx_hashes(self, inbound_tx_hashes):
        self.queries.mysql_load_temp_inbound_tx_hashes(inbound_tx_hashes)

    def mysql_get_unprocessed_txs(self, new_tx_hashes, inbound_tx_table_name):
        return self.queries.mysql_get_unprocessed_txs(new_tx_hashes, inbound_tx_table_name)

    def mysql_invalidate_mempool_rows(self, api_block_tip_height: int):
        self.queries.mysql_invalidate_mempool_rows(api_block_tip_height)

    def mysql_update_api_tip_height_and_hash(self, api_tip_height: int, api_tip_hash: bytes):
        self.queries.mysql_update_api_tip_height_and_hash(api_tip_height, api_tip_hash)

    # BULK LOADS
    def mysql_bulk_load_confirmed_tx_rows(self, tx_rows):
        self.bulk_loads.mysql_bulk_load_confirmed_tx_rows(tx_rows)

    def mysql_bulk_load_mempool_tx_rows(self, tx_rows):
        self.bulk_loads.mysql_bulk_load_mempool_tx_rows(tx_rows)

    def mysql_bulk_load_output_rows(self, out_rows):
        self.bulk_loads.mysql_bulk_load_output_rows(out_rows)

    def mysql_bulk_load_input_rows(self, in_rows):
        self.bulk_loads.mysql_bulk_load_input_rows(in_rows)

    def mysql_bulk_load_pushdata_rows(self, pd_rows):
        self.bulk_loads.mysql_bulk_load_pushdata_rows(pd_rows)


def mysql_connect(worker_id=None) -> MySQLDatabase:
    host = os.environ.get('MYSQL_HOST', '127.0.0.1')
    port = int(os.environ.get('MYSQL_PORT', 52525))  # not 3306 because of docker issues
    user = os.getenv('MYSQL_USER', 'root')
    password = os.getenv('MYSQL_PASSWORD', 'conduitpass')
    database = os.getenv('MYSQL_DATABASE', 'conduitdb')

    conn = MySQLdb.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        local_infile=1
    )
    return MySQLDatabase(conn, worker_id=worker_id)


def mysql_test_connect() -> MySQLDatabase:
    host = os.environ.get('MYSQL_HOST', '127.0.0.1')
    port = int(os.environ.get('MYSQL_PORT', 52525))  # not 3306 because of docker issues
    user = os.getenv('MYSQL_USER', 'root')
    password = os.getenv('MYSQL_PASSWORD', 'conduitpass')
    database = os.getenv('MYSQL_DATABASE', 'conduitdbtesting')

    conn = MySQLdb.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        local_infile=1
    )
    return MySQLDatabase(conn)


def load_mysql_database() -> MySQLDatabase:
    mysql_database = mysql_connect()
    return mysql_database


def load_test_mysql_database() -> MySQLDatabase:
    mysql_database = mysql_test_connect()
    return mysql_database
