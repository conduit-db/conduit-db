import asyncio
import logging
from concurrent.futures.thread import ThreadPoolExecutor
from functools import partial

from MySQLdb import _mysql

from .mysql_bulk_loads import MySQLBulkLoads
from .mysql_queries import MySQLQueries
from .mysql_tables import MySQLTables
from ...constants import PROFILING


class MySQLDatabase:

    def __init__(self, mysql_conn: _mysql.connection):
        self.mysql_conn = mysql_conn
        self.tables = MySQLTables(self.mysql_conn)
        self.bulk_loads = MySQLBulkLoads(self.mysql_conn, self)
        self.queries = MySQLQueries(self.mysql_conn, self.tables, self.bulk_loads, self)

        self.set_myrocks_settings()

        self.executor = ThreadPoolExecutor(max_workers=1)
        self.logger = logging.getLogger("mysql-database")
        self.logger.setLevel(logging.DEBUG)
        self.logger.setLevel(PROFILING)

    def set_myrocks_settings(self):
        settings = f"""SET session rocksdb_max_row_locks={100_000_000};
            SET global rocksdb_write_disable_wal=0;
            SET global rocksdb_wal_recovery_mode=0;
            SET global rocksdb_max_background_jobs=8;
            SET global rocksdb_block_cache_size={1024 ** 3 * 12};"""
        for sql in settings.splitlines(keepends=False):
            self.mysql_conn.query(sql)

    def run_in_executor(self, func, *args, **kwargs):
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

    # TABLES
    def mysql_drop_tables(self):
        self.tables.mysql_drop_tables()

    def mysql_drop_temp_mined_tx_hashes(self):
        self.tables.mysql_drop_temp_mined_tx_hashes()

    def mysql_drop_temp_inbound_tx_hashes(self):
        self.tables.mysql_drop_temp_inbound_tx_hashes()

    def mysql_create_temp_mined_tx_hashes_table(self):
        self.tables.mysql_create_temp_mined_tx_hashes_table()

    def mysql_create_temp_inbound_tx_hashes_table(self):
        self.tables.mysql_create_temp_inbound_tx_hashes_table()

    # QUERIES
    def mysql_load_temp_mined_tx_hashes(self, mined_tx_hashes):
        self.queries.mysql_load_temp_mined_tx_hashes(mined_tx_hashes)

    def mysql_load_temp_inbound_tx_hashes(self, inbound_tx_hashes):
        self.queries.mysql_load_temp_inbound_tx_hashes(inbound_tx_hashes)

    def mysql_get_unprocessed_txs(self, new_tx_hashes):
        return self.queries.mysql_get_unprocessed_txs(new_tx_hashes)

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


def mysql_connect() -> MySQLDatabase:
    conn = _mysql.connect(
        host="127.0.0.1",
        port=3306,
        user="conduitadmin",
        passwd="conduitpass",
        db="conduitdb",
    )
    return MySQLDatabase(conn)


def mysql_test_connect() -> MySQLDatabase:
    conn = _mysql.connect(
        host="127.0.0.1",
        port=3306,
        user="conduitadmin",
        passwd="conduitpass",
        db="conduittestdb"
    )
    return MySQLDatabase(conn)


def load_mysql_database() -> MySQLDatabase:
    # Todo - SHOW PROCESSLIST; -> kill any sleeping connections (zombies from a previous forced
    #  kill)
    mysql_database = mysql_connect()
    return mysql_database


def load_test_mysql_database() -> MySQLDatabase:
    mysql_database = mysql_test_connect()
    mysql_database.mysql_update_settings()
    return mysql_database
