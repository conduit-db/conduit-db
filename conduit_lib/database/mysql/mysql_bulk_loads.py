import logging
import os
import sys
import time
import uuid
from pathlib import Path
from typing import List

import MySQLdb
from MySQLdb import _mysql

from ...constants import PROFILING
from ...utils import get_log_level

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))


class MySQLBulkLoads:

    def __init__(self, mysql_conn: _mysql.connection, mysql_db):
        self.mysql_db = mysql_db
        self.worker_id = self.mysql_db.worker_id
        if self.worker_id:
            self.logger = logging.getLogger(f"mysql-tables-{self.worker_id}")
        else:
            self.logger = logging.getLogger(f"mysql-tables")
        self.mysql_conn = mysql_conn

        self.logger.setLevel(get_log_level('conduit_index'))
        self.total_db_time = 0
        self.total_rows_flushed_since_startup = 0  # for current controller
        self.newline_symbol = r"'\r\n'" if sys.platform == 'win32' else r"'\n'"

    def set_local_infile_on(self):
        extra_settings = f"SET @@GLOBAL.local_infile = 1;"
        self.mysql_conn.query(extra_settings)

    def set_rocks_db_bulk_load_on(self):
        self.mysql_db.start_transaction()
        settings = f"""SET global rocksdb_bulk_load_allow_unsorted=0;
            SET global rocksdb_bulk_load=1;"""
        for sql in settings.splitlines(keepends=False):
            self.mysql_conn.query(sql)
        self.mysql_db.commit_transaction()

    def set_rocks_db_bulk_load_off(self):
        self.mysql_db.start_transaction()
        settings = f"""SET global rocksdb_bulk_load=0;
            SET global rocksdb_bulk_load_allow_unsorted=0;"""
        for sql in settings.splitlines(keepends=False):
            self.mysql_conn.query(sql)
        self.mysql_db.commit_transaction()

    def _load_data_infile(self, table_name: str, string_rows: List[str],
            column_names: List[str], binary_column_indices: List[int], have_retried=False):

        t0 = time.time()

        MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
        outfile = Path(MODULE_DIR).parent.parent.parent.parent.parent / "temp_files" / \
                  (str(uuid.uuid4()) + ".csv")
        os.makedirs(os.path.dirname(outfile), exist_ok=True)
        try:
            string_rows.sort()
            with open(outfile, 'w') as csvfile:
                csvfile.writelines(string_rows)

            set_params = []
            query_column_names = column_names.copy()  # So retrying can use the old column_names
            for index, column_name in enumerate(query_column_names):
                if index in binary_column_indices:
                    query_column_names[index] = "@var" + str(index)
                    set_params.append(column_name + f" = UNHEX({query_column_names[index]})")
            set_statement = f"SET " + ",".join(set_params)

            query = f"""
                LOAD DATA LOCAL INFILE '{outfile.absolute().as_posix()}' 
                INTO TABLE {table_name}
                FIELDS TERMINATED BY ','
                LINES TERMINATED BY """ + self.newline_symbol + "\n" + \
                f"""({", ".join(query_column_names)})"""
            if len(binary_column_indices) != 0:
                query += f"\n{set_statement}"

            query += ";"
            self.mysql_db.start_transaction()
            self.mysql_conn.query(query)
            self.mysql_db.commit_transaction()
        except MySQLdb.OperationalError:
            if not have_retried:
                self._load_data_infile(table_name, string_rows, column_names,
                    binary_column_indices, True)
        except Exception:
            self.logger.exception("unexpected exception in _load_data_infile")
            self.mysql_db.rollback_transaction()
            raise
        finally:
            if os.path.exists(outfile):
                os.remove(outfile)
            t1 = time.time() - t0
            self.total_db_time += t1
            self.total_rows_flushed_since_startup += len(string_rows)
            self.logger.log(PROFILING, f"total db flush time={self.total_db_time}")
            self.logger.log(PROFILING, f"total rows flushed since startup "
                                       f"(worker_id={self.worker_id if self.worker_id else None})"
                f"={self.total_rows_flushed_since_startup}")

    def handle_coinbase_dup_tx_hash(self, tx_rows):
        # Todo may need to search the other input/output/pushdata rows too for these problem txids
        # rare issue see: https://en.bitcoin.it/wiki/BIP_0034
        # There are only two cases of duplicate tx_hashes:
        # d5d27987d2a3dfc724e359870c6644b40e497bdc0589a033220fe15429d88599
        # e3bf3d07d4b0375638d5f1db5255fe07ba2c4cb067cd81b84ee974b6585fb468
        # The hashes are stored in reversed byte order in the database for efficiency with parsing
        # Rather than have a costly, unified detection for these - we just special-case for them
        dup1 = bytes(reversed(bytes.fromhex(
            "d5d27987d2a3dfc724e359870c6644b40e497bdc0589a033220fe15429d88599"))).hex()
        dup2 = bytes(reversed(bytes.fromhex(
            "e3bf3d07d4b0375638d5f1db5255fe07ba2c4cb067cd81b84ee974b6585fb468"))).hex()
        self.logger.debug(f"found duplicate tx_hashes in batch - see BIP_0034 for details")
        self.logger.debug(f"len(tx_rows)={len(tx_rows)} before de-duplication")
        new_tx_rows = []
        for row in tx_rows:
            if row[0] in {dup1, dup2}:
                self.logger.debug(f"found 1 duplicate: "
                                  f"{bytes(reversed(bytes.fromhex(row[0]))).hex()}")
                pass
            else:
                new_tx_rows.append(row)
        self.logger.debug(f"len(new_tx_rows)={len(new_tx_rows)} after de-duplication")
        self.logger.debug(f"bulk loading new tx rows")
        self.mysql_bulk_load_confirmed_tx_rows(new_tx_rows)  # retry without problematic coinbase tx

    def mysql_bulk_load_confirmed_tx_rows(self, tx_rows):
        t0 = time.time()
        outfile = Path(str(uuid.uuid4()) + ".csv")
        try:
            string_rows = ["%s,%s,%s,%s,%s\n" % (row) for row in tx_rows]
            column_names = ['tx_hash', 'tx_height', 'tx_position', 'tx_offset_start',
                'tx_offset_end']
            self._load_data_infile("confirmed_transactions", string_rows, column_names,
                binary_column_indices=[0])
        except MySQLdb._exceptions.IntegrityError as e:
            self.logger.error(f"{e}")
            self.handle_coinbase_dup_tx_hash(tx_rows)
        finally:
            if os.path.exists(outfile):
                os.remove(outfile)
        t1 = time.time() - t0
        self.logger.log(PROFILING,
            f"elapsed time for mysql_bulk_load_confirmed_tx_rows = {t1} seconds for {len(tx_rows)}"
        )

    def mysql_bulk_load_mempool_tx_rows(self, tx_rows):
        t0 = time.time()
        outfile = Path(str(uuid.uuid4()) + ".csv")
        try:
            string_rows = ["%s,%s,%s\n" % (row) for row in tx_rows]
            column_names = ['mp_tx_hash', 'mp_tx_timestamp', 'mp_rawtx']
            self._load_data_infile("mempool_transactions", string_rows, column_names,
                binary_column_indices=[0, 2])
        finally:
            if os.path.exists(outfile):
                os.remove(outfile)
        t1 = time.time() - t0

        self.logger.log(PROFILING,
            f"elapsed time for mysql_bulk_load_mempool_tx_rows = {t1} seconds for {len(tx_rows)}"
        )

    def mysql_bulk_load_output_rows(self, out_rows):
        t0 = time.time()
        outfile = Path(str(uuid.uuid4()) + ".csv")
        try:
            string_rows = ["%s,%s,%s,%s,%s\n" % (row) for row in out_rows]
            column_names = ['out_tx_hash', 'out_idx', 'out_value', 'out_offset_start',
                'out_offset_end']
            self._load_data_infile("txo_table", string_rows, column_names,
                binary_column_indices=[0])
        finally:
            if os.path.exists(outfile):
                os.remove(outfile)
        t1 = time.time() - t0

        self.logger.log(PROFILING,
            f"elapsed time for mysql_bulk_load_output_rows = {t1} seconds for {len(out_rows)}"
        )

    def mysql_bulk_load_input_rows(self, in_rows):
        t0 = time.time()
        outfile = Path(str(uuid.uuid4()) + ".csv")
        try:
            string_rows = ["%s,%s,%s,%s,%s,%s\n" % (row) for row in in_rows]
            column_names = ['out_tx_hash', 'out_idx', 'in_tx_hash', 'in_idx',
                'in_offset_start', 'in_offset_end']
            self._load_data_infile("inputs_table", string_rows, column_names,
                binary_column_indices=[0, 2])
        finally:
            if os.path.exists(outfile):
                os.remove(outfile)
        t1 = time.time() - t0
        self.logger.log(PROFILING,
            f"elapsed time for mysql_bulk_load_input_rows = {t1} seconds for {len(in_rows)}"
        )

    def mysql_bulk_load_pushdata_rows(self, pd_rows):
        t0 = time.time()
        outfile = Path(str(uuid.uuid4()) + ".csv")
        try:
            string_rows = ["%s,%s,%s,%s\n" % (row) for row in pd_rows]
            column_names = ['pushdata_hash', 'tx_hash', 'idx', 'ref_type']
            self._load_data_infile("pushdata", string_rows, column_names,
                binary_column_indices=[0, 1])
        finally:
            if os.path.exists(outfile):
                os.remove(outfile)
        t1 = time.time() - t0
        self.logger.log(PROFILING,
            f"elapsed time for mysql_bulk_load_pushdata_rows = {t1} seconds for {len(pd_rows)}"
        )

    def mysql_bulk_load_temp_unsafe_txs(self, unsafe_tx_rows):
        t0 = time.time()
        outfile = Path(str(uuid.uuid4()) + ".csv")
        try:
            string_rows = ["%s\n" % (row) for row in unsafe_tx_rows]
            column_names = ['tx_hash']
            self._load_data_infile("temp_unsafe_txs", string_rows, column_names,
                binary_column_indices=[0])
        finally:
            if os.path.exists(outfile):
                os.remove(outfile)
        t1 = time.time() - t0
        self.logger.log(PROFILING,
            f"elapsed time for mysql_bulk_load_temp_unsafe_txs = {t1} seconds for "
            f"{len(unsafe_tx_rows)}"
        )
