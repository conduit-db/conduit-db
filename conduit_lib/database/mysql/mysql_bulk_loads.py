import logging
import math
import os
import sys
import time
import uuid
from pathlib import Path
import typing

import MySQLdb

from .types import PushdataRow, InputRow, OutputRow, ConfirmedTransactionRow, MempoolTransactionRow
from ...constants import PROFILING, BULK_LOADING_BATCH_SIZE_ROW_COUNT
from ...types import BlockHeaderRow
from ...utils import get_log_level

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))

if typing.TYPE_CHECKING:
    from ... import MySQLDatabase


class FailedMySQLOperation(Exception):
    pass


class MySQLBulkLoads:

    def __init__(self, mysql_conn: MySQLdb.Connection, mysql_db: 'MySQLDatabase') -> None:
        self.mysql_db = mysql_db
        self.worker_id = self.mysql_db.worker_id
        if self.worker_id:
            self.logger = logging.getLogger(f"mysql-tables-{self.worker_id}")
        else:
            self.logger = logging.getLogger(f"mysql-tables")
        self.mysql_conn = mysql_conn

        self.logger.setLevel(get_log_level('conduit_index'))
        self.total_db_time = 0.
        self.total_rows_flushed_since_startup = 0  # for current controller
        self.newline_symbol = r"'\r\n'" if sys.platform == 'win32' else r"'\n'"
        self.TEMP_FILES_DIR = Path(os.environ["DATADIR_SSD"]) / 'temp_files'

    def set_local_infile_on(self) -> None:
        extra_settings = f"SET @@GLOBAL.local_infile = 1;"
        self.mysql_conn.query(extra_settings)

    def _load_data_infile_batched(self, table_name: str, string_rows: list[str],
            column_names: list[str], binary_column_indices: list[int]) \
                -> None:
        t0 = time.time()
        try:
            string_rows.sort()
            BATCH_SIZE = BULK_LOADING_BATCH_SIZE_ROW_COUNT
            BATCHES_COUNT = math.ceil(len(string_rows)/BATCH_SIZE)
            for i in range(BATCHES_COUNT):
                if i == BATCHES_COUNT - 1:
                    string_rows_batch = string_rows[i*BATCH_SIZE:]
                else:
                    string_rows_batch = string_rows[i*BATCH_SIZE:(i+1)*BATCH_SIZE]

                self._load_data_infile(table_name, string_rows_batch, column_names,
                    binary_column_indices)
        finally:
            t1 = time.time() - t0
            self.total_db_time += t1
            self.total_rows_flushed_since_startup += len(string_rows)
            self.logger.log(PROFILING, f"total db flush time={self.total_db_time}")
            self.logger.log(PROFILING, f"total rows flushed since startup "
                                       f"(worker_id={self.worker_id if self.worker_id else None})"
                f"={self.total_rows_flushed_since_startup}")

    def _load_data_infile(self, table_name: str, string_rows: list[str],
            column_names: list[str], binary_column_indices: list[int], have_retried: bool=False) \
                -> None:
        """Todo: If the total query exceeds 1GB I wonder if I may need to cut it up into
            smaller sizes. Have been running into the occasional MySQL error with:
            MySQLdb._exceptions.OperationalError: (2006, '') and logs for mariadb showing:
            Got an error reading communication packets.

            I think the solution is to raise these MySQL settings:
            max_allowed_packet      = 1024M
            net_buffer_length       = 1048576

            But I am not 100% sure and so batching into smaller chunks may be required.
        """
        outfile = self.TEMP_FILES_DIR / (str(uuid.uuid4()) + ".csv")
        os.makedirs(os.path.dirname(outfile), exist_ok=True)
        try:
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
            try:
                self.mysql_conn.query(query)
                self.mysql_conn.commit()
            finally:
                self.mysql_db.commit_transaction()
        except MySQLdb.OperationalError as e:
            if not have_retried:
                self.logger.error(f"MySQLdb.OperationalError: {e}; "
                    f"Retrying bulk insert for column_names: {column_names}")
                time.sleep(10)  # give MariaDB a rest
                self._load_data_infile(table_name, string_rows, column_names,
                    binary_column_indices, True)
            if have_retried:
                # If this happens may need to look at limiting the maximum batch size for
                # bulk inserts to see if that helps. Auto-reconnect does not seem to be viable.
                raise FailedMySQLOperation("Failed second re-attempt at bulk load to MySQL")
        except Exception:
            self.logger.exception("unexpected exception in _load_data_infile")
            self.mysql_db.rollback_transaction()
            raise
        finally:
            if os.path.exists(outfile):
                os.remove(outfile)

    def handle_coinbase_dup_tx_hash(self, tx_rows: list[ConfirmedTransactionRow]) -> None:
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

    def mysql_bulk_load_confirmed_tx_rows(self, tx_rows: list[ConfirmedTransactionRow]) -> None:
        t0 = time.time()
        try:
            string_rows = ["%s,%s,%s\n" % (row) for row in tx_rows]
            column_names = ['tx_hash', 'tx_block_num', 'tx_position']
            self._load_data_infile_batched("confirmed_transactions", string_rows, column_names,
                binary_column_indices=[0])
        except MySQLdb.IntegrityError as e:
            self.logger.error(f"{e}")
            self.handle_coinbase_dup_tx_hash(tx_rows)
        t1 = time.time() - t0
        self.logger.log(PROFILING,
            f"elapsed time for mysql_bulk_load_confirmed_tx_rows = {t1} seconds for {len(tx_rows)}"
        )

    def mysql_bulk_load_mempool_tx_rows(self, tx_rows: list[MempoolTransactionRow]) -> None:
        t0 = time.time()
        string_rows = ["%s,%s\n" % (row[0:2]) for row in tx_rows]
        column_names = ['mp_tx_hash', 'mp_tx_timestamp']
        self._load_data_infile_batched("mempool_transactions", string_rows, column_names,
            binary_column_indices=[0])
        t1 = time.time() - t0
        self.logger.log(PROFILING,
            f"elapsed time for mysql_bulk_load_mempool_tx_rows = {t1} seconds for {len(tx_rows)}"
        )

    def mysql_bulk_load_output_rows(self, out_rows: list[OutputRow]) -> None:
        t0 = time.time()
        string_rows = ["%s,%s,%s\n" % (row) for row in out_rows]
        column_names = ['out_tx_hash', 'out_idx', 'out_value']
        self._load_data_infile_batched("txo_table", string_rows, column_names,
            binary_column_indices=[0])
        t1 = time.time() - t0
        self.logger.log(PROFILING,
            f"elapsed time for mysql_bulk_load_output_rows = {t1} seconds for {len(out_rows)}"
        )

    def mysql_bulk_load_input_rows(self, in_rows: list[InputRow]) -> None:
        t0 = time.time()
        string_rows = ["%s,%s,%s,%s\n" % (row) for row in in_rows]
        column_names = ['out_tx_hash', 'out_idx', 'in_tx_hash', 'in_idx']
        self._load_data_infile_batched("inputs_table", string_rows, column_names,
            binary_column_indices=[0, 2])
        t1 = time.time() - t0
        self.logger.log(PROFILING,
            f"elapsed time for mysql_bulk_load_input_rows = {t1} seconds for {len(in_rows)}"
        )

    def mysql_bulk_load_pushdata_rows(self, pd_rows: list[PushdataRow]) -> None:
        t0 = time.time()
        string_rows = ["%s,%s,%s,%s\n" % (row) for row in pd_rows]
        column_names = ['pushdata_hash', 'tx_hash', 'idx', 'ref_type']
        self._load_data_infile_batched("pushdata", string_rows, column_names,
            binary_column_indices=[0, 1])
        t1 = time.time() - t0
        self.logger.log(PROFILING,
            f"elapsed time for mysql_bulk_load_pushdata_rows = {t1} seconds for {len(pd_rows)}"
        )

    def mysql_bulk_load_temp_unsafe_txs(self, unsafe_tx_rows: list[str]) -> None:
        t0 = time.time()
        string_rows = ["%s\n" % (row) for row in unsafe_tx_rows]
        column_names = ['tx_hash']
        self._load_data_infile_batched("temp_unsafe_txs", string_rows, column_names,
            binary_column_indices=[0])
        t1 = time.time() - t0
        self.logger.log(PROFILING,
            f"elapsed time for mysql_bulk_load_temp_unsafe_txs = {t1} seconds for "
            f"{len(unsafe_tx_rows)}"
        )

    def mysql_bulk_load_headers(self, block_header_rows: list[BlockHeaderRow]) -> None:
        """block_num, block_hash, block_height, block_header"""
        string_rows = ["%s,%s,%s,%s,%s,%s,%s\n" % (row) for row in block_header_rows]
        column_names = ['block_num', 'block_hash', 'block_height', 'block_header', 'block_tx_count',
            'block_size', 'is_orphaned']
        self._load_data_infile_batched(f'headers', string_rows, column_names,
            binary_column_indices=[1, 3])
