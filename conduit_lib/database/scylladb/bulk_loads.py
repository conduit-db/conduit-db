import logging
import math
import os
import sys
import time

import typing
from pathlib import Path

from cassandra import ConsistencyLevel, WriteTimeout, WriteFailure
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.query import BatchStatement, BatchType

from .exceptions import FailedScyllaOperation
from ..db_interface.types import (
    ConfirmedTransactionRow,
    MempoolTransactionRow,
    OutputRow,
    InputRow,
    PushdataRow,
)
from ...constants import PROFILING, BULK_LOADING_BATCH_SIZE_ROW_COUNT
from ...types import BlockHeaderRow
from ...utils import get_log_level

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))

if typing.TYPE_CHECKING:
    from .scylladb import ScyllaDB


class ScyllaDBBulkLoads:
    def __init__(self, db: "ScyllaDB") -> None:
        self.db = db
        self.worker_id = self.db.worker_id
        if self.worker_id:
            self.logger = logging.getLogger(f"scylla-tables-{self.worker_id}")
        else:
            self.logger = logging.getLogger(f"scylla-tables")
        self.session = self.db.session
        self.logger.setLevel(get_log_level("conduit_index"))
        self.total_db_time = 0.0
        self.total_rows_flushed_since_startup = 0  # for current controller

        self.newline_symbol = r"'\r\n'" if sys.platform == "win32" else r"'\n'"
        self.TEMP_FILES_DIR = Path(os.environ["DATADIR_SSD"]) / "temp_files"

    def _prepare_batch_statement(self):
        return BatchStatement(consistency_level=ConsistencyLevel.QUORUM, batch_type=BatchType.UNLOGGED)

    def _execute_batch(self, batch):
        t0 = time.time()
        try:
            self.session.execute(batch)
        except Exception as e:
            self.logger.exception("unexpected exception in _execute_batch")
            raise FailedScyllaOperation(f"Failed to execute batch operation: {e}")
        finally:
            t1 = time.time() - t0
            self.total_db_time += t1
            self.logger.log(PROFILING, f"total db flush time={self.total_db_time}")

    def load_data_batched(self, insert_statement, rows):
        BATCH_SIZE = BULK_LOADING_BATCH_SIZE_ROW_COUNT
        BATCHES_COUNT = math.ceil(len(rows) / BATCH_SIZE)

        for i in range(BATCHES_COUNT):
            if i == BATCHES_COUNT - 1:
                rows_batch = rows[i * BATCH_SIZE :]
            else:
                rows_batch = rows[i * BATCH_SIZE : (i + 1) * BATCH_SIZE]

            # Execute the statements in parallel using execute_concurrent_with_args
            execute_concurrent_with_args(
                self.session,  # Assuming self.session is a Cassandra cluster session
                insert_statement,
                rows_batch,
                concurrency=50,
                raise_on_first_error=True,
            )
        self.total_rows_flushed_since_startup += len(rows)
        self.logger.log(
            PROFILING,
            f"total rows flushed since startup (worker_id={self.worker_id if self.worker_id else 'None'})="
            f"{self.total_rows_flushed_since_startup}",
        )

    def load_data_concurrently(self, insert_statement, rows):
        try:
            # Execute the statements in parallel using execute_concurrent_with_args
            execute_concurrent_with_args(
                self.session,  # Assuming self.session is a Cassandra cluster session
                insert_statement,
                rows,
                concurrency=50,  # Setting the desired concurrency level
                raise_on_first_error=True,  # Will raise an exception on first error
            )
            # If all operations succeed, update the total count
            self.total_rows_flushed_since_startup += len(rows)

            # Log the success
            self.logger.log(
                PROFILING,
                f"Total rows successfully flushed since startup (worker_id={self.worker_id if self.worker_id else 'None'}): "
                f"{self.total_rows_flushed_since_startup}",
            )

        except (WriteTimeout, WriteFailure) as e:
            # Handle the exception as needed (log, retry, etc.)
            self.logger.error(f"Error occurred during concurrent write operations: {str(e)}")
            # If needed, implement a retry mechanism here

    def handle_coinbase_dup_tx_hash(self, tx_rows: list[ConfirmedTransactionRow]) -> None:
        # rare issue see: https://en.bitcoin.it/wiki/BIP_0034
        # There are only two cases of duplicate tx_hashes:
        # d5d27987d2a3dfc724e359870c6644b40e497bdc0589a033220fe15429d88599
        # e3bf3d07d4b0375638d5f1db5255fe07ba2c4cb067cd81b84ee974b6585fb468
        raise NotImplementedError()  # Can just always drop these from tx_rows if detected

    def bulk_load_confirmed_tx_rows(
        self, tx_rows: list[ConfirmedTransactionRow], check_duplicates: bool = False
    ) -> None:
        t0 = time.time()
        if check_duplicates:  # should only be True for blocks: 91812, 91842, 91722, 91880
            self.handle_coinbase_dup_tx_hash(tx_rows)
        insert_statement = self.session.prepare(
            "INSERT INTO confirmed_transactions (tx_hash, tx_block_num, tx_position) " "VALUES (?, ?, ?)"
        )
        self.load_data_batched(insert_statement, tx_rows)
        t1 = time.time() - t0
        self.logger.log(
            PROFILING,
            f"elapsed time for bulk_load_confirmed_tx_rows = {t1} seconds for {len(tx_rows)}",
        )

    def bulk_load_mempool_tx_rows(self, tx_rows: list[MempoolTransactionRow]) -> None:
        t0 = time.time()
        insert_statement = self.session.prepare(
            "INSERT INTO mempool_transactions (mp_tx_hash, mp_tx_timestamp) VALUES (?, ?)"
        )
        self.load_data_batched(insert_statement, tx_rows)
        t1 = time.time() - t0
        self.logger.log(
            PROFILING, f"elapsed time for bulk_load_mempool_tx_rows = {t1} seconds for {len(tx_rows)}"
        )

    def bulk_load_output_rows(self, out_rows: list[OutputRow]) -> None:
        t0 = time.time()
        insert_statement = self.session.prepare(
            "INSERT INTO txo_table (out_tx_hash, out_idx, out_value) VALUES (?, ?, ?)"
        )
        self.load_data_batched(insert_statement, out_rows)
        t1 = time.time() - t0
        self.logger.log(
            PROFILING, f"elapsed time for bulk_load_output_rows = {t1} seconds for {len(out_rows)}"
        )

    def bulk_load_input_rows(self, in_rows: list[InputRow]) -> None:
        t0 = time.time()
        insert_statement = self.session.prepare(
            "INSERT INTO inputs_table (out_tx_hash, out_idx, in_tx_hash, in_idx) VALUES (?, ?, ?, ?)"
        )
        self.load_data_batched(insert_statement, in_rows)
        t1 = time.time() - t0
        self.logger.log(PROFILING, f"elapsed time for bulk_load_input_rows = {t1} seconds for {len(in_rows)}")

    def bulk_load_pushdata_rows(self, pd_rows: list[PushdataRow]) -> None:
        t0 = time.time()
        insert_statement = self.session.prepare(
            "INSERT INTO pushdata (pushdata_hash, tx_hash, idx, ref_type) VALUES (?, ?, ?, ?)"
        )
        self.load_data_batched(insert_statement, pd_rows)
        t1 = time.time() - t0
        self.logger.log(
            PROFILING, f"elapsed time for bulk_load_pushdata_rows = {t1} seconds for {len(pd_rows)}"
        )

    def bulk_load_temp_unsafe_txs(self, unsafe_tx_rows: list[str]) -> None:
        t0 = time.time()
        insert_statement = self.session.prepare("INSERT INTO temp_unsafe_txs (tx_hash) VALUES (?)")
        self.load_data_batched(insert_statement, unsafe_tx_rows)
        t1 = time.time() - t0
        self.logger.log(
            PROFILING,
            f"elapsed time for bulk_load_temp_unsafe_txs = {t1} seconds for {len(unsafe_tx_rows)}",
        )

    def bulk_load_headers(self, block_header_rows: list[BlockHeaderRow]) -> None:
        t0 = time.time()
        insert_statement = self.session.prepare(
            "INSERT INTO headers (block_num, block_hash, block_height, block_header, "
            "block_tx_count, block_size, is_orphaned) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)"
        )
        self.load_data_batched(insert_statement, block_header_rows)
        t1 = time.time() - t0
        self.logger.log(
            PROFILING,
            f"elapsed time for bulk_load_headers = {t1} seconds for {len(block_header_rows)}",
        )
