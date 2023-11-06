import logging
import math
import os
import time

import typing
from cassandra.query import BatchStatement, BatchType
from cassandra import ConsistencyLevel

from .exceptions import FailedScyllaOperation
from ..mysql.types import ConfirmedTransactionRow, PushdataRow, InputRow, OutputRow, MempoolTransactionRow
from ...constants import PROFILING, BULK_LOADING_BATCH_SIZE_ROW_COUNT
from ...types import BlockHeaderRow
from ...utils import get_log_level

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))

if typing.TYPE_CHECKING:
    from .scylladb import ScyllaDatabase


class ScyllaDBBulkLoads:
    def __init__(self, scylladb: "ScyllaDatabase") -> None:
        self.scylladb = scylladb
        self.worker_id = self.scylladb.worker_id
        if self.worker_id:
            self.logger = logging.getLogger(f"scylla-tables-{self.worker_id}")
        else:
            self.logger = logging.getLogger(f"scylla-tables")
        self.scylla_session = self.scylladb.session
        self.logger.setLevel(get_log_level("conduit_index"))
        self.scylladb.total_db_time = 0.0
        self.scylladb.total_rows_flushed_since_startup = 0  # for current controller

    def _execute_batch(self, batch):
        t0 = time.time()
        try:
            self.scylla_session.execute(batch)
        except Exception as e:
            self.logger.exception("unexpected exception in _execute_batch")
            raise FailedScyllaOperation(f"Failed to execute batch operation: {e}")
        finally:
            t1 = time.time() - t0
            self.scylladb.total_db_time += t1
            self.logger.log(PROFILING, f"total db flush time={self.scylladb.total_db_time}")

    def scylla_bulk_load_confirmed_tx_rows(self, tx_rows: list[ConfirmedTransactionRow]) -> None:
        t0 = time.time()
        insert_statement = self.scylla_session.prepare(
            "INSERT INTO confirmed_transactions (tx_hash, tx_block_num, tx_position) " "VALUES (?, ?, ?)"
        )
        self.scylladb.load_data_batched(insert_statement, tx_rows)
        t1 = time.time() - t0
        self.logger.log(
            PROFILING,
            f"elapsed time for scylla_bulk_load_confirmed_tx_rows = {t1} seconds for {len(tx_rows)}",
        )

    def scylla_bulk_load_mempool_tx_rows(self, tx_rows: list[MempoolTransactionRow]) -> None:
        t0 = time.time()
        insert_statement = self.scylla_session.prepare(
            "INSERT INTO mempool_transactions (mp_tx_hash, mp_tx_timestamp) VALUES (?, ?)"
        )
        self.scylladb.load_data_batched(insert_statement, tx_rows)
        t1 = time.time() - t0
        self.logger.log(
            PROFILING, f"elapsed time for scylla_bulk_load_mempool_tx_rows = {t1} seconds for {len(tx_rows)}"
        )

    def scylla_bulk_load_output_rows(self, out_rows: list[OutputRow]) -> None:
        t0 = time.time()
        insert_statement = self.scylla_session.prepare(
            "INSERT INTO txo_table (out_tx_hash, out_idx, out_value) VALUES (?, ?, ?)"
        )
        self.scylladb.load_data_batched(insert_statement, out_rows)
        t1 = time.time() - t0
        self.logger.log(
            PROFILING, f"elapsed time for scylla_bulk_load_output_rows = {t1} seconds for {len(out_rows)}"
        )

    def scylla_bulk_load_input_rows(self, in_rows: list[InputRow]) -> None:
        t0 = time.time()
        insert_statement = self.scylla_session.prepare(
            "INSERT INTO inputs_table (out_tx_hash, out_idx, in_tx_hash, in_idx) VALUES (?, ?, ?, ?)"
        )
        self.scylladb.load_data_batched(insert_statement, in_rows)
        t1 = time.time() - t0
        self.logger.log(
            PROFILING, f"elapsed time for scylla_bulk_load_input_rows = {t1} seconds for {len(in_rows)}"
        )

    def scylla_bulk_load_pushdata_rows(self, pd_rows: list[PushdataRow]) -> None:
        t0 = time.time()
        insert_statement = self.scylla_session.prepare(
            "INSERT INTO pushdata (pushdata_hash, tx_hash, idx, ref_type) VALUES (?, ?, ?, ?)"
        )
        self.scylladb.load_data_batched(insert_statement, pd_rows)
        t1 = time.time() - t0
        self.logger.log(
            PROFILING, f"elapsed time for scylla_bulk_load_pushdata_rows = {t1} seconds for {len(pd_rows)}"
        )

    def scylla_bulk_load_temp_unsafe_txs(self, unsafe_tx_rows: list[str]) -> None:
        t0 = time.time()
        insert_statement = self.scylla_session.prepare("INSERT INTO temp_unsafe_txs (tx_hash) VALUES (?)")
        self.scylladb.load_data_batched(insert_statement, unsafe_tx_rows)
        t1 = time.time() - t0
        self.logger.log(
            PROFILING,
            f"elapsed time for scylla_bulk_load_temp_unsafe_txs = {t1} seconds for {len(unsafe_tx_rows)}",
        )

    def scylla_bulk_load_headers(self, block_header_rows: list[BlockHeaderRow]) -> None:
        t0 = time.time()
        insert_statement = self.scylla_session.prepare(
            "INSERT INTO headers (block_num, block_hash, block_height, block_header, "
            "block_tx_count, block_size, is_orphaned) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)"
        )
        self.scylladb.load_data_batched(insert_statement, block_header_rows)
        t1 = time.time() - t0
        self.logger.log(
            PROFILING,
            f"elapsed time for scylla_bulk_load_headers = {t1} seconds for {len(block_header_rows)}",
        )
