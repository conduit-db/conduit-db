import logging
import math
import os
import sys
import time

import typing
from pathlib import Path

from cassandra.cluster import Session

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
    from .db import ScyllaDB


class ScyllaDBBulkLoads:
    def __init__(self, db: "ScyllaDB") -> None:
        self.db = db
        self.worker_id = self.db.worker_id
        if self.worker_id:
            self.logger = logging.getLogger(f"scylla-tables-{self.worker_id}")
        else:
            self.logger = logging.getLogger(f"scylla-tables")
        self.session: Session = self.db.session
        self.logger.setLevel(get_log_level("conduit_index"))
        self.total_db_time = 0.0
        self.total_rows_flushed_since_startup = 0  # for current controller

        self.newline_symbol = r"'\r\n'" if sys.platform == "win32" else r"'\n'"
        self.TEMP_FILES_DIR = Path(os.environ["DATADIR_SSD"]) / "temp_files"

    def load_data_batched(self, insert_statement, rows):
        BATCH_SIZE = BULK_LOADING_BATCH_SIZE_ROW_COUNT
        BATCHES_COUNT = math.ceil(len(rows) / BATCH_SIZE)

        for i in range(BATCHES_COUNT):
            if i == BATCHES_COUNT - 1:
                rows_batch = rows[i * BATCH_SIZE :]
            else:
                rows_batch = rows[i * BATCH_SIZE : (i + 1) * BATCH_SIZE]
            self.db.execute_with_concurrency(insert_statement, rows_batch)
        self.total_rows_flushed_since_startup += len(rows)
        self.logger.log(
            PROFILING,
            f"total rows flushed since startup (worker_id={self.worker_id if self.worker_id else 'None'})="
            f"{self.total_rows_flushed_since_startup}",
        )

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
        insert_rows = [
            (bytes.fromhex(row.tx_hash), row.tx_block_num, row.tx_position) for row in tx_rows]
        self.load_data_batched(insert_statement, insert_rows)
        t1 = time.time() - t0
        self.logger.log(
            PROFILING,
            f"elapsed time for bulk_load_confirmed_tx_rows = {t1} seconds for {len(tx_rows)}",
        )

    def bulk_load_mempool_tx_rows(self, tx_rows: list[MempoolTransactionRow]) -> None:
        """This uses redis but it is included here to make it easier to translate from
        MySQL to ScyllaDB"""
        t0 = time.time()
        pairs = [(bytes.fromhex(row.mp_tx_hash), row.mp_tx_timestamp) for row in tx_rows]
        self.db.cache.bulk_load_in_namespace(namespace=b'mempool', pairs=pairs)
        t1 = time.time() - t0
        self.logger.log(
            PROFILING, f"elapsed time for bulk_load_mempool_tx_rows = {t1} seconds for {len(tx_rows)}"
        )

    def bulk_load_output_rows(self, out_rows: list[OutputRow]) -> None:
        t0 = time.time()
        insert_statement = self.session.prepare(
            "INSERT INTO txo_table (out_tx_hash, out_idx, out_value) VALUES (?, ?, ?)"
        )
        insert_rows = [
            (bytes.fromhex(row.out_tx_hash), row.out_idx, row.out_value) for row in out_rows]
        self.load_data_batched(insert_statement, insert_rows)
        t1 = time.time() - t0
        self.logger.log(
            PROFILING, f"elapsed time for bulk_load_output_rows = {t1} seconds for {len(out_rows)}"
        )

    def bulk_load_input_rows(self, in_rows: list[InputRow]) -> None:
        t0 = time.time()
        insert_statement = self.session.prepare(
            "INSERT INTO inputs_table (out_tx_hash, out_idx, in_tx_hash, in_idx) VALUES (?, ?, ?, ?)"
        )
        insert_rows = [(bytes.fromhex(row.out_tx_hash), row.out_idx, bytes.fromhex(row.in_tx_hash),
                row.in_idx) for row in in_rows]
        self.load_data_batched(insert_statement, insert_rows)
        t1 = time.time() - t0
        self.logger.log(PROFILING, f"elapsed time for bulk_load_input_rows = {t1} seconds for {len(in_rows)}")

    def bulk_load_pushdata_rows(self, pd_rows: list[PushdataRow]) -> None:
        t0 = time.time()
        insert_statement = self.session.prepare(
            "INSERT INTO pushdata (pushdata_hash, tx_hash, idx, ref_type) VALUES (?, ?, ?, ?)"
        )
        insert_rows = [(bytes.fromhex(row.pushdata_hash), bytes.fromhex(row.tx_hash),
            row.idx, row.idx) for row in pd_rows]
        self.load_data_batched(insert_statement, insert_rows)
        t1 = time.time() - t0
        self.logger.log(
            PROFILING, f"elapsed time for bulk_load_pushdata_rows = {t1} seconds for {len(pd_rows)}"
        )

    def bulk_load_headers(self, block_header_rows: list[BlockHeaderRow]) -> None:
        t0 = time.time()
        insert_statement = self.session.prepare(
            "INSERT INTO headers (block_num, block_hash, block_height, block_header, "
            "block_tx_count, block_size, is_orphaned) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)"
        )
        insert_rows = [(row.block_num, bytes.fromhex(row.block_hash), row.block_height,
            bytes.fromhex(row.block_header), row.block_tx_count, row.block_size, row.is_orphaned)
            for row in block_header_rows]
        self.load_data_batched(insert_statement, insert_rows)
        t1 = time.time() - t0
        self.logger.log(
            PROFILING,
            f"elapsed time for bulk_load_headers = {t1} seconds for {len(block_header_rows)}",
        )
