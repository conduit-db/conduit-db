# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.

import logging
import os
import time

from typing import Any, Sequence, Iterator
import typing

from cassandra import WriteTimeout  # pylint:disable=E0611
from cassandra.cluster import Session, ResultSet, NoHostAvailable  # pylint:disable=E0611
from cassandra.concurrent import execute_concurrent, ExecutionResult  # pylint:disable=E0611
from cassandra.pool import Host  # pylint:disable=E0611
from cassandra.query import PreparedStatement, BatchStatement, BatchType  # pylint:disable=E0611

from .exceptions import FailedScyllaOperation
from ..db_interface.types import (
    ConfirmedTransactionRow,
    MempoolTransactionRow,
    InputRow,
    PushdataRow,
)
from ...constants import PROFILING, CONDUIT_INDEX_SERVICE_NAME
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
            self.logger = logging.getLogger(f"scylla-bulk-loads-{self.worker_id}")
        else:
            self.logger = logging.getLogger(f"scylla-bulk-loads")
        self.session: Session = self.db.session
        self.logger.setLevel(get_log_level(CONDUIT_INDEX_SERVICE_NAME))
        self.total_db_time = 0.0
        self.total_rows_flushed_since_startup = 0

    def load_data_batched(
        self,
        insert_statement: PreparedStatement,
        rows: Sequence[tuple[Any, ...]],
        max_retries: int = 5,
        mempool: bool = False,
    ) -> None:
        """This function requires that the `insert_statement` is idempotent so that failed
        UNLOGGED batches (which are not atomic and may be partially applied) can be retries and
        overwrite the rows that were already written as part of the partial batch.

        When ScyllaDB is saturated with compactions etc. the RETRY_DELAY gives it some breathing
        room
        """
        # Favour stability over perfectly optimizing the batch size to the limit
        # larger batch sizes risk timeouts and overloading the coordinator
        MAX_BATCH_COUNT = 200
        BATCH_SIZE = 250  # warn threshold is 128KiB
        INITIAL_RETRY_DELAY = 5  # Initial delay in seconds before retrying

        num_batches = len(rows) // BATCH_SIZE + (1 if len(rows) % BATCH_SIZE != 0 else 0)
        batches = [BatchStatement(batch_type=BatchType.UNLOGGED) for _ in range(num_batches)]
        if not mempool:
            self.logger.debug(f"Submitting {len(batches)} write batches")

        for i, row in enumerate(rows):
            batch_index = i // BATCH_SIZE
            batches[batch_index].add(insert_statement, row)

        # Function to execute batches and return those that failed
        def execute_and_find_failures(batches_to_execute: list[BatchStatement]) -> list[BatchStatement]:
            results: Iterator[ExecutionResult] = execute_concurrent(
                self.db.session,
                [(batch, ()) for batch in batches_to_execute],
                concurrency=100,
                raise_on_first_error=False,
                results_generator=False,
            )
            failed_batches = []
            check_connection = False
            success: bool
            result_or_exception: ResultSet | Exception
            for i, (success, result_or_exception) in enumerate(results):
                if success:
                    continue
                # Retry these
                if isinstance(result_or_exception, WriteTimeout):
                    failed_batches.append(batches_to_execute[i])

                # Catching TypeError will seem weird. This is returned if the exception recursion
                # limit is reached when results_generator is False. This is a hard-coded setting
                # within the python driver.
                # https://github.com/datastax/python-driver/pull/605/files
                elif isinstance(result_or_exception, TypeError) or isinstance(
                    result_or_exception, NoHostAvailable
                ):
                    # The assumption being made here is that the reason they all errored out
                    # was due to an excessive number of WriteTimeout errors.
                    # Or due to heavy load the ScyllaDB node has been marked as "down"
                    # and we need to now reconnect again.
                    failed_batches.append(batches_to_execute[i])
                    check_connection = True
                else:
                    check_connection = True
                    try:
                        raise result_or_exception
                    except Exception:
                        self.logger.exception(
                            f"Unexpected exception for the item number {i} in " f"the batch"
                        )

            if check_connection:
                hosts: list[Host] = list(self.db.session.get_pool_state().keys())
                while len(hosts) == 0:
                    self.logger.error(f"There are no ScyllaDB hosts available. " f"Waiting for reconnection")
                    time.sleep(10)
                    hosts = list(self.db.session.get_pool_state().keys())
                host = hosts[0]
                while host.is_currently_reconnecting():
                    self.logger.debug(f"Host: {host} is reconnecting...")
                    time.sleep(10)
                while not host.is_up:
                    self.logger.debug(f"Host: {host} is still not up yet...")
                    time.sleep(10)

                compulsory_recovery_time = 10
                self.logger.info(
                    f"ScyllaDB connection check complete, host: {host} is up."
                    f"Will resume bulk loading after a {compulsory_recovery_time} "
                    f"second recovery time"
                )
                time.sleep(compulsory_recovery_time)

            return failed_batches

        while len(batches) > 0:
            failed_batches = execute_and_find_failures(batches[0:MAX_BATCH_COUNT])
            # Retry logic for failed batches with exponential backoff
            # Starting from 2 as the first attempt is already made
            for attempt in range(2, max_retries + 1):
                if not failed_batches:
                    break

                retry_delay = INITIAL_RETRY_DELAY * (2 ** (attempt - 2))
                time.sleep(retry_delay)
                self.logger.warning(
                    f"Retrying {len(failed_batches)} failed batches, attempt {attempt} "
                    f"of {max_retries} after {retry_delay} seconds"
                )
                failed_batches = execute_and_find_failures(failed_batches)

            # Logging the result
            if failed_batches:
                self.logger.error(f"Failed to write rows after {max_retries} attempts")
                # crash out
                raise FailedScyllaOperation(f"Failed to write rows after {max_retries} attempts")
            else:
                self.total_rows_flushed_since_startup += len(rows)
                self.logger.log(
                    PROFILING,
                    f"total rows flushed since startup (worker_id={self.worker_id if self.worker_id else 'None'})="
                    f"{self.total_rows_flushed_since_startup}",
                )
            batches = batches[MAX_BATCH_COUNT:]

    def handle_coinbase_dup_tx_hash(
        self, tx_rows: list[ConfirmedTransactionRow]
    ) -> list[ConfirmedTransactionRow]:
        # Todo may need to search the other input/output/pushdata rows too for these problem txids
        # rare issue see: https://en.bitcoin.it/wiki/BIP_0034
        # There are only two cases of duplicate tx_hashes:
        # d5d27987d2a3dfc724e359870c6644b40e497bdc0589a033220fe15429d88599
        # e3bf3d07d4b0375638d5f1db5255fe07ba2c4cb067cd81b84ee974b6585fb468
        # Iterate over the list in reverse order because we are mutating it
        dup1 = bytes(
            reversed(bytes.fromhex("d5d27987d2a3dfc724e359870c6644b40e497bdc0589a033220fe15429d88599"))
        ).hex()
        dup2 = bytes(
            reversed(bytes.fromhex("e3bf3d07d4b0375638d5f1db5255fe07ba2c4cb067cd81b84ee974b6585fb468"))
        ).hex()
        for i in range(len(tx_rows) - 1, -1, -1):
            if tx_rows[i].tx_hash in dup1 or tx_rows[i].tx_hash in dup2:
                self.logger.debug(f"removing a special case txid from insert batch: {tx_rows[i].tx_hash}")
                del tx_rows[i]
        return tx_rows

    def bulk_load_confirmed_tx_rows(
        self, tx_rows: list[ConfirmedTransactionRow], check_duplicates: bool = False, mempool: bool = False
    ) -> None:
        if tx_rows:
            assert isinstance(tx_rows[0].tx_hash, str)
        t0 = time.time()
        if check_duplicates:  # should only be True for blocks: 91812, 91842, 91722, 91880
            tx_rows = self.handle_coinbase_dup_tx_hash(tx_rows)
        insert_statement = self.session.prepare(
            "INSERT INTO confirmed_transactions (tx_hash, tx_block_num, tx_position) " "VALUES (?, ?, ?)"
        )
        insert_rows = [(bytes.fromhex(row.tx_hash), row.tx_block_num, row.tx_position) for row in tx_rows]
        self.logger.debug(f"Bulk loading {len(insert_rows)} confirmed tx rows")
        self.load_data_batched(insert_statement, insert_rows, mempool)
        t1 = time.time() - t0
        self.logger.log(
            PROFILING,
            f"elapsed time for bulk_load_confirmed_tx_rows = {t1} seconds for {len(tx_rows)}",
        )

    def bulk_load_mempool_tx_rows(self, tx_rows: list[MempoolTransactionRow]) -> None:
        """This uses redis but it is included here to make it easier to translate from
        MySQL to ScyllaDB"""

        def load_batch(batch: list[bytes]) -> None:
            self.db.cache.r.sadd("mempool", *batch)

        t0 = time.time()
        batch_size = 350  # warn threshold is 128KiB

        futures = []
        assert self.db.executor is not None
        for i in range(0, len(tx_rows), batch_size):
            batch = [bytes.fromhex(row.mp_tx_hashX) for row in tx_rows[i : i + batch_size]]
            future = self.db.executor.submit(load_batch, batch)
            futures.append(future)

        for future in futures:
            future.result()

        t1 = time.time() - t0
        self.logger.log(
            PROFILING, f"elapsed time for bulk_load_mempool_tx_rows = {t1} seconds for {len(tx_rows)}"
        )

    def bulk_load_input_rows(self, in_rows: list[InputRow], mempool: bool = False) -> None:
        t0 = time.time()
        insert_statement = self.session.prepare(
            "INSERT INTO inputs_table (out_tx_hash, out_idx, in_tx_hash, in_idx) VALUES (?, ?, ?, ?)"
        )
        insert_rows = [
            (bytes.fromhex(row.out_tx_hash), row.out_idx, bytes.fromhex(row.in_tx_hash), row.in_idx)
            for row in in_rows
        ]
        if not mempool:
            self.logger.debug(f"Bulk loading {len(in_rows)} input rows")
        self.load_data_batched(insert_statement, insert_rows, mempool)
        t1 = time.time() - t0
        self.logger.log(PROFILING, f"elapsed time for bulk_load_input_rows = {t1} seconds for {len(in_rows)}")

    def bulk_load_pushdata_rows(self, pd_rows: list[PushdataRow], mempool: bool = False) -> None:
        t0 = time.time()
        insert_statement = self.session.prepare(
            "INSERT INTO pushdata (pushdata_hash, tx_hash, idx, ref_type) VALUES (?, ?, ?, ?)"
        )
        insert_rows = [
            (bytes.fromhex(row.pushdata_hash), bytes.fromhex(row.tx_hash), row.idx, row.ref_type)
            for row in pd_rows
        ]
        if not mempool:
            self.logger.debug(f"Bulk loading {len(insert_rows)} pushdata rows")
        self.load_data_batched(insert_statement, insert_rows, mempool)
        t1 = time.time() - t0
        self.logger.log(
            PROFILING, f"elapsed time for bulk_load_pushdata_rows = {t1} seconds for " f"{len(pd_rows)} rows"
        )

    def bulk_load_headers(self, block_header_rows: list[BlockHeaderRow]) -> None:
        t0 = time.time()
        insert_statement = self.session.prepare(
            "INSERT INTO headers (block_num, block_hash, block_height, block_header, "
            "block_tx_count, block_size, is_orphaned) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)"
        )
        insert_rows = []
        for row in block_header_rows:
            assert row.block_header is not None
            insert_rows.append(
                (
                    row.block_num,
                    bytes.fromhex(row.block_hash),
                    row.block_height,
                    bytes.fromhex(row.block_header),
                    row.block_tx_count,
                    row.block_size,
                    bool(row.is_orphaned),
                )
            )
        self.load_data_batched(insert_statement, insert_rows)
        t1 = time.time() - t0
        self.logger.log(
            PROFILING,
            f"elapsed time for bulk_load_headers = {t1} seconds for {len(block_header_rows)}",
        )
