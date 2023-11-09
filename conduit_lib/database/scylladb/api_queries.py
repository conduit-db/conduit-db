import concurrent.futures
import logging
import os

import typing
from concurrent.futures import Future
from dataclasses import dataclass
from functools import partial
from typing import NamedTuple

from cassandra.cluster import Session, ResultSet
from cassandra.concurrent import execute_concurrent_with_args, ExecutionResult

from ..db_interface.tip_filter_types import OutputSpendRow
from ..lmdb.lmdb_database import get_full_tx_hash, LMDB_Database
from ...constants import MAX_UINT32, HashXLength
from ...types import RestorationFilterQueryResult, TxLocation, TxMetadata, BlockHeaderRow, \
    OutpointType, PushdataMatchFlags

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))

if typing.TYPE_CHECKING:
    from .db import ScyllaDB


class PushdataResult(NamedTuple):
    pushdata_hash: bytes
    tx_hash: bytes
    idx: int
    ref_type: int


class InputResult(NamedTuple):
    out_tx_hash: bytes
    out_idx: int
    in_tx_hash: bytes
    in_idx: int


class ConfirmedTxResult(NamedTuple):
    tx_hash: bytes
    tx_block_num: int
    tx_position: int


class HeaderResult(NamedTuple):
    block_hash: bytes
    block_num: int
    is_orphaned: int


class TxJoinResult(NamedTuple):
    tx_block_num: int
    tx_position: int


class HeaderJoinResult(NamedTuple):
    block_hash: bytes


@dataclass
class JoinedRows:
    pushdata_result: PushdataResult
    input_result: InputResult | None = None
    tx_result: list[ConfirmedTxResult] | None = None  # Reorgs mean we need to have a list...
    header_result: HeaderResult | None = None


class GetPushdataFilterMatchesStatements:
    def __init__(self, session: Session):
        self.pushdata_stmt = session.prepare(
            "SELECT pushdata_hash, tx_hash, idx, ref_type FROM pushdata WHERE pushdata_hash = ? LIMIT 1000"
        )
        self.inputs_stmt = session.prepare(
            "SELECT out_tx_hash, out_idx, in_tx_hash, in_idx FROM inputs_table WHERE out_tx_hash = ? AND out_idx = ?"
        )
        self.confirmed_transactions_stmt = session.prepare(
            "SELECT tx_hash, tx_block_num, tx_position FROM confirmed_transactions WHERE tx_hash = ?"
        )
        self.headers_stmt = session.prepare(
            "SELECT block_hash, block_num, is_orphaned FROM headers "
            "WHERE block_num = ?"  #  AND is_orphaned = 0  commented out to filter in callback
        )


class GetSpentOutpointsStatements:
    def __init__(self, session: Session):
        self.inputs_stmt = session.prepare(
            "SELECT out_tx_hash, out_idx, in_tx_hash, in_idx FROM inputs_table "
            "WHERE out_tx_hash = ? AND out_idx = ?"
        )
        self.txs_stmt = self.session.prepare(
            "SELECT tx_block_num, tx_position FROM confirmed_transactions " "WHERE tx_hash = ?"
        )
        self.input_headers_stmt = session.prepare(
            "SELECT block_hash FROM headers WHERE block_num = ? AND is_orphaned = 0"
        )
        # CQL doesn't allow (is_orphaned = 0 OR is_orphaned is NULL) so we need to combine
        # the results of two queries.
        self.output_headers_stmt1 = session.prepare(
            "SELECT block_hash FROM headers WHERE block_num = ? AND is_orphaned = 0"
        )
        self.output_headers_stmt2 = session.prepare(
            "SELECT block_hash FROM headers WHERE block_num = ? AND is_orphaned is NULL"
        )


class ScyllaDBAPIQueries:
    def __init__(self, db: "ScyllaDB") -> None:
        self.db = db
        self.session: Session = self.db.session
        self.logger = logging.getLogger('scylla-api-queries')
        # Lazy load and cache prepared statements
        self.prepared_statements_cached = False
        self.get_spent_outpoints_stmts: GetSpentOutpointsStatements | None = None
        self.get_pushdata_filter_matches_stmts: GetPushdataFilterMatchesStatements | None = None

    def get_transaction_metadata_hashX(self, tx_hashX: bytes) -> TxMetadata | None:
        try:
            assert len(tx_hashX) == HashXLength
            # First, we get the transaction details
            tx_query = """
                SELECT tx_hash, tx_block_num, tx_position FROM confirmed_transactions
                WHERE tx_hash = %s;
            """
            tx_result = self.session.execute(tx_query, [tx_hashX])
            tx_rows = tx_result.all()

            # Now, we fetch the corresponding header for each transaction
            placeholders = ','.join('%s' for _ in tx_rows)
            headers_query = f"""
                SELECT block_num, block_hash, block_height FROM headers
                WHERE block_num IN ({placeholders}) AND is_orphaned = 0
                ALLOW FILTERING;
            """

            # Here we manually handle the 'join'
            params = tuple([tx_row.tx_block_num for tx_row in tx_rows])
            headers_result = self.session.execute(headers_query, parameters=params)
            header_rows = headers_result.all()
            if len(header_rows) == 0:
                return
            elif len(header_rows) == 1:
                header_row = header_rows[0]
                if header_row:
                    tx_row = [tx_row for tx_row in tx_rows if tx_row.tx_block_num == header_row.block_num][0]
                    return TxMetadata(
                        tx_row.tx_hash,
                        tx_row.tx_block_num,
                        tx_row.tx_position,
                        header_row.block_num,
                        header_row.block_hash,
                        header_row.block_height,
                    )
            else:
                raise AssertionError(
                    f"More than a single tx_hashX was returned for hashX: {tx_hashX.hex()} "
                    f"from the ScyllaDB query. This should never happen. "
                    f"It should always give a materialized view of the longest chain "
                    f"where the header's is_orphaned field is set to False"
                )
        except Exception as e:
            raise

    def get_header_data(self, block_hash: bytes, raw_header_data: bool = True) -> BlockHeaderRow | None:
        try:
            # If raw_header_data is True, we fetch all columns; otherwise, we specify which ones
            if raw_header_data:
                header_query = """
                    SELECT block_num, block_hash, block_height, block_header, block_tx_count, block_size, is_orphaned
                    FROM headers WHERE block_hash = %s AND is_orphaned = 0
                    ALLOW FILTERING;  -- Will only do scanning within the block_hash secondary index
                """
            else:
                header_query = """
                    SELECT block_num, block_hash, block_height, block_tx_count, block_size, is_orphaned
                    FROM headers WHERE block_hash = %s AND is_orphaned = 0
                    ALLOW FILTERING;  -- Will only do scanning within the block_hash secondary index
                """

            header_result = self.session.execute(header_query, [block_hash])
            header_row = header_result.one()

            if header_row:
                return BlockHeaderRow(
                    header_row.block_num,
                    header_row.block_hash.hex(),
                    header_row.block_height,
                    None if not raw_header_data else header_row.block_header.hex(),
                    header_row.block_tx_count,
                    header_row.block_size,
                    header_row.is_orphaned,
                )
            return None
        except Exception as e:
            # Handle the exception as needed
            raise

    def get_pushdata_filter_matches(self, pushdata_hashXes: list[str]):
        # NOTE: execute_concurrent_with_args returns results out of order. Need to
        # use futures callbacks and hash maps to 'join' the results together manually
        if not self.prepared_statements_cached:
            self.get_pushdata_filter_matches_stmts = GetPushdataFilterMatchesStatements(self.session)
        pushdata_stmt = self.get_pushdata_filter_matches_stmts.pushdata_stmt
        inputs_stmt = self.get_pushdata_filter_matches_stmts.inputs_stmt
        confirmed_transactions_stmt = self.get_pushdata_filter_matches_stmts.confirmed_transactions_stmt
        headers_stmt = self.get_pushdata_filter_matches_stmts.headers_stmt

        joined_rows: list[JoinedRows] = []

        try:
            # 1) Pushdata Matches
            pushdata_results: list[ExecutionResult] = execute_concurrent_with_args(
                self.session,
                pushdata_stmt,
                [(bytes.fromhex(pd_hashX),) for pd_hashX in pushdata_hashXes],
                raise_on_first_error=True,
            )
            for result in pushdata_results:
                rows = result.result_or_exc.all()
                for row in rows:
                    pd_row = PushdataResult(*row)
                    if pd_row.ref_type == PushdataMatchFlags.OUTPUT:
                        joined_rows.append(JoinedRows(pushdata_result=pd_row))
                    else:
                        self.logger.error(f"Unexpected input match - this feature is turned off")

            # 2) Input Rows & (concurrently together)
            # 3) Confirmed Transactions (concurrently together)
            futures = []

            def input_callback(joined_row: JoinedRows, future: Future[ResultSet]) -> None:
                result: ResultSet = future.result()
                joined_row.input_result = InputResult(*result.one())

            for joined_row in joined_rows:
                pd_row = joined_row.pushdata_result
                future: Future = self.db.executor.submit(
                    self.session.execute, inputs_stmt, (pd_row.tx_hash, pd_row.idx)
                )
                future.add_done_callback(partial(input_callback, joined_row))
                futures.append(future)

            def transactions_callback(joined_row: JoinedRows, future: Future[ResultSet]) -> None:
                result: ResultSet = future.result()
                joined_row.tx_result = []
                for row in result.all():
                    joined_row.tx_result.append(ConfirmedTxResult(*row))

            for joined_row in joined_rows:
                pd_row = joined_row.pushdata_result
                future: Future = self.db.executor.submit(
                    self.session.execute, confirmed_transactions_stmt, (pd_row.tx_hash, )
                )
                future.add_done_callback(partial(transactions_callback, joined_row))
                futures.append(future)

            concurrent.futures.wait(futures)

            # 4) Headers
            def headers_callback(joined_row: JoinedRows, future: Future[ResultSet]) -> None:
                result: ResultSet = future.result()
                header_result: HeaderResult = result.one()
                if header_result.is_orphaned == 1:
                    assert len(joined_row.tx_result) > 1
                    # Remove the orphaned transaction:
                    idx_for_del: int | None = None
                    for i in range(len(joined_row.tx_result)):
                        if joined_row.tx_result[i].tx_block_num == header_result.block_num:
                            idx_for_del = i
                    assert idx_for_del is not None
                    del joined_row.tx_result[idx_for_del]
                    return  # do NOT overwrite the `joined_row.header_result`
                assert header_result.is_orphaned == 0

                joined_row.header_result = header_result

            futures = []
            for joined_row in joined_rows:
                tx_rows = joined_row.tx_result
                # Reorgs result in more than one row in the transaction table with the same
                # tx_hash but with different block_num
                for tx_row in tx_rows:
                    future: Future = self.db.executor.submit(
                        self.session.execute, headers_stmt, (tx_row.tx_block_num,)
                    )
                    future.add_done_callback(partial(headers_callback, joined_row))
                    futures.append(future)

            concurrent.futures.wait(futures)

            # Now combine all data and yield results
            for joined_row in joined_rows:
                yield self.combine_rows_to_result(joined_row)
        finally:
            self.logger.exception("Unexpected exception in `get_pushdata_filter_matches`")

    def combine_rows_to_result(
        self,
        joined_row
    ):
        tx_location = TxLocation(joined_row.header_result.block_hash,
            joined_row.header_result.block_num, joined_row.tx_result[0].tx_position)

        in_tx_hash: bytes = None if joined_row.input_result is None \
            else joined_row.input_result.in_tx_hash
        in_idx: int = MAX_UINT32
        if joined_row.input_result is not None:
            in_idx = joined_row.input_result.in_idx

        return RestorationFilterQueryResult(
            ref_type=joined_row.pushdata_result.ref_type,
            pushdata_hashX=joined_row.pushdata_result.pushdata_hash,
            transaction_hash=joined_row.tx_result[0].tx_hash,
            spend_transaction_hash=in_tx_hash,
            transaction_output_index=joined_row.pushdata_result.idx,
            spend_input_index=in_idx,
            tx_location=tx_location,
        )

    def get_spent_outpoints(self, entries: list[OutpointType], lmdb: LMDB_Database) -> list[OutputSpendRow]:
        if not self.prepared_statements_cached:
            self.get_spent_outpoints_stmts = GetSpentOutpointsStatements(self.session)
        inputs_stmt = self.get_spent_outpoints_stmts.inputs_stmt
        txs_stmt = self.get_spent_outpoints_stmts.txs_stmt
        input_headers_stmt = self.get_spent_outpoints_stmts.input_headers_stmt
        output_headers_stmt1 = self.get_spent_outpoints_stmts.output_headers_stmt1
        output_headers_stmt2 = self.get_spent_outpoints_stmts.output_headers_stmt2

        # Get input rows given the outpoint
        input_results = self.db.execute_with_concurrency(inputs_stmt, entries)
        input_rows = [InputResult(*result[1]) for result in input_results if result[0]]

        # Get tx_block_num and tx_position for inputs and outputs
        input_txs_future = self.db.executor.submit(self.db.execute_with_concurrency, txs_stmt, input_rows)
        output_txs_future = self.db.executor.submit(self.db.execute_with_concurrency, txs_stmt, input_rows)
        input_txs_results = input_txs_future.result()
        output_txs_results = output_txs_future.result()
        input_txs_rows = [TxJoinResult(*result[1]) for result in input_txs_results if result[0]]
        output_txs_rows = [TxJoinResult(*result[1]) for result in output_txs_results if result[0]]

        # Get block_hash given the block_num from the tx table
        input_headers_future = self.db.executor.submit(
            self.db.execute_with_concurrency, input_headers_stmt, input_rows
        )
        output_headers_future1 = self.db.executor.submit(
            self.db.execute_with_concurrency, output_headers_stmt1, input_rows
        )
        output_headers_future2 = self.db.executor.submit(
            self.db.execute_with_concurrency, output_headers_stmt2, input_rows
        )
        input_headers_results = input_headers_future.result()
        output_headers_results1 = output_headers_future1.result()
        output_headers_results2 = output_headers_future2.result()
        input_headers_rows = [HeaderJoinResult(*result[1]) for result in input_headers_results if result[0]]
        output_headers_rows1 = [
            HeaderJoinResult(*result[1]) for result in output_headers_results1 if result[0]
        ]
        output_headers_rows2 = [
            HeaderJoinResult(*result[1]) for result in output_headers_results2 if result[0]
        ]
        output_headers_rows = output_headers_rows1
        output_headers_rows.extend(output_headers_rows2)

        assert (
            len(input_rows)
            == len(output_txs_rows)
            == len(input_txs_rows)
            == len(output_headers_rows)
            == len(input_headers_rows)
        )

        # Merge all results together
        output_spend_rows = []
        for i in range(len(input_rows)):
            input_result = input_rows[i]
            input_tx_result = input_txs_rows[i]
            output_tx_result = output_txs_rows[i]
            input_header_result = input_headers_rows[i]
            output_header_result = output_headers_rows[i]

            # Get full length tx hash for input tx
            input_tx_location = TxLocation(
                input_header_result.block_hash, input_tx_result.tx_block_num, input_tx_result.tx_position
            )
            in_tx_hash = get_full_tx_hash(input_tx_location, lmdb)
            assert in_tx_hash is not None

            # Get full length tx hash for output tx
            output_tx_location = TxLocation(
                output_header_result.block_hash, output_tx_result.tx_block_num, output_tx_result.tx_position
            )
            out_tx_hash = get_full_tx_hash(output_tx_location, lmdb)
            assert out_tx_hash is not None
            output_spend_row = OutputSpendRow(
                out_tx_hash,
                input_result.out_idx,
                in_tx_hash,
                input_result.in_idx,
                input_header_result.block_hash,
            )
            output_spend_rows.append(output_spend_row)
        return output_spend_rows
