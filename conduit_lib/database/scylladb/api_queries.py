import logging
import os

import typing
from typing import NamedTuple

from cassandra.cluster import Session
from cassandra.concurrent import execute_concurrent_with_args

from ..db_interface.tip_filter_types import OutputSpendRow
from ..lmdb.lmdb_database import get_full_tx_hash, LMDB_Database
from ...constants import MAX_UINT32
from ...types import RestorationFilterQueryResult, TxLocation, TxMetadata, BlockHeaderRow, \
    OutpointType

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

class TxJoinResult(NamedTuple):
    tx_block_num: int
    tx_position: int

class HeaderJoinResult(NamedTuple):
    block_hash: bytes


class GetPushdataFilterMatchesStatements:
    def __init__(self, session: Session):
        self.pushdata_stmt = session.prepare(
            "SELECT pushdata_hash, tx_hash, idx, ref_type FROM pushdata WHERE pushdata_hash = ?")
        self.inputs_stmt = session.prepare(
            "SELECT out_tx_hash, out_idx, in_tx_hash, in_idx FROM inputs_table WHERE out_tx_hash = ? AND out_idx = ?")
        self.confirmed_transactions_stmt = session.prepare(
            "SELECT tx_hash, tx_block_num, tx_position FROM confirmed_transactions WHERE tx_hash = ?")
        self.headers_stmt = session.prepare(
            "SELECT block_hash, block_num FROM headers WHERE block_num = ? AND is_orphaned = 0")


class GetSpentOutpointsStatements:
    def __init__(self, session: Session):
        self.inputs_stmt = session.prepare(
            "SELECT out_tx_hash, out_idx, in_tx_hash, in_idx FROM inputs_table "
            "WHERE out_tx_hash = ? AND out_idx = ?")
        self.txs_stmt = self.session.prepare(
            "SELECT tx_block_num, tx_position FROM confirmed_transactions "
            "WHERE tx_hash = ?"
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
        # Cache prepared statements to avoid overhead on each call
        self.get_pushdata_filter_matches_stmts = GetPushdataFilterMatchesStatements(self.session)
        self.get_spent_outpoints_stmts = GetSpentOutpointsStatements(self.session)

    from cassandra.cluster import Cluster

    # Assuming you've initialized the Cluster and Session elsewhere
    cluster = Cluster(['scylla-node1', 'scylla-node2'])
    session = cluster.connect('your_keyspace')

    def get_transaction_metadata_hashX(self, tx_hashX: bytes) -> TxMetadata | None:
        try:
            # First, we get the transaction details
            tx_query = """
                SELECT tx_hash, tx_block_num, tx_position FROM confirmed_transactions
                WHERE tx_hash = %s;
            """
            tx_result = self.session.execute(tx_query, [tx_hashX])
            tx_rows = tx_result.all()

            # Now, we fetch the corresponding header for each transaction
            headers_query = """
                SELECT block_num, block_hash, block_height FROM headers
                WHERE block_num = %s AND is_orphaned = 0;
            """

            # Here we manually handle the 'join'
            for tx_row in tx_rows:
                headers_result = self.session.execute(headers_query, [tx_row.tx_block_num])
                header_row = headers_result.one()

                if header_row:
                    # Assuming you have a TxMetadata class defined somewhere
                    return TxMetadata(
                        tx_row.tx_hash,
                        tx_row.tx_block_num,
                        tx_row.tx_position,
                        header_row.block_num,
                        header_row.block_hash,
                        header_row.block_height,
                    )
            return None
        except Exception as e:
            # Handle the exception as needed
            raise

    def get_header_data(self, block_hash: bytes,
            raw_header_data: bool = True) -> BlockHeaderRow | None:
        try:
            # If raw_header_data is True, we fetch all columns; otherwise, we specify which ones
            if raw_header_data:
                header_query = """
                    SELECT * FROM headers WHERE block_hash = %s AND is_orphaned = 0;
                """
            else:
                header_query = """
                    SELECT block_num, block_hash, block_height, block_tx_count, block_size
                    FROM headers WHERE block_hash = %s AND is_orphaned = 0;
                """

            header_result = self.session.execute(header_query, [block_hash])
            header_row = header_result.one()

            if header_row:
                if raw_header_data:
                    # Parse the header_row to match the expected return type
                    return BlockHeaderRow(
                        header_row.block_num,
                        header_row.block_hash.hex(),  # Assuming block_hash is a blob type
                        header_row.block_height,
                        header_row.block_header.hex(),  # Only if it's included in the fetched data
                        header_row.block_tx_count,
                        header_row.block_size,
                        header_row.is_orphaned,
                    )
                else:
                    # Parse the header_row to match the expected return type without the raw header data
                    return BlockHeaderRow(
                        header_row.block_num,
                        header_row.block_hash.hex(),  # Assuming block_hash is a blob type
                        header_row.block_height,
                        None,  # No raw header data
                        header_row.block_tx_count,
                        header_row.block_size,
                        header_row.is_orphaned,
                    )
            return None
        except Exception as e:
            # Handle the exception as needed
            raise

    def get_pushdata_filter_matches(self, pushdata_hashXes: list[str]):
        pushdata_stmt = self.get_pushdata_filter_matches_stmts.pushdata_stmt
        inputs_stmt = self.get_pushdata_filter_matches_stmts.inputs_stmt
        confirmed_transactions_stmt = self.get_pushdata_filter_matches_stmts.confirmed_transactions_stmt
        headers_stmt = self.get_pushdata_filter_matches_stmts.headers_stmt

        try:
            # First, get all pushdata concurrently
            pushdata_results = execute_concurrent_with_args(
                self.session,
                pushdata_stmt,
                [(bytes.fromhex(pd_hashX),) for pd_hashX in pushdata_hashXes],
                raise_on_first_error=True
            )
            # Extract successful pushdata fetch results
            pushdata_rows = [PushdataResult(*result[1]) for result in pushdata_results if result[0]]
            input_future = self.db.executor.submit(
                execute_concurrent_with_args,
                self.session,
                inputs_stmt,
                [(row.tx_hash, row.idx) for row in pushdata_rows],
                raise_on_first_error=True
            )
            confirmed_tx_future = self.db.executor.submit(
                execute_concurrent_with_args,
                self.session,
                confirmed_transactions_stmt,
                [(row.tx_hash,) for row in pushdata_rows],
                raise_on_first_error=True
            )
            input_rows = input_future.result()
            confirmed_tx_rows = confirmed_tx_future.result()
            input_rows = [InputResult(*result[1]) for result in input_rows if result[0]]
            confirmed_tx_rows = [ConfirmedTxResult(*result[1]) for result in confirmed_tx_rows if result[0]]
            assert len(pushdata_rows) == len(input_rows) == len(confirmed_tx_rows)
            header_results = execute_concurrent_with_args(
                self.session,
                headers_stmt,
                [(row.tx_block_num,) for row in confirmed_tx_rows],
                raise_on_first_error=True
            )
            header_rows = [HeaderResult(*result[1]) for result in header_results if result[0]]

            # Now combine all data and yield results
            for i in range(len(pushdata_rows)):
                # Combine data from pushdata, input, confirmed transactions, and header rows
                yield self.combine_rows_to_result(
                    pushdata_rows[i], input_rows[i], confirmed_tx_rows[i], header_rows[i]
                )
        finally:
            self.logger.exception("Unexpected exception in `get_pushdata_filter_matches`")

    def combine_rows_to_result(self, pushdata_row: PushdataResult, input_row: InputResult,
            confirmed_tx_row: ConfirmedTxResult, header_row: HeaderResult):
        tx_location = TxLocation(header_row.block_hash, header_row.block_num,
            confirmed_tx_row.tx_position)

        in_tx_hash: bytes = input_row.in_tx_hash  # Can be null
        in_idx: int = MAX_UINT32
        if input_row.in_idx is not None:
            in_idx = input_row.in_idx

        RestorationFilterQueryResult(
            ref_type=pushdata_row.ref_type,
            pushdata_hashX=pushdata_row.pushdata_hash,
            transaction_hash=confirmed_tx_row.tx_hash,
            spend_transaction_hash=in_tx_hash,
            transaction_output_index=input_row.out_idx,
            spend_input_index=in_idx,
            tx_location=tx_location,
        )

    def get_spent_outpoints(self, entries: list[OutpointType], lmdb: LMDB_Database) \
            -> list[OutputSpendRow]:
        inputs_stmt = self.get_spent_outpoints_stmts.inputs_stmt
        txs_stmt = self.get_spent_outpoints_stmts.txs_stmt
        input_headers_stmt = self.get_spent_outpoints_stmts.input_headers_stmt
        output_headers_stmt1 = self.get_spent_outpoints_stmts.output_headers_stmt1
        output_headers_stmt2 = self.get_spent_outpoints_stmts.output_headers_stmt2

        # Get input rows given the outpoint
        input_results = self.db.execute_with_concurrency(inputs_stmt, entries)
        input_rows = [InputResult(*result[1]) for result in input_results if result[0]]

        # Get tx_block_num and tx_position for inputs and outputs
        input_txs_future = self.db.executor.submit(self.db.execute_with_concurrency,
            txs_stmt, input_rows)
        output_txs_future = self.db.executor.submit(self.db.execute_with_concurrency,
            txs_stmt, input_rows)
        input_txs_results = input_txs_future.result()
        output_txs_results = output_txs_future.result()
        input_txs_rows = [TxJoinResult(*result[1]) for result in input_txs_results if result[0]]
        output_txs_rows = [TxJoinResult(*result[1]) for result in output_txs_results if result[0]]

        # Get block_hash given the block_num from the tx table
        input_headers_future = self.db.executor.submit(self.db.execute_with_concurrency,
            input_headers_stmt, input_rows)
        output_headers_future1 = self.db.executor.submit(self.db.execute_with_concurrency,
            output_headers_stmt1, input_rows)
        output_headers_future2 = self.db.executor.submit(self.db.execute_with_concurrency,
            output_headers_stmt2, input_rows)
        input_headers_results = input_headers_future.result()
        output_headers_results1 = output_headers_future1.result()
        output_headers_results2 = output_headers_future2.result()
        input_headers_rows = [HeaderJoinResult(*result[1]) for result in input_headers_results if result[0]]
        output_headers_rows1 = [HeaderJoinResult(*result[1]) for result in output_headers_results1 if result[0]]
        output_headers_rows2 = [HeaderJoinResult(*result[1]) for result in output_headers_results2 if result[0]]
        output_headers_rows = output_headers_rows1
        output_headers_rows.extend(output_headers_rows2)

        assert len(input_rows) == \
               len(output_txs_rows) == \
               len(input_txs_rows) == \
               len(output_headers_rows) == \
               len(input_headers_rows)

        # Merge all results together
        output_spend_rows = []
        for i in range(len(input_rows)):
            input_result = input_rows[i]
            input_tx_result = input_txs_rows[i]
            output_tx_result = output_txs_rows[i]
            input_header_result = input_headers_rows[i]
            output_header_result = output_headers_rows[i]

            # Get full length tx hash for input tx
            input_tx_location = TxLocation(input_header_result.block_hash,
                input_tx_result.tx_block_num, input_tx_result.tx_position)
            in_tx_hash = get_full_tx_hash(input_tx_location, lmdb)
            assert in_tx_hash is not None

            # Get full length tx hash for output tx
            output_tx_location = TxLocation(output_header_result.block_hash,
                output_tx_result.tx_block_num, output_tx_result.tx_position)
            out_tx_hash = get_full_tx_hash(output_tx_location, lmdb)
            assert out_tx_hash is not None
            output_spend_row = OutputSpendRow(
                out_tx_hash, input_result.out_idx,
                in_tx_hash, input_result.in_idx,
                input_header_result.block_hash)
            output_spend_rows.append(output_spend_row)
        return output_spend_rows
