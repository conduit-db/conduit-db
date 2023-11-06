import logging
import math
import os
import time

import bitcoinx
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, Session
from cassandra.auth import PlainTextAuthProvider
from cassandra.concurrent import execute_concurrent_with_args
from typing import Any, Callable, TypeVar

from cassandra.query import BatchStatement, BatchType

from conduit_lib.constants import PROFILING, BULK_LOADING_BATCH_SIZE_ROW_COUNT
from conduit_lib.database.mysql.types import (
    MempoolTransactionRow,
    OutputRow,
    InputRow,
    PushdataRow,
    ConfirmedTransactionRow,
    MinedTxHashes,
)
from conduit_lib.database.scylladb.exceptions import FailedScyllaOperation
from conduit_lib.database.scylladb.scylladb_api_queries import ScyllaDBAPIQueries
from conduit_lib.database.scylladb.scylladb_bulk_loads import ScyllaDBBulkLoads
from conduit_lib.database.scylladb.scylladb_queries import ScyllaDBQueries
from conduit_lib.database.scylladb.scylladb_tables import ScyllaDBTables

T1 = TypeVar("T1")

KEYSPACE = 'conduitdb'


class ScyllaDatabase:
    def __init__(self, cluster: Cluster, session: Session, worker_id: int | None = None) -> None:
        self.cluster = cluster
        self.session = session
        self.worker_id = worker_id
        self.logger = logging.getLogger("scylla-database")
        self.logger.setLevel(logging.INFO)

        self.tables = ScyllaDBTables(self)
        self.bulk_loads = ScyllaDBBulkLoads(self)
        self.queries = ScyllaDBQueries(self)
        self.api_queries = ScyllaDBAPIQueries(self)

        self.total_db_time = 0.0
        self.total_rows_flushed_since_startup = 0  # for current controller

    def close(self) -> None:
        self.cluster.shutdown()

    def prepare_batch_statement(self):
        return BatchStatement(consistency_level=ConsistencyLevel.QUORUM, batch_type=BatchType.UNLOGGED)

    def execute_batch(self, batch):
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
            batch = self.prepare_batch_statement()
            if i == BATCHES_COUNT - 1:
                rows_batch = rows[i * BATCH_SIZE :]
            else:
                rows_batch = rows[i * BATCH_SIZE : (i + 1) * BATCH_SIZE]

            for row in rows_batch:
                batch.add(insert_statement, row)
            self.execute_batch(batch)
            self.total_rows_flushed_since_startup += len(rows_batch)

        self.logger.log(
            PROFILING,
            f"total rows flushed since startup (worker_id={self.worker_id if self.worker_id else 'None'})="
            f"{self.total_rows_flushed_since_startup}",
        )

    def scylla_drop_tables(self) -> None:
        self.tables.scylla_drop_tables()

    def scylla_drop_temp_mined_tx_hashes(self) -> None:
        self.tables.scylla_drop_temp_mined_tx_hashes()

    def scylla_drop_temp_inbound_tx_hashes(self, inbound_tx_table_name: str) -> None:
        self.tables.scylla_drop_temp_inbound_tx_hashes(inbound_tx_table_name)

    def scylla_create_temp_mined_tx_hashes_table(self) -> None:
        self.tables.scylla_create_temp_mined_tx_hashes_table()

    def scylla_create_temp_inbound_tx_hashes_table(self, inbound_tx_table_name: str) -> None:
        self.tables.scylla_create_temp_inbound_tx_hashes_table(inbound_tx_table_name)

    # QUERIES
    def scylla_load_temp_mined_tx_hashes(self, mined_tx_hashes: list[MinedTxHashes]) -> None:
        self.queries.scylla_load_temp_mined_tx_hashes(mined_tx_hashes)

    def scylla_load_temp_inbound_tx_hashes(
        self, inbound_tx_hashes: list[tuple[str]], inbound_tx_table_name: str
    ) -> None:
        self.queries.scylla_load_temp_inbound_tx_hashes(inbound_tx_hashes, inbound_tx_table_name)

    def scylla_get_unprocessed_txs(
        self,
        is_reorg: bool,
        new_tx_hashes: list[tuple[str]],
        inbound_tx_table_name: str,
    ) -> set[bytes]:
        return self.queries.scylla_get_unprocessed_txs(is_reorg, new_tx_hashes, inbound_tx_table_name)

    def scylla_invalidate_mempool_rows(self) -> None:
        self.queries.scylla_invalidate_mempool_rows()

    def scylla_update_checkpoint_tip(self, checkpoint_tip: bitcoinx.Header) -> None:
        self.queries.scylla_update_checkpoint_tip(checkpoint_tip)

    # BULK LOADS
    def scylla_bulk_load_confirmed_tx_rows(self, tx_rows: list[ConfirmedTransactionRow]) -> None:
        self.bulk_loads.scylla_bulk_load_confirmed_tx_rows(tx_rows)

    def scylla_bulk_load_mempool_tx_rows(self, tx_rows: list[MempoolTransactionRow]) -> None:
        self.bulk_loads.scylla_bulk_load_mempool_tx_rows(tx_rows)

    def scylla_bulk_load_output_rows(self, out_rows: list[OutputRow]) -> None:
        self.bulk_loads.scylla_bulk_load_output_rows(out_rows)

    def scylla_bulk_load_input_rows(self, in_rows: list[InputRow]) -> None:
        self.bulk_loads.scylla_bulk_load_input_rows(in_rows)

    def scylla_bulk_load_pushdata_rows(self, pd_rows: list[PushdataRow]) -> None:
        self.bulk_loads.scylla_bulk_load_pushdata_rows(pd_rows)


def scylla_connect(keyspace: str, worker_id: int | None = None) -> ScyllaDatabase:
    auth_provider = PlainTextAuthProvider(
        username=os.environ['SCYLLA_USERNAME'],
        password=os.environ['SCYLLA_PASSWORD'],
    )
    cluster = Cluster(
        contact_points=[os.getenv('SCYLLA_HOST', '127.0.0.1')],
        port=int(os.getenv('SCYLLA_PORT', 9042)),
        auth_provider=auth_provider,
    )
    session = cluster.connect(KEYSPACE, wait_for_all_pools=True)
    return ScyllaDatabase(cluster, session, worker_id=worker_id)


# You would use scylla_connect now instead of scylla_connect to establish a connection
load_scylla_database = scylla_connect
