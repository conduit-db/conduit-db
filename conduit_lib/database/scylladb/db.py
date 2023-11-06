import logging
import os

import bitcoinx
from cassandra.cluster import Cluster, Session
from cassandra.auth import PlainTextAuthProvider
from typing import TypeVar, Generator, Sequence

from conduit_lib import LMDB_Database
from conduit_lib.database.db_interface.tip_filter_types import OutputSpendRow
from conduit_lib.database.db_interface.types import (
    MinedTxHashes,
    ConfirmedTransactionRow,
    MempoolTransactionRow,
    OutputRow,
    InputRow,
    PushdataRow,
)
from conduit_lib.database.scylladb.scylladb_api_queries import ScyllaDBAPIQueries
from conduit_lib.database.scylladb.scylladb_bulk_loads import ScyllaDBBulkLoads
from conduit_lib.database.scylladb.scylladb_queries import ScyllaDBQueries
from conduit_lib.database.scylladb.scylladb_tables import ScyllaDBTables
from conduit_lib.types import (
    RestorationFilterQueryResult,
    BlockHeaderRow,
    TxMetadata,
    OutpointType,
    ChainHashes,
)

T1 = TypeVar("T1")

KEYSPACE = 'conduitdb'


class ScyllaDB:
    def __init__(self, cluster: Cluster, session: Session, worker_id: int | None = None) -> None:
        self.cluster = cluster
        self.session = session
        self.worker_id = worker_id

        self.tables = ScyllaDBTables(self)
        self.bulk_loads = ScyllaDBBulkLoads(self)
        self.queries = ScyllaDBQueries(self)
        self.api_queries = ScyllaDBAPIQueries(self)

        self.logger = logging.getLogger("scylla-database")
        self.logger.setLevel(logging.INFO)

    def close(self) -> None:
        self.cluster.shutdown()

    def drop_tables(self) -> None:
        self.tables.drop_tables()

    def drop_temp_mined_tx_hashes(self) -> None:
        self.tables.drop_temp_mined_tx_hashes()

    def drop_temp_inbound_tx_hashes(self, inbound_tx_table_name: str) -> None:
        self.tables.drop_temp_inbound_tx_hashes(inbound_tx_table_name)

    def create_temp_mined_tx_hashes_table(self) -> None:
        self.tables.create_temp_mined_tx_hashes_table()

    def create_temp_inbound_tx_hashes_table(self, inbound_tx_table_name: str) -> None:
        self.tables.create_temp_inbound_tx_hashes_table(inbound_tx_table_name)

    # QUERIES
    def load_temp_mined_tx_hashes(self, mined_tx_hashes: list[MinedTxHashes]) -> None:
        self.queries.load_temp_mined_tx_hashes(mined_tx_hashes)

    def load_temp_inbound_tx_hashes(
        self, inbound_tx_hashes: list[tuple[str]], inbound_tx_table_name: str
    ) -> None:
        self.queries.load_temp_inbound_tx_hashes(inbound_tx_hashes, inbound_tx_table_name)

    def get_unprocessed_txs(
        self,
        is_reorg: bool,
        new_tx_hashes: list[tuple[str]],
        inbound_tx_table_name: str,
    ) -> set[bytes]:
        return self.queries.get_unprocessed_txs(is_reorg, new_tx_hashes, inbound_tx_table_name)

    def invalidate_mempool_rows(self) -> None:
        self.queries.invalidate_mempool_rows()

    def update_checkpoint_tip(self, checkpoint_tip: bitcoinx.Header) -> None:
        self.queries.update_checkpoint_tip(checkpoint_tip)

    # BULK LOADS
    def bulk_load_confirmed_tx_rows(self, tx_rows: list[ConfirmedTransactionRow]) -> None:
        self.bulk_loads.bulk_load_confirmed_tx_rows(tx_rows)

    def bulk_load_mempool_tx_rows(self, tx_rows: list[MempoolTransactionRow]) -> None:
        self.bulk_loads.bulk_load_mempool_tx_rows(tx_rows)

    def bulk_load_output_rows(self, out_rows: list[OutputRow]) -> None:
        self.bulk_loads.bulk_load_output_rows(out_rows)

    def bulk_load_input_rows(self, in_rows: list[InputRow]) -> None:
        self.bulk_loads.bulk_load_input_rows(in_rows)

    def bulk_load_pushdata_rows(self, pd_rows: list[PushdataRow]) -> None:
        self.bulk_loads.bulk_load_pushdata_rows(pd_rows)

    def drop_indices(self) -> None:
        self.tables.drop_indices()

    def get_tables(self) -> Sequence[tuple[str]]:
        return self.tables.get_tables()

    def drop_mempool_table(self) -> None:
        self.tables.drop_mempool_table()

    def create_mempool_table(self) -> None:
        self.tables.create_mempool_table()

    def create_permanent_tables(self) -> None:
        self.tables.create_permanent_tables()

    def get_checkpoint_state(
        self,
    ) -> tuple[int, bytes, bool, bytes, bytes, bytes, bytes] | None:
        return self.queries.get_checkpoint_state()

    def initialise_checkpoint_state(self) -> None:
        self.tables.initialise_checkpoint_state()

    def delete_transaction_rows(self, tx_hash_hexes: list[str]) -> None:
        self.queries.delete_transaction_rows(tx_hash_hexes)

    def update_orphaned_headers(self, block_hashes: list[bytes]) -> None:
        self.queries.update_orphaned_headers(block_hashes)

    def load_temp_mempool_additions(self, additions_to_mempool: set[bytes]) -> None:
        self.queries.load_temp_mempool_additions(additions_to_mempool)

    def load_temp_mempool_removals(self, removals_from_mempool: set[bytes]) -> None:
        self.queries.load_temp_mempool_removals(removals_from_mempool)

    def load_temp_orphaned_tx_hashes(self, orphaned_tx_hashes: set[bytes]) -> None:
        self.queries.load_temp_orphaned_tx_hashes(orphaned_tx_hashes)

    def update_allocated_state(
        self,
        reorg_was_allocated: bool,
        first_allocated: bitcoinx.Header,
        last_allocated: bitcoinx.Header,
        old_hashes: ChainHashes | None,
        new_hashes: ChainHashes | None,
    ) -> None:
        self.queries.update_allocated_state(
            reorg_was_allocated, first_allocated, last_allocated, old_hashes, new_hashes
        )

    def drop_temp_orphaned_txs(self) -> None:
        self.tables.drop_temp_orphaned_txs()

    def get_mempool_size(self) -> int:
        return self.queries.get_mempool_size()

    def remove_from_mempool(self) -> None:
        self.queries.remove_from_mempool()

    def add_to_mempool(self) -> None:
        self.queries.add_to_mempool()

    def delete_pushdata_rows(self, pushdata_rows: list[PushdataRow]) -> None:
        raise NotImplementedError()

    def delete_output_rows(self, output_rows: list[OutputRow]) -> None:
        raise NotImplementedError()

    def delete_input_rows(self, input_rows: list[InputRow]) -> None:
        raise NotImplementedError()

    def delete_header_row(self, block_hash: bytes) -> None:
        raise NotImplementedError()

    def get_header_data(self, block_hash: bytes, raw_header_data: bool = True) -> BlockHeaderRow | None:
        return self.api_queries.get_header_data(block_hash, raw_header_data)

    def get_transaction_metadata_hashX(self, tx_hashX: bytes) -> TxMetadata | None:
        return self.api_queries.get_transaction_metadata_hashX(tx_hashX)

    def get_pushdata_filter_matches(
        self, pushdata_hashXes: list[str]
    ) -> Generator[RestorationFilterQueryResult, None, None]:
        return self.api_queries.get_pushdata_filter_matches(pushdata_hashXes)

    def bulk_load_headers(self, block_header_rows: list[BlockHeaderRow]) -> None:
        return self.bulk_loads.bulk_load_headers(block_header_rows)

    def get_spent_outpoints(self, entries: list[OutpointType], lmdb: LMDB_Database) -> list[OutputSpendRow]:
        return self.api_queries.get_spent_outpoints(entries, lmdb)


def load_scylla_database(worker_id: int | None = None) -> ScyllaDB:
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
    return ScyllaDB(cluster, session, worker_id=worker_id)
