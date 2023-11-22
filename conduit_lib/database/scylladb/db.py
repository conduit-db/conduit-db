import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor

import bitcoinx
from typing import TypeVar, Sequence, Any, cast, Iterator

from cassandra import ConsistencyLevel, WriteTimeout, WriteFailure, ProtocolVersion  # pylint:disable=E0611
from cassandra.cluster import (  # pylint:disable=E0611
    Cluster,
    Session,
    ResultSet,
    DCAwareRoundRobinPolicy,
    TokenAwarePolicy, NoHostAvailable,
)
from cassandra.concurrent import execute_concurrent_with_args, ExecutionResult  # pylint:disable=E0611
from cassandra.query import PreparedStatement  # pylint:disable=E0611

from conduit_lib import LMDB_Database, DBInterface
from conduit_lib.database.db_interface.db import DatabaseType
from conduit_lib.database.db_interface.tip_filter_types import OutputSpendRow
from conduit_lib.database.db_interface.types import (
    MinedTxHashes,
    ConfirmedTransactionRow,
    MempoolTransactionRow,
    InputRow,
    PushdataRow,
)
from conduit_lib.database.redis.db import RedisCache
from conduit_lib.database.scylladb.api_queries import ScyllaDBAPIQueries
from conduit_lib.database.scylladb.bulk_loads import ScyllaDBBulkLoads
from conduit_lib.database.scylladb.queries import ScyllaDBQueries
from conduit_lib.database.scylladb.tables import ScyllaDBTables, KEYSPACE
from conduit_lib.database.scylladb.tip_filtering import ScyllaDBTipFilterQueries
from conduit_lib.types import (
    RestorationFilterQueryResult,
    BlockHeaderRow,
    TxMetadata,
    OutpointType,
    ChainHashes,
)

T1 = TypeVar("T1")


class ScyllaDB(DBInterface):
    def __init__(self, cluster: Cluster, session: Session, cache: RedisCache, worker_id: int | None) -> None:
        self.cluster: Cluster = cluster
        self.session: Session = session
        self.db_type = DatabaseType.ScyllaDB
        # Redis is needed to compensate for lack of in-memory only tables or SQL temp tables
        self.cache: RedisCache = cache

        self.worker_id = worker_id

        self.tables = ScyllaDBTables(self)
        self.create_keyspace()
        session.set_keyspace(KEYSPACE)

        self.bulk_loads = ScyllaDBBulkLoads(self)
        self.queries = ScyllaDBQueries(self)
        self.api_queries = ScyllaDBAPIQueries(self)
        self.tip_filter_api = ScyllaDBTipFilterQueries(self)

        self.executor = ThreadPoolExecutor(max_workers=100)

        self.logger = logging.getLogger("scylla-database")
        self.logger.setLevel(logging.INFO)

    def close(self) -> None:
        self.cluster.shutdown()

    def commit_transaction(self) -> None:
        pass  # Not used in the ScyllaDB implementation

    def create_keyspace(self) -> None:
        self.tables.create_keyspace()

    def drop_keyspace(self) -> None:
        self.tables.drop_keyspace()

    def maybe_refresh_connection(self, last_activity: int, logger: logging.Logger) -> tuple[DBInterface, int]:
        return self, int(time.time())

    def ping(self) -> None:
        # The below query is lightweight and often used to test connectivity
        rs = self.session.execute('SELECT now() FROM system.local')
        print("Ping successful, connected to cluster: " + str(self.cluster.metadata.cluster_name))
        for row in rs:
            print("Date and time from ScyllaDB:", row[0])

    def start_transaction(self) -> None:
        pass  # Not used in the ScyllaDB implementation

    def load_data_batched(self, insert_statement: PreparedStatement, rows: Sequence[tuple[Any, ...]]) -> None:
        self.bulk_loads.load_data_batched(insert_statement, rows)

    def execute_with_concurrency(
        self, prepared_statement: PreparedStatement, rows: Sequence[tuple[Any, ...]], concurrency: int = 100
    ) -> list[ResultSet]:
        try:
            execution_results: list[ExecutionResult] = execute_concurrent_with_args(
                self.session,
                prepared_statement,
                rows,
                concurrency=concurrency,
                raise_on_first_error=True,
            )
            result_sets: list[ResultSet] = []
            for execution_result in execution_results:
                result_sets.append(cast(ResultSet, execution_result.result_or_exc))
            return result_sets
        except (WriteTimeout, WriteFailure) as e:
            self.logger.error(f"Error occurred during concurrent write operations: {str(e)}")
            raise

    def drop_tables(self) -> None:
        self.tables.drop_tables()

    def drop_temp_mined_tx_hashes(self) -> None:
        self.tables.drop_temp_mined_tx_hashes()

    def drop_temp_inbound_tx_hashes(self, inbound_tx_table_name: str) -> None:
        self.tables.drop_temp_inbound_tx_hashes(inbound_tx_table_name)

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
    def bulk_load_confirmed_tx_rows(
        self, tx_rows: list[ConfirmedTransactionRow], check_duplicates: bool = False
    ) -> None:
        self.bulk_loads.bulk_load_confirmed_tx_rows(tx_rows, check_duplicates)

    def bulk_load_mempool_tx_rows(self, tx_rows: list[MempoolTransactionRow]) -> None:
        self.bulk_loads.bulk_load_mempool_tx_rows(tx_rows)

    def bulk_load_input_rows(self, in_rows: list[InputRow]) -> None:
        self.bulk_loads.bulk_load_input_rows(in_rows)

    def bulk_load_pushdata_rows(self, pd_rows: list[PushdataRow]) -> None:
        self.bulk_loads.bulk_load_pushdata_rows(pd_rows)

    def get_tables(self) -> Sequence[tuple[str]]:
        return self.tables.get_tables()

    def drop_mempool_table(self) -> None:
        self.cache.r.delete(b"mempool")

    def create_mempool_table(self) -> None:
        pass  # The ScyllaDB implementation uses Redis in place of MySQL temp tables

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
        return self.queries.delete_pushdata_rows(pushdata_rows)

    def delete_input_rows(self, input_rows: list[InputRow]) -> None:
        return self.queries.delete_input_rows(input_rows)

    def delete_header_rows(self, block_hashes: list[bytes]) -> None:
        return self.queries.delete_header_rows(block_hashes)

    def get_header_data(self, block_hash: bytes, raw_header_data: bool = True) -> BlockHeaderRow | None:
        return self.api_queries.get_header_data(block_hash, raw_header_data)

    def get_transaction_metadata_hashX(self, tx_hashX: bytes) -> TxMetadata | None:
        return self.api_queries.get_transaction_metadata_hashX(tx_hashX)

    def get_pushdata_filter_matches(
        self, pushdata_hashXes: list[str]
    ) -> Iterator[RestorationFilterQueryResult]:
        return self.api_queries.get_pushdata_filter_matches(pushdata_hashXes)

    def bulk_load_headers(self, block_header_rows: list[BlockHeaderRow]) -> None:
        return self.bulk_loads.bulk_load_headers(block_header_rows)

    def get_spent_outpoints(self, entries: list[OutpointType], lmdb: LMDB_Database) -> list[OutputSpendRow]:
        return self.api_queries.get_spent_outpoints(entries, lmdb)

    def create_temp_mempool_removals_table(self) -> None:
        # This is not needed in the ScyllaDB implementation. Redis is used for this.
        pass

    def create_temp_mempool_additions_table(self) -> None:
        # This is not needed in the ScyllaDB implementation. Redis is used for this.
        pass

    def create_temp_orphaned_txs_table(self) -> None:
        # This is not needed in the ScyllaDB implementation. Redis is used for this.
        pass

    def drop_temp_mempool_removals(self) -> None:
        self.tables.drop_temp_mempool_removals()

    def drop_temp_mempool_additions(self) -> None:
        self.tables.drop_temp_mempool_additions()


def wait_for_db_ready() -> ScyllaDB | None:
    while True:
        try:
            scylladb = load_scylla_database()
            print('ScyllaDB is now available')
            return scylladb
        except (NoHostAvailable, ConnectionRefusedError):
            print("ScyllaDB is not yet available")
        except Exception as e:
            print(f"Unexpected exception type: {e}. Exiting loop")
            return None


def load_scylla_database(worker_id: int | None = None) -> ScyllaDB:
    # auth_provider = PlainTextAuthProvider(
    #     username=os.environ['SCYLLA_USERNAME'],
    #     password=os.environ['SCYLLA_PASSWORD'],
    # )
    logger = logging.getLogger("load_scylla_database")
    logging.getLogger('cassandra').setLevel(logging.CRITICAL)
    while True:
        try:
            cluster = Cluster(
                contact_points=[os.getenv('SCYLLA_HOST', '127.0.0.1')],
                port=int(os.getenv('SCYLLA_PORT', 19042)),
                protocol_version=ProtocolVersion.V4,
                load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy()),
                executor_threads=4,
                connect_timeout=30,
                idle_heartbeat_interval=15
                # auth_provider=auth_provider,
            )
            session = cluster.connect()
            session.default_timeout = 120
            session.default_consistency_level = ConsistencyLevel.LOCAL_QUORUM
            logging.getLogger('cassandra').setLevel(logging.WARNING)
            logger.info("ScyllaDB is now available")
            cache = RedisCache()
            return ScyllaDB(cluster, session, cache, worker_id=worker_id)
        except (NoHostAvailable, ConnectionRefusedError):
            logger.info("ScyllaDB is not yet available")
            time.sleep(2)
        except Exception as e:
            logger.exception(f"Unexpected exception type: {e}. Exiting loop")
            exit(1)
