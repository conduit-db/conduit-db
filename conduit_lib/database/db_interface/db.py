import abc
import logging
from enum import IntEnum

import bitcoinx
from typing import TypeVar, Generator, Any, Sequence

from MySQLdb import Connection
from cassandra.cluster import Session

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
from conduit_lib.database.redis.db import RedisCache
from conduit_lib.types import (
    ChainHashes,
    BlockHeaderRow,
    TxMetadata,
    RestorationFilterQueryResult,
    OutpointType,
)

T1 = TypeVar("T1")


class DatabaseType(IntEnum):
    MySQL = 1
    ScyllaDB = 2


class DBInterface(abc.ABC):
    """Abstraction to allow for seamlessly switching from MySQL to ScyllaDB

    The `load_db` classmethod should always be used throughout the codebase to load this
    DBInterface to make switching possible whilst maintaining type safety.
    """

    def __init__(
        self,
        *args: Any,
        worker_id: int | None = None,
        **kwargs: Any,
    ) -> None:
        self.worker_id = worker_id
        self.db_type: DatabaseType | None = None

        self.session: Session | None = None  # ScyllaDB
        self.cache: RedisCache | None = None  # ScyllaDB
        self.conn: Connection | None = None  # MySQLDB

    @classmethod
    def load_db(
        cls, worker_id: int | None = None, db_type: DatabaseType = DatabaseType.MySQL
    ) -> "DBInterface":
        if db_type == DatabaseType.MySQL:
            from conduit_lib.database.mysql.db import load_mysql_database

            return load_mysql_database(worker_id)
        elif db_type == DatabaseType.ScyllaDB:
            from conduit_lib.database.scylladb.db import load_scylla_database

            return load_scylla_database(worker_id)
        raise ValueError(f"Unsupported db_type: {db_type}")

    @abc.abstractmethod
    def close(self) -> None:
        pass

    @abc.abstractmethod
    def start_transaction(self) -> None:
        raise NotImplementedError()  # Only used for MySQL

    @abc.abstractmethod
    def commit_transaction(self) -> None:
        raise NotImplementedError()  # Only used for MySQL

    @abc.abstractmethod
    def maybe_refresh_connection(
        self, last_activity: int, logger: logging.Logger
    ) -> tuple["DBInterface", int]:
        raise NotImplementedError()  # Only used for MySQL

    @abc.abstractmethod
    def drop_tables(self) -> None:
        pass

    @abc.abstractmethod
    def drop_temp_mined_tx_hashes(self) -> None:
        pass

    @abc.abstractmethod
    def drop_temp_inbound_tx_hashes(self, inbound_tx_table_name: str) -> None:
        pass

    # QUERIES
    @abc.abstractmethod
    def load_temp_mined_tx_hashes(self, mined_tx_hashes: list[MinedTxHashes]) -> None:
        pass

    @abc.abstractmethod
    def load_temp_inbound_tx_hashes(
        self, inbound_tx_hashes: list[tuple[str]], inbound_tx_table_name: str
    ) -> None:
        pass

    @abc.abstractmethod
    def get_unprocessed_txs(
        self,
        is_reorg: bool,
        new_tx_hashes: list[tuple[str]],
        inbound_tx_table_name: str,
    ) -> set[bytes]:
        pass

    @abc.abstractmethod
    def invalidate_mempool_rows(self) -> None:
        pass

    @abc.abstractmethod
    def update_checkpoint_tip(self, checkpoint_tip: bitcoinx.Header) -> None:
        pass

    @abc.abstractmethod
    # BULK LOADS
    def bulk_load_confirmed_tx_rows(
        self, tx_rows: list[ConfirmedTransactionRow], check_duplicates: bool = False
    ) -> None:
        pass

    @abc.abstractmethod
    def bulk_load_mempool_tx_rows(self, tx_rows: list[MempoolTransactionRow]) -> None:
        pass

    @abc.abstractmethod
    def bulk_load_output_rows(self, out_rows: list[OutputRow]) -> None:
        pass

    @abc.abstractmethod
    def bulk_load_input_rows(self, in_rows: list[InputRow]) -> None:
        pass

    @abc.abstractmethod
    def bulk_load_pushdata_rows(self, pd_rows: list[PushdataRow]) -> None:
        pass

    @abc.abstractmethod
    def get_tables(self) -> Sequence[tuple[str]]:
        pass

    @abc.abstractmethod
    def drop_mempool_table(self) -> None:
        pass

    @abc.abstractmethod
    def create_permanent_tables(self) -> None:
        pass

    @abc.abstractmethod
    def get_checkpoint_state(
        self,
    ) -> tuple[int, bytes, bool, bytes, bytes, bytes, bytes] | None:
        pass

    @abc.abstractmethod
    def initialise_checkpoint_state(self) -> None:
        pass

    @abc.abstractmethod
    def delete_transaction_rows(self, tx_hash_hexes: list[str]) -> None:
        pass

    @abc.abstractmethod
    def update_orphaned_headers(self, block_hashes: list[bytes]) -> None:
        pass

    @abc.abstractmethod
    def load_temp_mempool_additions(self, additions_to_mempool: set[bytes]) -> None:
        pass

    @abc.abstractmethod
    def load_temp_mempool_removals(self, removals_from_mempool: set[bytes]) -> None:
        pass

    @abc.abstractmethod
    def load_temp_orphaned_tx_hashes(self, orphaned_tx_hashes: set[bytes]) -> None:
        pass

    @abc.abstractmethod
    def update_allocated_state(
        self,
        reorg_was_allocated: bool,
        first_allocated: bitcoinx.Header,
        last_allocated: bitcoinx.Header,
        old_hashes: ChainHashes | None,
        new_hashes: ChainHashes | None,
    ) -> None:
        pass

    @abc.abstractmethod
    def drop_temp_orphaned_txs(self) -> None:
        pass

    @abc.abstractmethod
    def get_mempool_size(self) -> int:
        pass

    @abc.abstractmethod
    def remove_from_mempool(self) -> None:
        pass

    @abc.abstractmethod
    def add_to_mempool(self) -> None:
        pass

    @abc.abstractmethod
    def create_mempool_table(self) -> None:
        pass

    @abc.abstractmethod
    def delete_pushdata_rows(self, pushdata_rows: list[PushdataRow]) -> None:
        pass

    @abc.abstractmethod
    def delete_output_rows(self, output_rows: list[OutputRow]) -> None:
        pass

    @abc.abstractmethod
    def delete_input_rows(self, input_rows: list[InputRow]) -> None:
        pass

    @abc.abstractmethod
    def delete_header_row(self, block_hash: bytes) -> None:
        pass

    @abc.abstractmethod
    def get_header_data(self, block_hash: bytes, raw_header_data: bool = True) -> BlockHeaderRow | None:
        pass

    @abc.abstractmethod
    def get_transaction_metadata_hashX(self, tx_hashX: bytes) -> TxMetadata | None:
        pass

    @abc.abstractmethod
    def get_pushdata_filter_matches(
        self, pushdata_hashXes: list[str]
    ) -> Generator[RestorationFilterQueryResult, None, None]:
        pass

    @abc.abstractmethod
    def bulk_load_headers(self, block_header_rows: list[BlockHeaderRow]) -> None:
        pass

    @abc.abstractmethod
    def ping(self) -> None:
        pass

    @abc.abstractmethod
    def get_spent_outpoints(self, entries: list[OutpointType], lmdb: LMDB_Database) -> list[OutputSpendRow]:
        pass

    @abc.abstractmethod
    def create_temp_mempool_removals_table(self) -> None:
        pass

    @abc.abstractmethod
    def create_temp_mempool_additions_table(self) -> None:
        pass

    @abc.abstractmethod
    def drop_temp_mempool_removals(self) -> None:
        pass

    @abc.abstractmethod
    def drop_temp_mempool_additions(self) -> None:
        pass
