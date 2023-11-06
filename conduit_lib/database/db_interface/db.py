import abc
import logging

import bitcoinx
from typing import TypeVar, Generator, Any, Sequence

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
from conduit_lib.types import (
    ChainHashes,
    BlockHeaderRow,
    TxMetadata,
    RestorationFilterQueryResult,
    OutpointType,
)

T1 = TypeVar("T1")


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

    @classmethod
    def load_db(cls, worker_id: int | None = None) -> "DBInterface":
        from conduit_lib.database.mysql.db import load_mysql_database

        return load_mysql_database(worker_id)

    @abc.abstractmethod
    def close(self) -> None:
        ...

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
        ...

    @abc.abstractmethod
    def drop_temp_mined_tx_hashes(self) -> None:
        ...

    @abc.abstractmethod
    def drop_temp_inbound_tx_hashes(self, inbound_tx_table_name: str) -> None:
        ...

    @abc.abstractmethod
    def create_temp_mined_tx_hashes_table(self) -> None:
        ...

    @abc.abstractmethod
    def create_temp_inbound_tx_hashes_table(self, inbound_tx_table_name: str) -> None:
        ...

    # QUERIES
    @abc.abstractmethod
    def load_temp_mined_tx_hashes(self, mined_tx_hashes: list[MinedTxHashes]) -> None:
        ...

    @abc.abstractmethod
    def load_temp_inbound_tx_hashes(
        self, inbound_tx_hashes: list[tuple[str]], inbound_tx_table_name: str
    ) -> None:
        ...

    @abc.abstractmethod
    def get_unprocessed_txs(
        self,
        is_reorg: bool,
        new_tx_hashes: list[tuple[str]],
        inbound_tx_table_name: str,
    ) -> set[bytes]:
        ...

    @abc.abstractmethod
    def invalidate_mempool_rows(self) -> None:
        ...

    @abc.abstractmethod
    def update_checkpoint_tip(self, checkpoint_tip: bitcoinx.Header) -> None:
        ...

    @abc.abstractmethod
    # BULK LOADS
    def bulk_load_confirmed_tx_rows(self, tx_rows: list[ConfirmedTransactionRow]) -> None:
        ...

    @abc.abstractmethod
    def bulk_load_mempool_tx_rows(self, tx_rows: list[MempoolTransactionRow]) -> None:
        ...

    @abc.abstractmethod
    def bulk_load_output_rows(self, out_rows: list[OutputRow]) -> None:
        ...

    @abc.abstractmethod
    def bulk_load_input_rows(self, in_rows: list[InputRow]) -> None:
        ...

    @abc.abstractmethod
    def bulk_load_pushdata_rows(self, pd_rows: list[PushdataRow]) -> None:
        ...

    @abc.abstractmethod
    def drop_indices(self) -> None:
        ...

    @abc.abstractmethod
    def get_tables(self) -> Sequence[tuple[str]]:
        ...

    @abc.abstractmethod
    def drop_mempool_table(self) -> None:
        ...

    @abc.abstractmethod
    def create_permanent_tables(self) -> None:
        ...

    @abc.abstractmethod
    def get_checkpoint_state(
        self,
    ) -> tuple[int, bytes, bool, bytes, bytes, bytes, bytes] | None:
        ...

    @abc.abstractmethod
    def initialise_checkpoint_state(self) -> None:
        ...

    @abc.abstractmethod
    def delete_transaction_rows(self, tx_hash_hexes: list[str]) -> None:
        ...

    @abc.abstractmethod
    def update_orphaned_headers(self, block_hashes: list[bytes]) -> None:
        ...

    @abc.abstractmethod
    def load_temp_mempool_additions(self, additions_to_mempool: set[bytes]) -> None:
        ...

    @abc.abstractmethod
    def load_temp_mempool_removals(self, removals_from_mempool: set[bytes]) -> None:
        ...

    @abc.abstractmethod
    def load_temp_orphaned_tx_hashes(self, orphaned_tx_hashes: set[bytes]) -> None:
        ...

    @abc.abstractmethod
    def update_allocated_state(
        self,
        reorg_was_allocated: bool,
        first_allocated: bitcoinx.Header,
        last_allocated: bitcoinx.Header,
        old_hashes: ChainHashes | None,
        new_hashes: ChainHashes | None,
    ) -> None:
        ...

    @abc.abstractmethod
    def drop_temp_orphaned_txs(self) -> None:
        ...

    @abc.abstractmethod
    def get_mempool_size(self) -> int:
        ...

    @abc.abstractmethod
    def remove_from_mempool(self) -> None:
        ...

    @abc.abstractmethod
    def add_to_mempool(self) -> None:
        ...

    @abc.abstractmethod
    def create_mempool_table(self) -> None:
        ...

    @abc.abstractmethod
    def delete_pushdata_rows(self, pushdata_rows: list[PushdataRow]) -> None:
        ...

    @abc.abstractmethod
    def delete_output_rows(self, output_rows: list[OutputRow]) -> None:
        ...

    @abc.abstractmethod
    def delete_input_rows(self, input_rows: list[InputRow]) -> None:
        ...

    @abc.abstractmethod
    def delete_header_row(self, block_hash: bytes) -> None:
        ...

    @abc.abstractmethod
    def get_header_data(self, block_hash: bytes, raw_header_data: bool = True) -> BlockHeaderRow | None:
        ...

    @abc.abstractmethod
    def get_transaction_metadata_hashX(self, tx_hashX: bytes) -> TxMetadata | None:
        ...

    @abc.abstractmethod
    def get_pushdata_filter_matches(
        self, pushdata_hashXes: list[str]
    ) -> Generator[RestorationFilterQueryResult, None, None]:
        ...

    @abc.abstractmethod
    def bulk_load_headers(self, block_header_rows: list[BlockHeaderRow]) -> None:
        ...

    @abc.abstractmethod
    def ping(self) -> None:
        ...

    @abc.abstractmethod
    def get_spent_outpoints(self, entries: list[OutpointType], lmdb: LMDB_Database) -> list[OutputSpendRow]:
        ...
