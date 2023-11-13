import abc
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import (
    Optional,
    ParamSpec,
    TypeVar,
    cast,
)

from conduit_lib.types import TxMetadata
from .db import DBInterface
from .tip_filter_types import (
    OutboundDataFlag,
    OutboundDataRow,
    IndexerPushdataRegistrationFlag,
    TipFilterRegistrationEntry,
    AccountMetadata,
    FilterNotificationRow,
)

P1 = ParamSpec("P1")
T1 = TypeVar("T1")
T2 = TypeVar("T2")


def get_tx_metadata(tx_hash: bytes, db: DBInterface) -> TxMetadata | None:
    from conduit_lib.database.mysql.tip_filtering import get_tx_metadata
    from conduit_lib.database.mysql.db import MySQLDatabase

    db = cast(MySQLDatabase, db)
    return get_tx_metadata(tx_hash, db)


async def get_tx_metadata_async(
    tx_hash: bytes, db: DBInterface, executor: ThreadPoolExecutor
) -> TxMetadata | None:
    from conduit_lib.database.mysql.tip_filtering import get_tx_metadata_async
    from conduit_lib.database.mysql.db import MySQLDatabase

    db = cast(MySQLDatabase, db)
    return await get_tx_metadata_async(tx_hash, db, executor)


class TipFilterQueryAPI(abc.ABC):
    def __init__(self, db: DBInterface) -> None:
        self.db = db

    @classmethod
    def from_db(cls, db: DBInterface) -> "TipFilterQueryAPI":
        from conduit_lib.database.mysql.tip_filtering import MySQLTipFilterQueries
        from conduit_lib.database.mysql.db import MySQLDatabase

        db = cast(MySQLDatabase, db)
        return MySQLTipFilterQueries(db)

    @abc.abstractmethod
    def setup(self) -> None:
        ...

    @abc.abstractmethod
    def create_tables(self) -> None:
        ...

    @abc.abstractmethod
    def drop_tables(self) -> None:
        ...

    @abc.abstractmethod
    def create_accounts_table(self) -> None:
        ...

    @abc.abstractmethod
    def drop_accounts_table(self) -> None:
        ...

    @abc.abstractmethod
    def create_tip_filter_registrations_table(self) -> None:
        ...

    @abc.abstractmethod
    def drop_tip_filter_registrations_table(self) -> None:
        ...

    @abc.abstractmethod
    def create_outbound_data_table(self) -> None:
        ...

    @abc.abstractmethod
    def drop_outbound_data_table(self) -> None:
        ...

    @abc.abstractmethod
    def create_account_write(self, external_account_id: int) -> int | uuid.UUID:
        """
        This does partial updates depending on what is in `settings`.
        """
        ...

    @abc.abstractmethod
    def read_account_metadata(self, account_ids: list[uuid.UUID]) -> list[AccountMetadata]:
        ...

    @abc.abstractmethod
    def read_account_id_for_external_account_id(self, external_account_id: int) -> int | uuid.UUID:
        ...

    @abc.abstractmethod
    def create_tip_filter_registrations_write(
        self,
        external_account_id: int,
        date_created: int,
        registration_entries: list[TipFilterRegistrationEntry],
    ) -> bool:
        ...

    @abc.abstractmethod
    def read_tip_filter_registrations(
        self,
        account_id: Optional[uuid.UUID] = None,
        date_expires: Optional[int] = None,
        # These defaults include all rows no matter the flag value.
        expected_flags: IndexerPushdataRegistrationFlag = IndexerPushdataRegistrationFlag.NONE,
        mask: IndexerPushdataRegistrationFlag = IndexerPushdataRegistrationFlag.NONE,
    ) -> list[TipFilterRegistrationEntry]:
        """
        Load the non-expired tip filter registrations from the database especially to populate the
        tip filter.
        """
        ...

    @abc.abstractmethod
    def read_indexer_filtering_registrations_for_notifications(
        self, pushdata_hashes: list[bytes], account_id: uuid.UUID | None = None
    ) -> list[FilterNotificationRow]:
        """
        These are the matches that in either a new mempool transaction or a block which were
        present (correctly or falsely) in the common cuckoo filter.
        """
        ...

    @abc.abstractmethod
    def update_tip_filter_registrations_flags_write(
        self,
        external_account_id: int,
        pushdata_hashes: list[bytes],
        update_flags: IndexerPushdataRegistrationFlag = IndexerPushdataRegistrationFlag.NONE,
        update_mask: Optional[IndexerPushdataRegistrationFlag] = None,
        filter_flags: IndexerPushdataRegistrationFlag = IndexerPushdataRegistrationFlag.NONE,
        filter_mask: Optional[IndexerPushdataRegistrationFlag] = None,
        require_all: bool = False,
    ) -> None:
        """There is a MySQL and a ScyllaDB version of the update_tip_filter_registrations_flags_write
        because ScyllaDB cannot handle bitwise operations in queries and updates."""
        ...

    @abc.abstractmethod
    def expire_tip_filter_registrations(self, date_expires: int) -> list[bytes]:
        """
        Atomic call to locate expired registrations and to delete them. It will return the keys for
        all the rows that were deleted.

        Returns `[ (account_id, pushdata_hash), ... ]`
        Raises no known exceptions.
        """
        ...

    @abc.abstractmethod
    def delete_tip_filter_registrations_write(
        self,
        external_account_id: int,
        pushdata_hashes: list[bytes],
        # These defaults include all rows no matter the existing flag value.
        expected_flags: IndexerPushdataRegistrationFlag = IndexerPushdataRegistrationFlag.NONE,
        mask: IndexerPushdataRegistrationFlag = IndexerPushdataRegistrationFlag.NONE,
    ) -> None:
        ...

    @abc.abstractmethod
    def create_outbound_data_write(self, creation_row: OutboundDataRow) -> None:
        ...

    @abc.abstractmethod
    def read_pending_outbound_datas(
        self, flags: OutboundDataFlag, mask: OutboundDataFlag, account_id: int | None = None
    ) -> list[OutboundDataRow]:
        ...

    @abc.abstractmethod
    def update_outbound_data_last_tried_write(self, entries: list[tuple[OutboundDataFlag, int, int]]) -> None:
        ...
