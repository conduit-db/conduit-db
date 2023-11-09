"""
These database access functions maintain the same API as the MySQL implementation which in turn is a
direct translation of the SQLite ElectrumSV reference implementation:
https://github.com/electrumsv/simple-indexer/blob/master/simple_indexer/sqlite_db.py
"""

import asyncio
import datetime
import logging
import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from operator import attrgetter
from typing import (
    Any,
    Optional,
    Sequence,
    ParamSpec,
    TypeVar,
    Type,
    Collection,
    Iterable,
)
from uuid import uuid4

from cassandra import WriteTimeout, ConsistencyLevel
from cassandra.cluster import Session
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.query import BatchStatement

from conduit_lib.constants import HashXLength
from conduit_lib.database.db_interface.tip_filter_types import (
    AccountMetadata,
    AccountFlag,
    TipFilterRegistrationEntry,
    IndexerPushdataRegistrationFlag,
    OutboundDataFlag,
    OutboundDataRow,
    FilterNotificationRow,
    DatabaseInsertConflict,
    DatabaseStateModifiedError,
)
from conduit_lib.database.db_interface.tip_filter import TipFilterQueryAPI
from conduit_lib.database.scylladb.db import ScyllaDB
from conduit_lib.types import TxMetadata

logger = logging.getLogger("sqlite-database")
mined_tx_hashes_table_lock = threading.RLock()


MARIADB_MAX_BIND_VARIABLES = 2 ^ 16 - 1


P1 = ParamSpec("P1")
T1 = TypeVar("T1")
T2 = TypeVar("T2")


# TODO Add LRU caching for these but clear the caches if there is a reorg because it
#  could result invalidating some of the cached results, Would greatly improve performance
#  for 1) Heavy key reuse 2) Multiple pushdata matches in the same transaction
def get_tx_metadata(tx_hash: bytes, db: ScyllaDB) -> TxMetadata | None:
    """Truncates full hash -> hashX length"""
    tx_metadata = db.get_transaction_metadata_hashX(tx_hash[0:HashXLength])
    if not tx_metadata:
        return None
    return tx_metadata


async def get_tx_metadata_async(
    tx_hash: bytes, db: ScyllaDB, executor: ThreadPoolExecutor
) -> TxMetadata | None:
    tx_metadata = await asyncio.get_running_loop().run_in_executor(executor, get_tx_metadata, tx_hash, db)
    return tx_metadata


def split_into_batches(iterable: Iterable[T2], batch_size: int) -> list[list[T2]]:
    """Splits an iterable into a list of lists with a maximum length of batch_size."""
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) == batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def read_rows_by_id(
    scylla_session: Session,
    return_type: Type[T1],
    cql: str,
    params: Sequence[Any],
    ids: Sequence[T2],
) -> list[T1]:
    """
    Concurrently read rows, raising an exception if any operation fails.
    """
    results: list[T1] = []
    prepared_statement = scylla_session.prepare(cql)
    query_parameters = [(tuple(params) + (batch_id,)) for batch_id in ids]
    try:
        for rows in execute_concurrent_with_args(
            scylla_session, prepared_statement, query_parameters, concurrency=100, raise_on_first_error=True
        ):
            results.extend(return_type(*row) for row in rows[1])
    except Exception as e:
        raise e

    return results


def read_rows_by_ids(
    scylla_session: Session,
    return_type: Type[T1],
    cql: str,
    cql_condition: str,
    cql_values: list[Any],
    ids: Sequence[Collection[T2]],
) -> list[T1]:
    """
    Concurrently read rows in from ScyllaDB, raising an exception if any operation fails.
    """
    results: list[T1] = []
    prepared_statement = scylla_session.prepare(cql + " WHERE " + cql_condition)
    query_parameters = [cql_values + list(batch_id) for batch_id in ids]
    try:
        for rows in execute_concurrent_with_args(
            scylla_session, prepared_statement, query_parameters, concurrency=100, raise_on_first_error=True
        ):
            results.extend(return_type(*row) for row in rows[1])
    except Exception as e:
        raise e

    return results


# TODO(db) just add this as another compositional attribute of the DBInterface
#  TipFilterQueryAPI requires a DBInterface instance anyway. And it will make it easier to test.
class ScyllaDBTipFilterQueries(TipFilterQueryAPI):
    def __init__(self, db: ScyllaDB) -> None:
        self.logger = logging.getLogger("scylladb-queries")
        self.logger.setLevel(logging.DEBUG)
        self.db = db
        self.session = db.session

    def setup(self) -> None:
        if int(os.getenv("RESET_EXTERNAL_API_DATABASE_TABLES", "0")):
            self.drop_tables()
        self.create_tables()

    def create_accounts_table(self) -> None:
        self.session.execute(
            """
            CREATE TABLE IF NOT EXISTS accounts (
                account_id                  UUID PRIMARY KEY,
                external_account_id         INT,
            ) WITH CLUSTERING ORDER BY (external_account_id DESC);
            """
        )
        # In ScyllaDB, you define the clustering order in the CREATE TABLE statement itself.
        # There's no separate command to create indexes like MySQL.

    def drop_accounts_table(self) -> None:
        self.session.execute("DROP TABLE IF EXISTS accounts")

    def create_tip_filter_registrations_table(self) -> None:
        self.session.execute(
            """
            CREATE TABLE IF NOT EXISTS tip_filter_registrations (
                account_id              UUID,
                pushdata_hash           BLOB,
                finalised_flag          BOOLEAN,
                deleting_flag           BOOLEAN,
                date_expires            TIMESTAMP,
                date_created            TIMESTAMP,
                PRIMARY KEY (account_id, pushdata_hash)
            ) WITH CLUSTERING ORDER BY (pushdata_hash ASC);
            """
        )
        self.session.execute("CREATE INDEX date_expires_idx ON tip_filter_registrations (date_expires);")
        # Deletes tip filter notifications after 30 days
        self.session.execute("ALTER TABLE tip_filter_registrations WITH default_time_to_live = 2592000;")

    def drop_tip_filter_registrations_table(self) -> None:
        self.session.execute("DROP TABLE IF EXISTS tip_filter_registrations")

    def create_outbound_data_table(self) -> None:
        self.session.execute(
            """
            CREATE TABLE IF NOT EXISTS outbound_data (
                outbound_data_id                    UUID PRIMARY KEY,
                outbound_data                       BLOB,
                tip_filter_notification_flag        BOOLEAN,
                dispatched_successfully_flag        BOOLEAN,
                date_created                        TIMESTAMP,
                date_last_tried                     TIMESTAMP
            );
            """
        )
        # UUID is used for the primary key to ensure uniqueness.

    def drop_outbound_data_table(self) -> None:
        self.session.execute("DROP TABLE IF EXISTS outbound_data")

    def create_account_write(self, external_account_id: int) -> uuid.UUID:
        """
        Create a new account or return the account_id of an existing one.
        """
        prepared_select = self.session.prepare("SELECT account_id FROM accounts WHERE external_account_id=?")
        rows = self.session.execute(prepared_select, [external_account_id])
        row = rows.one()
        if row:
            return row.account_id

        # If the account doesn't exist, create a new account with a new UUID
        new_account_id = uuid4()
        prepared_insert = self.session.prepare(
            "INSERT INTO accounts (account_id, external_account_id) VALUES (?, ?)"
        )
        self.session.execute(prepared_insert, [new_account_id, external_account_id, int(AccountFlag.NONE)])

        self.logger.debug("Created account in indexer settings db callback %s", new_account_id)
        return new_account_id

    def read_account_metadata(self, account_ids: list[int]) -> list[AccountMetadata]:
        """
        Read account metadata for a list of account_ids.
        """
        cql = """
            SELECT account_id, external_account_id FROM accounts WHERE account_id = ?
        """
        return read_rows_by_id(self.session, AccountMetadata, cql, [], account_ids)

    def read_account_id_for_external_account_id(self, external_account_id: int) -> uuid.UUID:
        """
        Fetches the account_id for a given external_account_id from the accounts table.
        """
        prepared_select = self.session.prepare("SELECT account_id FROM accounts WHERE external_account_id=?")
        rows = self.session.execute(prepared_select, [external_account_id])
        row = rows.one()
        if row is not None:
            return row.account_id
        else:
            raise ValueError("Account not found for external_account_id: {}".format(external_account_id))

    def create_tip_filter_registrations_write(
        self,
        external_account_id: int,
        date_created: datetime.datetime,
        registration_entries: list[TipFilterRegistrationEntry],
    ) -> bool:
        """
        Creates tip filter registrations for a given account and registration entries.
        """
        account_id = self.create_account_write(external_account_id)
        prepared_insert = self.session.prepare(
            """
            INSERT INTO tip_filter_registrations (account_id, pushdata_hash, finalised_flag, date_created, date_expires)
            VALUES (?, ?, ?, ?, ?)
        """
        )

        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        for entry in registration_entries:
            import datetime

            date_expires = date_created + datetime.timedelta(seconds=entry.duration_seconds)
            batch.add(
                prepared_insert,
                (
                    account_id,
                    entry.pushdata_hash,
                    1,
                    date_created,
                    date_expires,
                ),
            )

        try:
            self.session.execute(batch)
            return True
        except WriteTimeout:
            self.logger.exception("Failed inserting filtering registrations")
            return False

    def read_tip_filter_registrations(
        self,
        account_id: Optional[uuid.UUID] = None,
        date_expires: Optional[datetime.datetime] = None,
        expected_flags: IndexerPushdataRegistrationFlag = IndexerPushdataRegistrationFlag.NONE,
        mask: IndexerPushdataRegistrationFlag = IndexerPushdataRegistrationFlag.NONE,
    ) -> list[TipFilterRegistrationEntry]:
        """
        Load the non-expired tip filter registrations from the database to populate the tip filter.
        """
        assert account_id is not None, "Needed for ScyllaDB implementation"
        cql = "SELECT pushdata_hash, date_expires FROM tip_filter_registrations " "WHERE account_id=?"

        # WARNING: These are hard-coded in the ScyllaDB implementation
        assert expected_flags == IndexerPushdataRegistrationFlag.FINALISED
        assert mask == IndexerPushdataRegistrationFlag.DELETING | IndexerPushdataRegistrationFlag.FINALISED
        finalised_flag = 1
        deleting_flag = 0

        cql_values: list[Any] = [finalised_flag, deleting_flag]

        if date_expires is not None:
            cql += " AND date_expires<?"
            cql_values.append(date_expires)

        # Prepare the statement and bind the values
        prepared_statement = self.session.prepare(cql)
        rows = self.session.execute(prepared_statement, cql_values)

        # Construct TipFilterRegistrationEntry objects from result rows
        return [TipFilterRegistrationEntry(row.pushdata_hash, row.date_expires) for row in rows]

    def read_indexer_filtering_registrations_for_notifications(
        self, pushdata_hashes: list[bytes], account_id: int | None = None
    ) -> list[FilterNotificationRow]:
        """
        These are the matches that in either a new mempool transaction or a block which were
        present (correctly or falsely) in the common cuckoo filter.
        """
        assert account_id is not None, "the account_id is needed for the ScyllaDB implementation"
        results = []
        prepared_select = self.session.prepare(
            "SELECT account_id, pushdata_hash, finalised_flag, deleting_flag "
            "FROM tip_filter_registrations "
            "WHERE account_id=? AND pushdata_hash=?"
        )
        # Fetch rows individually per pushdata_hash
        batch = BatchStatement(consistency_level=ConsistencyLevel.LOCAL_ONE)
        for pushdata_hash in pushdata_hashes:
            bound_statement1 = prepared_select.bind([account_id, pushdata_hash])
            batch.add(bound_statement1)
        for rows in self.session.execute(batch):
            results.extend(
                FilterNotificationRow(row.account_id, row.pushdata_hash)
                for row in rows
                if (row.finalised_flag == 1 and row.deleting_flag == 0)
            )
        return results

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
        # WARNING: ScyllaDB does not allow bitwise operators and it is too complicated to maintain
        # the same API to this function call whilst implementing the same behaviour. So instead,
        # I notice that this function is only called once with one set of flags
        # - update_flags=IndexerPushdataRegistrationFlag.DELETING
        # - update_mask=None
        # - filter_flags=IndexerPushdataRegistrationFlag.FINALISED
        # - filter_mask=IndexerPushdataRegistrationFlag.FINALISED |
        #               IndexerPushdataRegistrationFlag.DELETING
        #
        # I will therefore implement the equivalent behaviour for ONLY this set of flags.
        # if any other combination of flags is used, an exception will be raised to be sure that
        # this workaround does not create unexpected bugs in future.
        #
        assert update_flags == IndexerPushdataRegistrationFlag.DELETING
        assert update_mask == None
        assert filter_flags == IndexerPushdataRegistrationFlag.FINALISED
        assert (
            filter_mask
            == IndexerPushdataRegistrationFlag.FINALISED | IndexerPushdataRegistrationFlag.DELETING
        )

        account_id = self.create_account_write(external_account_id)

        def _read_and_update(session: Session, pushdata_value: bytes, account_id: int) -> None:
            update_cql = (
                "UPDATE tip_filter_registrations SET deleting_flag=1 "
                "WHERE account_id=? AND pushdata_hash=? "
                "IF finalised_flag=1 AND deleting_flag=0"
            )
            # For the ScyllaDB implementation we do not use the update_mask or filter_mask
            result = session.execute(update_cql, (account_id, pushdata_value))
            was_applied = result.one().applied
            if not was_applied:
                raise DatabaseStateModifiedError

        futures = []
        for pushdata_value in pushdata_hashes:
            future = self.db.executor.submit(_read_and_update, self.session, pushdata_value, account_id)
            futures.append(future)

        for future in futures:
            try:
                future.result()  # This will block until the individual query is done
            except DatabaseStateModifiedError:
                self.logger.exception("Database is in an unexpected state")

    def expire_tip_filter_registrations(self, date_expires: int) -> list[bytes]:
        raise NotImplementedError("ScyllaDB can use a TTL setting instead")

    def delete_tip_filter_registrations_write(
        self,
        external_account_id: int,
        pushdata_hashes: list[bytes],
        expected_flags: IndexerPushdataRegistrationFlag = IndexerPushdataRegistrationFlag.NONE,
        mask: IndexerPushdataRegistrationFlag = IndexerPushdataRegistrationFlag.NONE,
    ) -> None:
        # WARNING: finalised_flag is hard-coded to 1 for the ScyllaDB implementation.
        # If this function is called with different flags, it will need updating.
        assert expected_flags == IndexerPushdataRegistrationFlag.FINALISED
        assert mask == IndexerPushdataRegistrationFlag.FINALISED
        delete_cql = """
        DELETE FROM tip_filter_registrations 
        WHERE account_id=? AND pushdata_hash=? IF finalised_flag=1
        """
        batch = BatchStatement()
        for pushdata_value in pushdata_hashes:
            batch.add(delete_cql, (external_account_id, pushdata_value))

        self.session.execute(batch)

    def create_outbound_data_write(self, creation_row: OutboundDataRow) -> int:
        # ScyllaDB uses INSERT INTO ... IF NOT EXISTS to prevent overwriting existing rows
        insert_cql = """
        INSERT INTO outbound_data (outbound_data_id, outbound_data, 
        tip_filter_notification_flag, dispatched_successfully_flag, 
        date_created, date_last_tried) VALUES (?, ?, ?, ?, ?, ?) IF NOT EXISTS
        """
        tip_filter_notification_flag = 0
        if creation_row.outbound_data_flags & OutboundDataFlag.TIP_FILTER_NOTIFICATIONS:
            tip_filter_notification_flag = 1
        dispatched_successfully_flag = 0
        if creation_row.outbound_data_flags & OutboundDataFlag.DISPATCHED_SUCCESSFULLY:
            dispatched_successfully_flag = 1

        params = (
            creation_row.outbound_data_id,
            creation_row.outbound_data,
            tip_filter_notification_flag,
            dispatched_successfully_flag,
            creation_row.date_created,
            creation_row.date_last_tried,
        )
        result = self.session.execute(insert_cql, params)
        if not result[0].applied:
            raise DatabaseInsertConflict()

        return result[0].outbound_data_id

    def read_pending_outbound_datas(
        self, flags: OutboundDataFlag, mask: OutboundDataFlag, account_id: int | None = None
    ) -> list[OutboundDataRow]:
        select_cql = """
        SELECT outbound_data_id, outbound_data, dispatched_successfully_flag, date_created, 
        date_last_tried FROM outbound_data 
        WHERE account_id=?
        """
        # Filter by dispatched_successfully_flag and sort by date_last_tried
        rows = self.session.execute(select_cql, (account_id,))
        filtered_rows = filter(lambda row: row.dispatched_successfully_flag == 0, rows)
        sorted_rows = sorted(filtered_rows, key=attrgetter('date_last_tried'))
        return [OutboundDataRow(*row) for row in sorted_rows]

    def update_outbound_data_last_tried_write(self, entries: list[tuple[OutboundDataFlag, int, int]]) -> None:
        """This implementation is hard-coded to only set the dispatched_successfully_flag=1
        because bitwise operators are not possible with CQL"""

        update_cql = """
        UPDATE outbound_data SET dispatched_successfully_flag=1, date_last_tried=? 
        WHERE outbound_data_id=?
        """
        batch = BatchStatement()
        for entry in entries:
            batch.add(update_cql, (entry[1], entry[2]))

        result = self.session.execute(batch)
        assert len(result) == len(entries)
