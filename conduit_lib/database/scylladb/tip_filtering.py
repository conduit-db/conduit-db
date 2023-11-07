import asyncio
import datetime
import logging
import os
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import (
    Any,
    cast,
    Optional,
    Sequence,
    ParamSpec,
    TypeVar,
    Type,
    Collection, Iterable,
)
from uuid import uuid4

import MySQLdb
from cassandra import WriteTimeout, ConsistencyLevel
from cassandra.cluster import Session
from cassandra.concurrent import execute_concurrent_with_args
from cassandra.query import BatchStatement

from conduit_lib.constants import HashXLength
from conduit_lib.database.db_interface.tip_filter_types import (
    AccountMetadata,
    AccountFlag,
    OutputSpendRow,
    TipFilterRegistrationEntry,
    IndexerPushdataRegistrationFlag,
    OutboundDataFlag,
    OutboundDataRow,
    FilterNotificationRow,
    DatabaseStateModifiedError,
    DatabaseInsertConflict,
)
from conduit_lib.database.db_interface.tip_filter import TipFilterQueryAPI
from conduit_lib.database.lmdb.lmdb_database import get_full_tx_hash
from conduit_lib.database.mysql.db import MySQLDatabase
from conduit_lib.database.scylladb.db import ScyllaDB
from conduit_lib.types import TxLocation, TxMetadata, OutpointType
from conduit_lib.utils import index_exists
from conduit_lib import LMDB_Database

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
                scylla_session, prepared_statement, query_parameters,
                concurrency=100, raise_on_first_error=True):
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
                scylla_session, prepared_statement, query_parameters,
                concurrency=100, raise_on_first_error=True):
            results.extend(return_type(*row) for row in rows[1])
    except Exception as e:
        raise e

    return results


class ScyllaDBTipFilterQueries(TipFilterQueryAPI):
    def __init__(self, session: Session) -> None:
        self.logger = logging.getLogger("scylladb-queries")
        self.logger.setLevel(logging.DEBUG)
        self.session = session

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
                account_flags               INT
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
                flags                   INT,
                date_expires            TIMESTAMP,
                date_created            TIMESTAMP,
                PRIMARY KEY (account_id, pushdata_hash)
            ) WITH CLUSTERING ORDER BY (pushdata_hash ASC);
            """
        )
        # Composite primary key consists of the partition key and clustering key(s).
        # In this case, account_id is the partition key, and pushdata_hash is the clustering key.

    def drop_tip_filter_registrations_table(self) -> None:
        self.session.execute("DROP TABLE IF EXISTS tip_filter_registrations")

    def create_outbound_data_table(self) -> None:
        self.session.execute(
            """
            CREATE TABLE IF NOT EXISTS outbound_data (
                outbound_data_id        UUID PRIMARY KEY,
                outbound_data           BLOB,
                outbound_data_flags     INT,
                date_created            TIMESTAMP,
                date_last_tried         TIMESTAMP
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
        prepared_select = self.session.prepare(
            "SELECT account_id FROM accounts WHERE external_account_id=?"
        )
        rows = self.session.execute(prepared_select, [external_account_id])
        row = rows.one()
        if row:
            return row.account_id

        # If the account doesn't exist, create a new account with a new UUID
        new_account_id = uuid4()
        prepared_insert = self.session.prepare(
            "INSERT INTO accounts (account_id, external_account_id, account_flags) VALUES (?, ?, ?)"
        )
        self.session.execute(prepared_insert, [new_account_id, external_account_id, int(AccountFlag.NONE)])

        self.logger.debug("Created account in indexer settings db callback %s", new_account_id)
        return new_account_id

    def read_account_metadata(self, account_ids: list[int]) -> list[AccountMetadata]:
        """
        Read account metadata for a list of account_ids.
        """
        cql = """
            SELECT account_id, external_account_id, account_flags FROM accounts WHERE account_id = ?
        """
        return read_rows_by_id(self.session, AccountMetadata, cql, [], account_ids)

    def read_account_id_for_external_account_id(self, external_account_id: int) -> uuid.UUID:
        """
        Fetches the account_id for a given external_account_id from the accounts table.
        """
        prepared_select = self.session.prepare(
            "SELECT account_id FROM accounts WHERE external_account_id=?"
        )
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
        prepared_insert = self.session.prepare("""
            INSERT INTO tip_filter_registrations (account_id, pushdata_hash, flags, date_created, date_expires)
            VALUES (?, ?, ?, ?, ?)
        """)

        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        for entry in registration_entries:
            import datetime
            date_expires = date_created + datetime.timedelta(seconds=entry.duration_seconds)
            batch.add(prepared_insert, (
                account_id,
                entry.pushdata_hash,
                int(IndexerPushdataRegistrationFlag.FINALISED),
                date_created,
                date_expires,
            ))

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
        # Construct CQL query dynamically based on provided arguments
        cql = "SELECT pushdata_hash, date_expires FROM tip_filter_registrations WHERE flags&?=?"
        cql_values: list[Any] = [mask.value, expected_flags.value]

        if account_id is not None:
            cql += " AND account_id=?"
            cql_values.append(account_id)
        if date_expires is not None:
            cql += " AND date_expires<?"
            cql_values.append(date_expires)

        # Prepare the statement and bind the values
        prepared_statement = self.session.prepare(cql)
        rows = self.session.execute(prepared_statement, cql_values)

        # Construct TipFilterRegistrationEntry objects from result rows
        return [TipFilterRegistrationEntry(row.pushdata_hash, row.date_expires) for row in rows]

    def read_indexer_filtering_registrations_for_notifications(
            self, pushdata_hashes: list[bytes]
    ) -> list[FilterNotificationRow]:
        """
        These are the matches that in either a new mempool transaction or a block which were
        present (correctly or falsely) in the common cuckoo filter.
        """
        # Note: ScyllaDB does not support the IN clause with prepared statements for non-primary key columns.
        # If pushdata_hash is not a primary key, you may need to execute multiple queries or restructure the schema.

        results = []
        prepared_select = self.session.prepare(
            "SELECT account_id, pushdata_hash FROM tip_filter_registrations "
            "WHERE flags&?=? AND pushdata_hash=?"
        )
        flags_value = IndexerPushdataRegistrationFlag.FINALISED | IndexerPushdataRegistrationFlag.DELETING

        # Fetch rows individually per pushdata_hash
        for pushdata_hash in pushdata_hashes:
            bound_statement = prepared_select.bind(
                [flags_value.value, IndexerPushdataRegistrationFlag.FINALISED.value, pushdata_hash])
            rows = self.session.execute(bound_statement)
            results.extend(FilterNotificationRow(row.account_id, row.pushdata_hash) for row in rows)

        return results

    def update_tip_filter_registrations_flags_write(self, ...):
        # Similar logic to build the UPDATE CQL statement based on provided flags
        # CQL doesn't support bitwise operations in UPDATE statements directly
        # You would need to retrieve the current flags first, perform the bitwise operation in Python, then update

        # Pseudocode for the logic since CQL does not support bitwise operations in UPDATE statements
        select_cql = "SELECT flags FROM tip_filter_registrations WHERE account_id=? AND pushdata_hash=?"
        update_cql = "UPDATE tip_filter_registrations SET flags=? WHERE account_id=? AND pushdata_hash=?"

        # Begin a batch statement
        batch = BatchStatement()

        for pushdata_value in pushdata_hashes:
            current_flags = session.execute(select_cql, (account_id, pushdata_value)).one()['flags']
            new_flags = (current_flags & int(final_update_mask)) | int(update_flags)

            # Apply filter conditions here as per your logic

            batch.add(update_cql, (new_flags, account_id, pushdata_value))

        session.execute(batch)

    def expire_tip_filter_registrations(self, date_expires):
        # ScyllaDB also doesn't support SELECT and DELETE in the same operation. You need to perform them separately.
        select_cql = """
        SELECT pushdata_hash FROM tip_filter_registrations 
        WHERE flags=? AND date_expires<?
        """
        delete_cql = """
        DELETE FROM tip_filter_registrations 
        WHERE flags=? AND date_expires<?
        """

        expired_rows = session.execute(select_cql, (expected_flags, date_expires))
        pushdata_hashes = [row['pushdata_hash'] for row in expired_rows]

        # Now, delete the expired rows
        session.execute(delete_cql, (expected_flags, date_expires))

        return pushdata_hashes

    def delete_tip_filter_registrations_write(self, ...):
        # Build and execute a DELETE CQL statement for each pushdata_hash
        delete_cql = """
        DELETE FROM tip_filter_registrations 
        WHERE account_id=? AND pushdata_hash=? AND flags=?
        """

        batch = BatchStatement()
        for pushdata_value in pushdata_hashes:
            batch.add(delete_cql, (account_id, pushdata_value, int(mask) & int(expected_flags)))

        session.execute(batch)

    def create_outbound_data_write(self, creation_row):
        # ScyllaDB uses INSERT INTO ... IF NOT EXISTS to prevent overwriting existing rows
        insert_cql = """
        INSERT INTO outbound_data (outbound_data_id, outbound_data, outbound_data_flags, 
        date_created, date_last_tried) VALUES (?, ?, ?, ?, ?) IF NOT EXISTS
        """

        result = self.session.execute(insert_cql, creation_row)

        # Check applied status to see if the insert was successful
        if not result[0].applied:
            raise DatabaseInsertConflict()

        return result[0].outbound_data_id

    def read_pending_outbound_datas(
        self, flags: OutboundDataFlag, mask: OutboundDataFlag
    ) -> list[OutboundDataRow]:
        # ScyllaDB doesn't support bitwise operations in WHERE clauses, so you would have to fetch the rows
        # and perform the bitwise operation in your application code.

        select_cql = """
        SELECT outbound_data_id, outbound_data, outbound_data_flags, date_created, 
        date_last_tried FROM outbound_data
        """

        rows = self.session.execute(select_cql)
        filtered_rows = [
            row for row in rows
            if int(row.outbound_data_flags) & mask == flags
        ]

        # Sort the filtered rows by date_last_tried in ascending order in application code
        filtered_rows.sort(key=lambda x: x.date_last_tried)

        return [OutboundDataRow(*row) for row in filtered_rows]

    def update_outbound_data_last_tried_write(self,
            entries: list[tuple[OutboundDataFlag, int, int]]) -> None:
        update_cql = """
        UPDATE outbound_data SET outbound_data_flags=?, date_last_tried=? 
        WHERE outbound_data_id=?
        """

        batch = BatchStatement()
        for entry in entries:
            batch.add(update_cql, entry)

        result = self.session.execute(batch)
        assert len(result) == len(entries)