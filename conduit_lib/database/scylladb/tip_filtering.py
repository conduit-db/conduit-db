# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.

"""
These database access functions maintain the same API as the MySQL implementation which in turn is a
direct translation of the SQLite ElectrumSV reference implementation:
https://github.com/electrumsv/simple-indexer/blob/master/simple_indexer/sqlite_db.py
"""

import asyncio
import logging
import os
import threading
import time
import typing
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
)
from uuid import uuid4

from cassandra import WriteTimeout, ConsistencyLevel  # pylint:disable=E0611
from cassandra.cluster import Session, ResultSet  # pylint:disable=E0611
from cassandra.concurrent import execute_concurrent_with_args, ExecutionResult  # pylint:disable=E0611
from cassandra.query import BatchStatement  # pylint:disable=E0611

from conduit_lib.constants import HashXLength
from conduit_lib.database.db_interface.tip_filter_types import (
    AccountMetadata,
    TipFilterRegistrationEntry,
    IndexerPushdataRegistrationFlag,
    OutboundDataFlag,
    OutboundDataRow,
    FilterNotificationRow,
    DatabaseInsertConflict,
    DatabaseStateModifiedError,
)
from conduit_lib.database.db_interface.tip_filter import TipFilterQueryAPI
from conduit_lib.types import TxMetadata

if typing.TYPE_CHECKING:
    from conduit_lib.database.scylladb.db import ScyllaDB

logger = logging.getLogger("sqlite-database")
mined_tx_hashes_table_lock = threading.RLock()


MARIADB_MAX_BIND_VARIABLES = 2 ^ 16 - 1


P1 = ParamSpec("P1")
T1 = TypeVar("T1")
T2 = TypeVar("T2")


# TODO Add LRU caching for these but clear the caches if there is a reorg because it
#  could result invalidating some of the cached results, Would greatly improve performance
#  for 1) Heavy key reuse 2) Multiple pushdata matches in the same transaction
def get_tx_metadata(tx_hash: bytes, db: "ScyllaDB") -> TxMetadata | None:
    """Truncates full hash -> hashX length"""
    tx_metadata = db.get_transaction_metadata_hashX(tx_hash[0:HashXLength])
    if not tx_metadata:
        return None
    return tx_metadata


async def get_tx_metadata_async(
    tx_hash: bytes, db: "ScyllaDB", executor: ThreadPoolExecutor
) -> TxMetadata | None:
    tx_metadata = await asyncio.get_running_loop().run_in_executor(executor, get_tx_metadata, tx_hash, db)
    return tx_metadata


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


class ScyllaDBTipFilterQueries(TipFilterQueryAPI):
    def __init__(self, db: "ScyllaDB") -> None:
        self.logger = logging.getLogger("scylladb-queries")
        self.logger.setLevel(logging.DEBUG)
        self.db = db
        self.session = db.session

    def setup(self) -> None:
        if int(os.getenv("RESET_EXTERNAL_API_DATABASE_TABLES", "0")):
            self.drop_tables()
        self.create_tables()

    def create_tables(self) -> None:
        logger.debug("creating database tables")
        self.create_accounts_table()
        self.create_tip_filter_registrations_table()
        self.create_outbound_data_table()

    def drop_tables(self) -> None:
        logger.debug("dropping database tables")
        self.drop_accounts_table()
        self.drop_tip_filter_registrations_table()
        self.drop_outbound_data_table()

    def create_accounts_table(self) -> None:
        self.session.execute(
            """
            CREATE TABLE IF NOT EXISTS accounts (
                account_id                  UUID PRIMARY KEY,
                external_account_id         INT,
            );
            """
        )

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
                date_expires            INT,
                date_created            INT,
                PRIMARY KEY (account_id, pushdata_hash)
            ) WITH CLUSTERING ORDER BY (pushdata_hash ASC);
            """
        )
        self.session.execute(
            "CREATE INDEX IF NOT EXISTS date_expires_idx " "ON tip_filter_registrations (date_expires);"
        )
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
                date_created                        INT,
                date_last_tried                     INT
            );
            """
        )
        # UUID is used for the primary key to ensure uniqueness.

    def drop_outbound_data_table(self) -> None:
        self.session.execute("DROP TABLE IF EXISTS outbound_data")

    def create_account_write(self, external_account_id: int) -> str:
        """
        Create a new account or return the account_id of an existing one.
        """
        prepared_select = self.session.prepare(
            "SELECT account_id FROM accounts WHERE external_account_id=? ALLOW FILTERING"
        )
        rows = self.session.execute(prepared_select, [external_account_id])
        row = rows.one()
        if row:
            return str(row.account_id)

        # If the account doesn't exist, create a new account with a new UUID
        new_account_id = uuid4()
        prepared_insert = self.session.prepare(
            "INSERT INTO accounts (account_id, external_account_id) VALUES (?, ?)"
        )
        self.session.execute(prepared_insert, [new_account_id, external_account_id])
        self.logger.debug("Created account in indexer settings db callback %s", new_account_id)
        return str(new_account_id)

    def read_account_metadata(self, account_ids: list[str]) -> list[AccountMetadata]:
        """
        Read account metadata for a list of account_ids.
        """
        prepared_statement = self.session.prepare(
            "SELECT account_id, external_account_id FROM accounts WHERE account_id = ?"
        )
        self.logger.debug(f"read_account_metadata: account_ids={account_ids}")
        query_parameters = [(uuid.UUID(account_id),) for account_id in account_ids]
        try:
            execution_result_list: list[ExecutionResult] = execute_concurrent_with_args(
                self.session,
                prepared_statement,
                query_parameters,
                concurrency=100,
                raise_on_first_error=True,
            )
            results = []
            for execution_result in execution_result_list:
                for row in execution_result.result_or_exc.all():
                    results.append(AccountMetadata(str(row.account_id), row.external_account_id))
        except Exception as e:
            raise e

        return results

    def read_account_id_for_external_account_id(self, external_account_id: int) -> str:
        """
        Fetches the account_id for a given external_account_id from the accounts table.
        """
        prepared_select = self.session.prepare(
            "SELECT account_id FROM accounts WHERE external_account_id=? ALLOW FILTERING "
        )
        rows = self.session.execute(prepared_select, [external_account_id])
        row = rows.one()
        if row is not None:
            return str(row.account_id)
        else:
            raise ValueError("Account not found for external_account_id: {}".format(str(external_account_id)))

    def create_tip_filter_registrations_write(
        self,
        external_account_id: int,
        date_created: int,
        registration_entries: list[TipFilterRegistrationEntry],
    ) -> bool:
        """
        Creates tip filter registrations for a given account and registration entries.
        """
        account_id = self.create_account_write(external_account_id)
        prepared_insert = self.session.prepare(
            """
            INSERT INTO tip_filter_registrations (account_id, pushdata_hash, deleting_flag, 
                finalised_flag, date_created, date_expires)
            VALUES (?, ?, ?, ?, ?, ?)
        """
        )

        batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
        for entry in registration_entries:
            date_expires = date_created + entry.duration_seconds
            batch.add(
                prepared_insert,
                (
                    uuid.UUID(account_id),
                    entry.pushdata_hash,
                    0,
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

    def _read_tip_filter_registrations_finalised(
        self,
        date_expires: Optional[int] = None,
        expected_flags: IndexerPushdataRegistrationFlag = IndexerPushdataRegistrationFlag.NONE,
        mask: IndexerPushdataRegistrationFlag = IndexerPushdataRegistrationFlag.NONE,
    ) -> list[TipFilterRegistrationEntry]:
        assert expected_flags == IndexerPushdataRegistrationFlag.FINALISED
        assert mask == IndexerPushdataRegistrationFlag.DELETING | IndexerPushdataRegistrationFlag.FINALISED
        cql = (
            "SELECT pushdata_hash, date_expires FROM tip_filter_registrations "
            "WHERE finalised_flag=? AND deleting_flag=?"
        )
        finalised_flag = True
        deleting_flag = False
        cql_values: list[Any] = [finalised_flag, deleting_flag]

        if date_expires is not None:
            cql += " AND date_expires<?"
            cql_values.append(date_expires)

        cql += " ALLOW FILTERING"

        prepared_statement = self.session.prepare(cql)
        rows = self.session.execute(prepared_statement, cql_values)
        return [
            TipFilterRegistrationEntry(row.pushdata_hash, row.date_expires - int(time.time())) for row in rows
        ]

    def _read_tip_filter_registrations_deleting(
        self,
        date_expires: Optional[int] = None,
        expected_flags: IndexerPushdataRegistrationFlag = IndexerPushdataRegistrationFlag.NONE,
        mask: IndexerPushdataRegistrationFlag = IndexerPushdataRegistrationFlag.NONE,
    ) -> list[TipFilterRegistrationEntry]:
        assert (
            expected_flags
            == IndexerPushdataRegistrationFlag.FINALISED | IndexerPushdataRegistrationFlag.DELETING
        )
        assert mask == IndexerPushdataRegistrationFlag.DELETING | IndexerPushdataRegistrationFlag.FINALISED
        cql = (
            "SELECT pushdata_hash, date_expires FROM tip_filter_registrations "
            "WHERE finalised_flag=? AND deleting_flag=?"
        )
        finalised_flag = True
        deleting_flag = True
        cql_values: list[Any] = [finalised_flag, deleting_flag]

        if date_expires is not None:
            cql += " AND date_expires<?"
            cql_values.append(date_expires)

        cql += " ALLOW FILTERING"

        prepared_statement = self.session.prepare(cql)
        rows = self.session.execute(prepared_statement, cql_values)
        return [
            TipFilterRegistrationEntry(row.pushdata_hash, row.date_expires - int(time.time())) for row in rows
        ]

    def _read_tip_filter_registrations_all(
        self,
        date_expires: int | None = None,
        expected_flags: IndexerPushdataRegistrationFlag = IndexerPushdataRegistrationFlag.NONE,
        mask: IndexerPushdataRegistrationFlag = IndexerPushdataRegistrationFlag.NONE,
    ) -> list[TipFilterRegistrationEntry]:
        assert expected_flags == IndexerPushdataRegistrationFlag.NONE
        assert mask == IndexerPushdataRegistrationFlag.NONE
        cql = (
            "SELECT pushdata_hash, date_expires FROM tip_filter_registrations "
            "WHERE date_expires<? ALLOW FILTERING"
        )
        cql_values: list[Any] = [date_expires]

        prepared_statement = self.session.prepare(cql)
        rows = self.session.execute(prepared_statement, cql_values)
        return [
            TipFilterRegistrationEntry(row.pushdata_hash, row.date_expires - int(time.time())) for row in rows
        ]

    def read_tip_filter_registrations(
        self,
        date_expires: int | None = None,
        expected_flags: IndexerPushdataRegistrationFlag = IndexerPushdataRegistrationFlag.NONE,
        mask: IndexerPushdataRegistrationFlag = IndexerPushdataRegistrationFlag.NONE,
    ) -> list[TipFilterRegistrationEntry]:
        """
        Load the non-expired tip filter registrations from the database to populate the tip filter.
        """
        # WARNING: Only hard-coded combinations of flags can be handled in ScyllaDB because CQL
        # doesn't support bitwise operators.

        if date_expires is None:
            date_expires = int(time.time()) + 3600 * 24 * 100  # 100 days

        if (
            expected_flags == IndexerPushdataRegistrationFlag.NONE
            and mask == IndexerPushdataRegistrationFlag.NONE
        ):
            return self._read_tip_filter_registrations_all(date_expires, expected_flags, mask)
        elif (
            expected_flags == IndexerPushdataRegistrationFlag.FINALISED
            and mask == IndexerPushdataRegistrationFlag.DELETING | IndexerPushdataRegistrationFlag.FINALISED
        ):
            return self._read_tip_filter_registrations_finalised(date_expires, expected_flags, mask)
        elif (
            expected_flags
            == IndexerPushdataRegistrationFlag.FINALISED | IndexerPushdataRegistrationFlag.DELETING
            and mask == IndexerPushdataRegistrationFlag.DELETING | IndexerPushdataRegistrationFlag.FINALISED
        ):
            return self._read_tip_filter_registrations_deleting(date_expires, expected_flags, mask)
        else:
            raise NotImplementedError(
                "This combination of flags and mask are not supported by" "the ScyllaDB implementation"
            )

    def read_indexer_filtering_registrations_for_notifications(
        self, pushdata_hashes: list[bytes]
    ) -> list[FilterNotificationRow]:
        """
        These are the matches that in either a new mempool transaction or a block which were
        present (correctly or falsely) in the common cuckoo filter.
        """
        results: list[FilterNotificationRow] = []
        prepared_select = self.session.prepare(
            "SELECT account_id, pushdata_hash "
            "FROM tip_filter_registrations "
            "WHERE pushdata_hash=? "
            "AND finalised_flag=true and deleting_flag=false "
            "ALLOW FILTERING"
        )
        # Fetch rows individually per pushdata_hash
        parameters = []
        for pushdata_hash in pushdata_hashes:
            parameters.append([pushdata_hash])

        result_list: list[ExecutionResult] = execute_concurrent_with_args(
            self.session, prepared_select, parameters, concurrency=100, raise_on_first_error=True
        )
        for execution_result in result_list:
            result_set: ResultSet = execution_result.result_or_exc
            for row in result_set.all():
                results.append(FilterNotificationRow(str(row.account_id), row.pushdata_hash))
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
        assert self.db.executor is not None
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
        assert update_mask is None
        assert filter_flags == IndexerPushdataRegistrationFlag.FINALISED
        assert (
            filter_mask
            == IndexerPushdataRegistrationFlag.FINALISED | IndexerPushdataRegistrationFlag.DELETING
        )

        account_id = self.create_account_write(external_account_id)

        def _read_and_update(session: Session, pushdata_value: bytes, account_id: str) -> None:
            update_cql = (
                "UPDATE tip_filter_registrations SET deleting_flag=true "
                "WHERE account_id=%s AND pushdata_hash=%s "
                "IF finalised_flag=true AND deleting_flag=false"
            )
            # For the ScyllaDB implementation we do not use the update_mask or filter_mask
            result = session.execute(update_cql, (uuid.UUID(account_id), pushdata_value))
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

    def delete_tip_filter_registrations_write(
        self,
        external_account_id: int,
        pushdata_hashes: list[bytes],
        expected_flags: IndexerPushdataRegistrationFlag = IndexerPushdataRegistrationFlag.NONE,
        mask: IndexerPushdataRegistrationFlag = IndexerPushdataRegistrationFlag.NONE,
    ) -> None:
        # WARNING: finalised_flag is hard-coded to 1 for the ScyllaDB implementation.
        # If this function is called with different flags, it will need updating.
        account_id = self.read_account_id_for_external_account_id(external_account_id)

        assert expected_flags == IndexerPushdataRegistrationFlag.FINALISED
        assert mask == IndexerPushdataRegistrationFlag.FINALISED
        delete_cql = """
        DELETE FROM tip_filter_registrations 
        WHERE account_id=%s AND pushdata_hash=%s IF finalised_flag=true
        """
        batch = BatchStatement()
        for pushdata_value in pushdata_hashes:
            batch.add(delete_cql, (uuid.UUID(account_id), pushdata_value))

        self.session.execute(batch)

    def create_outbound_data_write(self, creation_row: OutboundDataRow) -> None:
        # ScyllaDB uses INSERT INTO ... IF NOT EXISTS to prevent overwriting existing rows
        insert_cql = """
        INSERT INTO outbound_data (outbound_data_id, outbound_data, 
            tip_filter_notification_flag, dispatched_successfully_flag, 
            date_created, date_last_tried) VALUES (%s, %s, %s, %s, %s, %s) IF NOT EXISTS
        """
        tip_filter_notification_flag = False
        if creation_row.outbound_data_flags & OutboundDataFlag.TIP_FILTER_NOTIFICATIONS:
            tip_filter_notification_flag = True
        dispatched_successfully_flag = False
        if creation_row.outbound_data_flags & OutboundDataFlag.DISPATCHED_SUCCESSFULLY:
            dispatched_successfully_flag = True

        params = (
            uuid.UUID(creation_row.outbound_data_id),
            creation_row.outbound_data,
            tip_filter_notification_flag,
            dispatched_successfully_flag,
            creation_row.date_created,
            creation_row.date_last_tried,
        )
        result: ResultSet = self.session.execute(insert_cql, params)
        if not result.was_applied:
            raise DatabaseInsertConflict()

    def read_pending_outbound_datas(
        self, flags: OutboundDataFlag, mask: OutboundDataFlag
    ) -> list[OutboundDataRow]:
        select_cql = """
        SELECT outbound_data_id, outbound_data, dispatched_successfully_flag, date_created, 
            date_last_tried FROM outbound_data
        """
        # Filter by dispatched_successfully_flag and sort by date_last_tried
        rows = self.session.execute(select_cql)
        filtered_rows = filter(lambda row: row.dispatched_successfully_flag == 0, rows)
        sorted_rows = sorted(filtered_rows, key=attrgetter('date_last_tried'))
        return [OutboundDataRow(*row) for row in sorted_rows]

    def update_outbound_data_last_tried_write(self, entries: list[tuple[OutboundDataFlag, int, str]]) -> None:
        """This implementation is hard-coded to only set the dispatched_successfully_flag=true
        because bitwise operators are not possible with CQL"""

        update_cql = """
        UPDATE outbound_data SET dispatched_successfully_flag=true, date_last_tried=%s 
        WHERE outbound_data_id=%s
        """
        batch = BatchStatement()
        for entry in entries:
            batch.add(update_cql, (entry[1], uuid.UUID(entry[2])))
        self.session.execute(batch)
