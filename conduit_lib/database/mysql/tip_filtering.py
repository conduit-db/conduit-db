# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.

import asyncio
import logging
import os
import time
import typing
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
    Collection,
)

import MySQLdb

from conduit_lib.constants import HashXLength
from conduit_lib.database.db_interface.tip_filter_types import (
    AccountMetadata,
    TipFilterRegistrationEntry,
    IndexerPushdataRegistrationFlag,
    OutboundDataFlag,
    OutboundDataRow,
    FilterNotificationRow,
    DatabaseStateModifiedError,
    DatabaseInsertConflict,
)
from conduit_lib.database.db_interface.tip_filter import TipFilterQueryAPI
from conduit_lib.types import TxMetadata
from conduit_lib.utils import index_exists

if typing.TYPE_CHECKING:
    from conduit_lib.database.mysql.db import MySQLDatabase

logger = logging.getLogger("tip-filtering-mysql")


MARIADB_MAX_BIND_VARIABLES = 2 ^ 16 - 1


P1 = ParamSpec("P1")
T1 = TypeVar("T1")
T2 = TypeVar("T2")


# TODO Add LRU caching for these but clear the caches if there is a reorg because it
#  could result invalidating some of the cached results, Would greatly improve performance
#  for 1) Heavy key reuse 2) Multiple pushdata matches in the same transaction
def get_tx_metadata(tx_hash: bytes, db: "MySQLDatabase") -> TxMetadata | None:
    """Truncates full hash -> hashX length"""
    tx_metadata = db.get_transaction_metadata_hashX(tx_hash[0:HashXLength])
    if not tx_metadata:
        return None
    return tx_metadata


async def get_tx_metadata_async(
    tx_hash: bytes, db: "MySQLDatabase", executor: ThreadPoolExecutor
) -> TxMetadata | None:
    tx_metadata = await asyncio.get_running_loop().run_in_executor(executor, get_tx_metadata, tx_hash, db)
    return tx_metadata


def read_rows_by_id(
    mysqldb: "MySQLDatabase",
    return_type: Type[T1],
    sql: str,
    params: Sequence[Any],
    ids: Sequence[T2],
) -> list[T1]:
    """
    Batch read rows as constrained by database limitations.
    """
    results: list[T1] = []
    batch_size = MARIADB_MAX_BIND_VARIABLES - len(params)
    remaining_ids = ids
    params = tuple(params)
    assert mysqldb.conn is not None
    cursor: MySQLdb.cursors.Cursor = mysqldb.conn.cursor()
    try:
        while len(remaining_ids):
            batch_ids = tuple(remaining_ids[:batch_size])
            sql = sql.format(",".join("%s" for k in batch_ids))
            cursor.execute(sql, params + tuple(batch_ids))
            rows = cursor.fetchall()
            # Skip copying/conversion for standard types.
            if len(rows):
                if return_type is bytes:
                    assert len(rows[0]) == 1 and type(rows[0][0]) is return_type
                    results.extend(row[0] for row in rows)
                else:
                    results.extend(return_type(*row) for row in rows)
            remaining_ids = remaining_ids[batch_size:]
        return results
    finally:
        mysqldb.conn.commit()
        cursor.close()


def read_rows_by_ids(
    mysqldb: "MySQLDatabase",
    return_type: Type[T1],
    sql: str,
    sql_condition: str,
    sql_values: list[Any],
    ids: Sequence[Collection[T2]],
) -> list[T1]:
    """
    Read rows in batches as constrained by database limitations.
    """
    batch_size = MARIADB_MAX_BIND_VARIABLES // 2 - len(sql_values)
    results: list[T1] = []
    remaining_ids = ids
    assert mysqldb.conn is not None
    cursor: MySQLdb.cursors.Cursor = mysqldb.conn.cursor()
    try:
        while len(remaining_ids):
            batch = remaining_ids[:batch_size]
            batch_values: list[Any] = list(sql_values)
            for batch_entry in batch:
                batch_values.extend(batch_entry)
            conditions = [sql_condition] * len(batch)
            batch_query = sql + " WHERE " + " OR ".join(conditions)
            cursor.execute(batch_query, batch_values)
            results.extend(return_type(*row) for row in cursor.fetchall())
            remaining_ids = remaining_ids[batch_size:]
        return results
    finally:
        mysqldb.conn.commit()
        cursor.close()


class MySQLTipFilterQueries(TipFilterQueryAPI):
    def __init__(self, db: "MySQLDatabase") -> None:
        self.logger = logging.getLogger("mysql-queries")
        self.logger.setLevel(logging.DEBUG)
        self.conn: MySQLdb.Connection = db.conn
        self.db = db

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
        self.conn.query(
            """
            CREATE TABLE IF NOT EXISTS accounts (
                account_id                  VARCHAR(36) PRIMARY KEY,
                external_account_id         INT UNSIGNED     NOT NULL
        )
        """
        )
        if not index_exists(self.conn, 'accounts_external_id_idx', 'accounts'):
            self.conn.query(
                "CREATE UNIQUE INDEX accounts_external_id_idx " "ON accounts (external_account_id)"
            )
        self.conn.commit()

    def drop_accounts_table(self) -> None:
        self.conn.query("DROP TABLE IF EXISTS accounts")
        self.conn.commit()

    def create_tip_filter_registrations_table(self) -> None:
        self.conn.query(
            """
        CREATE TABLE IF NOT EXISTS tip_filter_registrations (
            account_id              VARCHAR(36)      NOT NULL,
            pushdata_hash           BINARY(32)       NOT NULL,
            flags                   INT UNSIGNED     NOT NULL,
            date_expires            INT UNSIGNED     NOT NULL,
            date_created            INT UNSIGNED     NOT NULL
        )
        """
        )
        if not index_exists(self.conn, 'tip_filter_registrations_idx', 'tip_filter_registrations'):
            self.conn.query(
                "CREATE UNIQUE INDEX tip_filter_registrations_idx "
                "ON tip_filter_registrations (account_id, pushdata_hash)"
            )
        self.conn.commit()

    def drop_tip_filter_registrations_table(self) -> None:
        self.conn.query("DROP TABLE IF EXISTS tip_filter_registrations")
        self.conn.commit()

    def create_outbound_data_table(self) -> None:
        self.conn.query(
            """
        CREATE TABLE IF NOT EXISTS outbound_data (
            outbound_data_id        VARCHAR(36)      PRIMARY KEY,
            outbound_data           BINARY(32)       NOT NULL,
            outbound_data_flags     INT UNSIGNED     NOT NULL,
            date_created            INT UNSIGNED     NOT NULL,
            date_last_tried         INT UNSIGNED     NOT NULL
        )
        """
        )
        self.conn.commit()

    def drop_outbound_data_table(self) -> None:
        self.conn.query("DROP TABLE IF EXISTS outbound_data")
        self.conn.commit()

    def create_account_write(self, external_account_id: int) -> str:
        """
        This does partial updates depending on what is in `settings`.
        """
        sql_values: tuple[Any, ...]
        cursor: MySQLdb.cursors.Cursor = self.conn.cursor()
        try:
            cursor.execute(
                "SELECT account_id FROM accounts WHERE external_account_id=%s",
                (external_account_id,),
            )
            row = cursor.fetchone()
            if row is not None:
                return str(row[0])

            sql = """
            INSERT INTO accounts (account_id, external_account_id) VALUES (%s,%s)
            """
            sql_values = (
                str(uuid.uuid4()),
                external_account_id,
            )
            cursor.execute(sql, sql_values)

            cursor.execute(
                "SELECT account_id FROM accounts WHERE external_account_id=%s",
                (external_account_id,),
            )
            row = cursor.fetchone()
            assert row is not None
            logger.debug("Created account in indexer settings db callback %s", row[0])
            return str(row[0])
        finally:
            self.conn.commit()
            cursor.close()

    def read_account_metadata(self, account_ids: list[str]) -> list[AccountMetadata]:
        sql = """
            SELECT account_id, external_account_id
            FROM accounts
            WHERE account_id IN ({})
        """
        self.db = cast("MySQLDatabase", self.db)
        return read_rows_by_id(self.db, AccountMetadata, sql, [], account_ids)

    def read_account_id_for_external_account_id(self, external_account_id: int) -> str:
        cursor: MySQLdb.cursors.Cursor = self.conn.cursor()
        cursor.execute(
            "SELECT account_id FROM accounts WHERE external_account_id=%s",
            (external_account_id,),
        )
        row = cursor.fetchone()
        assert row is not None
        return str(row[0])

    def create_tip_filter_registrations_write(
        self,
        external_account_id: int,
        date_created: int,
        registration_entries: list[TipFilterRegistrationEntry],
    ) -> bool:
        account_id = self.create_account_write(external_account_id)
        # We use the SQLite `OR ABORT` clause to ensure we either insert all registrations or none
        # if some are already present. This means we do not need to rely on rolling back the
        # transaction because no changes should have been made in event of conflict.
        sql = """
        INSERT tip_filter_registrations (account_id, pushdata_hash, flags, date_created, 
            date_expires) VALUES (%s, %s, %s, %s, %s)"""
        insert_rows: list[tuple[str, bytes, int, int, int]] = []
        for entry in registration_entries:
            insert_rows.append(
                (
                    account_id,
                    entry.pushdata_hash,
                    int(IndexerPushdataRegistrationFlag.FINALISED),
                    date_created,
                    date_created + entry.duration_seconds,
                )
            )
        cursor = self.conn.cursor()
        try:
            cursor.executemany(sql, insert_rows)
        except MySQLdb.IntegrityError:
            logger.exception("Failed inserting filtering registrations")
            # No changes should have been made. Indicate that what was inserted was nothing.
            return False
        else:
            return True
        finally:
            self.conn.commit()
            cursor.close()

    def read_tip_filter_registrations(
        self,
        date_expires: int | None = None,
        # These defaults include all rows no matter the flag value.
        expected_flags: IndexerPushdataRegistrationFlag = IndexerPushdataRegistrationFlag.NONE,
        mask: IndexerPushdataRegistrationFlag = IndexerPushdataRegistrationFlag.NONE,
    ) -> list[TipFilterRegistrationEntry]:
        """
        Load the non-expired tip filter registrations from the database especially to populate the
        tip filter.
        """
        sql_values: list[Any] = [int(mask), int(expected_flags)]
        sql = "SELECT pushdata_hash, date_expires FROM tip_filter_registrations WHERE flags&%s=%s"
        if date_expires is not None:
            sql += " AND date_expires < %s"
            sql_values.append(date_expires)
        cursor: MySQLdb.cursors.Cursor = self.conn.cursor()
        try:
            cursor.execute(sql, sql_values)
            # duration_seconds is how many seconds are left until expiry
            return [
                TipFilterRegistrationEntry(pushdata_hash=row[0], duration_seconds=row[1] - int(time.time()))
                for row in cursor.fetchall()
            ]
        finally:
            self.conn.commit()
            cursor.close()

    def read_indexer_filtering_registrations_for_notifications(
        self, pushdata_hashes: list[bytes]
    ) -> list[FilterNotificationRow]:
        """
        These are the matches that in either a new mempool transaction or a block which were
        present (correctly or falsely) in the common cuckoo filter.
        """
        sql = (
            "SELECT account_id, pushdata_hash FROM tip_filter_registrations "
            "WHERE (flags&%s)=%s AND pushdata_hash IN ({})"
        )

        sql_values = [
            int(IndexerPushdataRegistrationFlag.FINALISED | IndexerPushdataRegistrationFlag.DELETING),
            int(IndexerPushdataRegistrationFlag.FINALISED),
        ]
        self.db = cast("MySQLDatabase", self.db)
        return read_rows_by_id(
            self.db,
            FilterNotificationRow,
            sql,
            sql_values,
            pushdata_hashes,
        )

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
        account_id = self.create_account_write(external_account_id)

        # Ensure that the update only affects the update flags if no mask is provided.
        final_update_mask = ~update_flags if update_mask is None else update_mask
        # Ensure that the filter only looks at the filter flags if no mask is provided.
        final_filter_mask = filter_flags if filter_mask is None else filter_mask
        sql = """
        UPDATE tip_filter_registrations
        SET flags=(flags&%s)|%s
        WHERE account_id=%s AND pushdata_hash=%s AND (flags&%s)=%s
        """
        update_rows: list[tuple[int, int, str, bytes, int, int]] = []
        for pushdata_value in pushdata_hashes:
            update_rows.append(
                (
                    int(final_update_mask),
                    int(update_flags),
                    account_id,
                    pushdata_value,
                    int(final_filter_mask),
                    int(filter_flags),
                )
            )
        cursor: MySQLdb.cursors.Cursor = self.conn.cursor()
        try:
            cursor.executemany(sql, update_rows)
            if require_all and cursor.rowcount != len(pushdata_hashes):
                raise DatabaseStateModifiedError
        finally:
            self.conn.commit()
            cursor.close()

    def delete_tip_filter_registrations_write(
        self,
        external_account_id: int,
        pushdata_hashes: list[bytes],
        # These defaults include all rows no matter the existing flag value.
        expected_flags: IndexerPushdataRegistrationFlag = IndexerPushdataRegistrationFlag.NONE,
        mask: IndexerPushdataRegistrationFlag = IndexerPushdataRegistrationFlag.NONE,
    ) -> None:
        account_id = self.create_account_write(external_account_id)

        sql = """
        DELETE FROM tip_filter_registrations
        WHERE account_id=%s AND pushdata_hash=%s AND flags&%s=%s
        """
        update_rows: list[tuple[str, bytes, int, int]] = []
        for pushdata_value in pushdata_hashes:
            update_rows.append((account_id, pushdata_value, int(mask), int(expected_flags)))

        cursor: MySQLdb.cursors.Cursor = self.conn.cursor()
        try:
            cursor.executemany(sql, update_rows)
        finally:
            self.conn.commit()
            cursor.close()

    def create_outbound_data_write(self, creation_row: OutboundDataRow) -> None:
        if creation_row.outbound_data_id is None:
            creation_row = creation_row._replace(outbound_data_id=str(uuid.uuid4()))
        sql = (
            "INSERT INTO outbound_data (outbound_data_id, outbound_data, outbound_data_flags, "
            "date_created, date_last_tried) VALUES (%s, %s, %s, %s, %s)"
        )
        sql_select = "SELECT outbound_data_id FROM outbound_data WHERE outbound_data_id=%s"
        cursor: MySQLdb.cursors.Cursor = self.conn.cursor()
        try:
            # Ensure outbound_data_flags is an int type
            params = (
                creation_row.outbound_data_id,
                creation_row.outbound_data,
                int(creation_row.outbound_data_flags),
                creation_row.date_created,
                creation_row.date_last_tried,
            )
            cursor.execute(sql, params)
            cursor.execute(sql_select, [creation_row.outbound_data_id])
            result_row = cursor.fetchone()
            if result_row is None:
                raise DatabaseInsertConflict()
        finally:
            self.conn.commit()
            cursor.close()

    def read_pending_outbound_datas(
        self, flags: OutboundDataFlag, mask: OutboundDataFlag
    ) -> list[OutboundDataRow]:
        # assert account_id is not None, "Needed for ScyllaDB implementation"
        sql = (
            "SELECT outbound_data_id, outbound_data, outbound_data_flags, date_created, "
            "date_last_tried FROM outbound_data WHERE (outbound_data_flags&%s)=%s "
            "ORDER BY date_last_tried ASC"
        )
        sql_values = (int(mask), int(flags))
        cursor: MySQLdb.cursors.Cursor = self.conn.cursor()
        try:
            cursor.execute(sql, sql_values)
            rows = cursor.fetchall()
            return [OutboundDataRow(row[0], row[1], OutboundDataFlag(row[2]), row[3], row[4]) for row in rows]
        finally:
            self.conn.commit()
            cursor.close()

    def update_outbound_data_last_tried_write(self, entries: list[tuple[OutboundDataFlag, int, str]]) -> None:
        sql = "UPDATE outbound_data SET outbound_data_flags=%s, date_last_tried=%s WHERE outbound_data_id=%s"
        cursor: MySQLdb.cursors.Cursor = self.conn.cursor()
        try:
            entries_with_flag_as_int: list[tuple[int, int, str]] = [
                (int(outbound_data_flags), date_last_tried, outbound_data_id)
                for outbound_data_flags, date_last_tried, outbound_data_id in entries
            ]
            cursor.executemany(sql, entries_with_flag_as_int)
            assert cursor.rowcount == len(entries)
        finally:
            self.conn.commit()
            cursor.close()
