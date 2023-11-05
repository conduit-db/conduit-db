import asyncio
import logging
import os
import threading
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
def get_tx_metadata(tx_hash: bytes, db: MySQLDatabase) -> TxMetadata | None:
    """Truncates full hash -> hashX length"""
    tx_metadata = db.get_transaction_metadata_hashX(tx_hash[0:HashXLength])
    if not tx_metadata:
        return None
    return tx_metadata


async def get_tx_metadata_async(
    tx_hash: bytes, db: MySQLDatabase, executor: ThreadPoolExecutor
) -> TxMetadata | None:
    tx_metadata = await asyncio.get_running_loop().run_in_executor(executor, get_tx_metadata, tx_hash, db)
    return tx_metadata


def read_rows_by_id(
    mysqldb: MySQLDatabase,
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
    mysqldb: MySQLDatabase,
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
    def __init__(self, db: MySQLDatabase) -> None:
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
            account_id                  INT UNSIGNED     NOT NULL  AUTO_INCREMENT PRIMARY KEY,
            external_account_id         INT UNSIGNED     NOT NULL,
            account_flags               INT UNSIGNED     NOT NULL
        )
        """
        )
        if not index_exists(self.conn, 'accounts_external_id_idx', 'external_account_id'):
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
            account_id              INT UNSIGNED     NOT NULL,
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
            outbound_data_id        INT UNSIGNED     PRIMARY KEY,
            outbound_data           BINARY(32)  NOT NULL,
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

    def create_account_write(self, external_account_id: int) -> int:
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
                return cast(int, row[0])

            sql = """
            INSERT INTO accounts (external_account_id, account_flags)
            VALUES (%s, %s)
            """
            sql_values = (external_account_id, int(AccountFlag.NONE))
            cursor.execute(sql, sql_values)

            cursor.execute(
                "SELECT account_id FROM accounts WHERE external_account_id=%s",
                (external_account_id,),
            )
            row = cursor.fetchone()
            assert row is not None
            logger.debug("Created account in indexer settings db callback %d", row[0])
            return cast(int, row[0])
        finally:
            self.conn.commit()
            cursor.close()

    def read_account_metadata(self, account_ids: list[int]) -> list[AccountMetadata]:
        sql = """
            SELECT account_id, external_account_id, account_flags
            FROM accounts
            WHERE account_id IN ({})
        """
        self.db = cast(MySQLDatabase, self.db)
        return read_rows_by_id(self.db, AccountMetadata, sql, [], account_ids)

    def read_account_id_for_external_account_id(self, external_account_id: int) -> int:
        cursor: MySQLdb.cursors.Cursor = self.conn.cursor()
        cursor = cursor.execute(
            "SELECT account_id FROM accounts WHERE external_account_id=%s",
            (external_account_id,),
        )
        row = cursor.fetchone()
        assert row is not None
        return cast(int, row[0])

    def get_spent_outpoints(self, entries: list[OutpointType], lmdb: LMDB_Database) -> list[OutputSpendRow]:
        sql = """
            SELECT I.out_tx_hash, I.out_idx, I.in_tx_hash, I.in_idx,
                   HD.block_hash input_block_hash, CT.tx_block_num input_block_num, CT.tx_position input_tx_pos,
                   HD_OUT.block_hash output_block_hash, CT_OUT.tx_block_num output_block_num, CT_OUT.tx_position output_tx_pos
            FROM inputs_table I
                LEFT JOIN confirmed_transactions CT ON (CT.tx_hash = I.in_tx_hash)
                LEFT JOIN confirmed_transactions CT_OUT ON (CT_OUT.tx_hash = I.out_tx_hash)
                LEFT JOIN headers HD ON HD.block_num = CT.tx_block_num
                LEFT JOIN headers HD_OUT ON HD_OUT.block_num = CT_OUT.tx_block_num
            WHERE (HD.is_orphaned = 0 and (HD_OUT.is_orphaned = 0 OR HD_OUT.is_orphaned is NULL))
        """
        if len(entries) > 100:
            raise ValueError("Cannot process more than 100 utxos at a time")

        short_hash_entries = []
        for entry in entries:
            short_hash_entries.append((entry.tx_hash[0:HashXLength].hex(), entry.out_idx))

        parts = [sql]
        parts.append("AND ( ")
        first_entry = True
        for entryX in short_hash_entries:
            if first_entry:
                sql_condition = f"(I.out_tx_hash=X{entryX[0]!r} AND I.out_idx={entryX[1]!r}) "
                parts.append(sql_condition)
                first_entry = False
            else:
                sql_condition = f"OR (I.out_tx_hash=X{entryX[0]!r} AND I.out_idx={entryX[1]!r}) "
                parts.append(sql_condition)
        parts.append(")")

        cursor: MySQLdb.cursors.Cursor = self.conn.cursor()
        query = " ".join(parts)
        try:
            cursor.execute(query)

            output_spend_rows = []
            for row in cursor.fetchall():
                out_tx_hashX = row[0]
                out_idx = row[1]
                in_tx_hashX = row[2]
                in_idx = row[3]
                in_block_hash = row[4]
                in_block_num = row[5]
                in_tx_position = row[6]
                out_block_hash = row[7]
                out_block_num = row[8]
                out_tx_position = row[9]

                # Get full length tx hash for input tx
                input_tx_location = TxLocation(in_block_hash, in_block_num, in_tx_position)
                in_tx_hash = get_full_tx_hash(input_tx_location, lmdb)
                assert in_tx_hash is not None

                # Get full length tx hash for output tx
                output_tx_location = TxLocation(out_block_hash, out_block_num, out_tx_position)
                out_tx_hash = get_full_tx_hash(output_tx_location, lmdb)
                assert out_tx_hash is not None
                output_spend_row = OutputSpendRow(out_tx_hash, out_idx, in_tx_hash, in_idx, in_block_hash)
                output_spend_rows.append(output_spend_row)

            return output_spend_rows
        finally:
            self.conn.commit()
            cursor.close()

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
        insert_rows: list[tuple[int, bytes, int, int, int]] = []
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
        account_id: Optional[int] = None,
        date_expires: Optional[int] = None,
        # These defaults include all rows no matter the flag value.
        expected_flags: IndexerPushdataRegistrationFlag = IndexerPushdataRegistrationFlag.NONE,
        mask: IndexerPushdataRegistrationFlag = IndexerPushdataRegistrationFlag.NONE,
    ) -> list[TipFilterRegistrationEntry]:
        """
        Load the non-expired tip filter registrations from the database especially to populate the
        tip filter.
        """
        sql_values: list[Any] = [mask, int(expected_flags)]
        sql = "SELECT pushdata_hash, date_expires FROM tip_filter_registrations " "WHERE flags&%s=%s"
        if account_id is not None:
            sql += " AND account_id=%s"
            sql_values.append(account_id)
        if date_expires is not None:
            sql += " AND date_expires < %s"
            sql_values.append(date_expires)
        cursor: MySQLdb.cursors.Cursor = self.conn.cursor()
        try:
            cursor.execute(sql, sql_values)
            return [TipFilterRegistrationEntry(*row) for row in cursor.fetchall()]
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
        self.db = cast(MySQLDatabase, self.db)
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
        update_rows: list[tuple[int, int, int, bytes, int, int]] = []
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

    def expire_tip_filter_registrationss(self, date_expires: int) -> list[bytes]:
        """
        Atomic call to locate expired registrations and to delete them. It will return the keys for
        all the rows that were deleted.

        Returns `[ (account_id, pushdata_hash), ... ]`
        Raises no known exceptions.
        """
        sql_deletion = "DELETE FROM tip_filter_registrations " "WHERE flags&%s=%s AND date_expires<%s"
        sql_select = (
            "SELECT pushdata_hash FROM tip tip_filter_registrations " "WHERE flags&%s=%s AND date_expires<%s"
        )
        expected_flags = IndexerPushdataRegistrationFlag.FINALISED
        mask_flags = IndexerPushdataRegistrationFlag.DELETING | expected_flags
        sql_values = (mask_flags, expected_flags, date_expires)
        cursor: MySQLdb.cursors.Cursor = self.conn.cursor()
        try:
            cursor.execute(sql_deletion, sql_values)
            cursor.execute(sql_select, sql_values)
            rows = cursor.fetchall()
            return [cast(bytes, row[0]) for row in rows]
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
        update_rows: list[tuple[int, bytes, int, int]] = []
        for pushdata_value in pushdata_hashes:
            update_rows.append((account_id, pushdata_value, int(mask), int(expected_flags)))

        cursor: MySQLdb.cursors.Cursor = self.conn.cursor()
        try:
            cursor.executemany(sql, update_rows)
        finally:
            self.conn.commit()
            cursor.close()

    def create_outbound_data_write(self, creation_row: OutboundDataRow) -> int:
        sql = (
            "INSERT INTO outbound_data (outbound_data_id, outbound_data, outbound_data_flags, "
            "date_created, date_last_tried) VALUES (%s, %s, %s, %s, %s)"
        )
        sql_select = "SELECT outbound_data_id FROM outbound_data WHERE outbound_data_id=%s"
        cursor: MySQLdb.cursors.Cursor = self.conn.cursor()
        try:
            cursor.execute(sql, creation_row)
            cursor.execute(sql_select, [creation_row.outbound_data_id])
            result_row = cursor.fetchone()
            if result_row is None:
                raise DatabaseInsertConflict()
            return cast(int, result_row[0])
        finally:
            self.conn.commit()
            cursor.close()

    def read_pending_outbound_datas(
        self, flags: OutboundDataFlag, mask: OutboundDataFlag
    ) -> list[OutboundDataRow]:
        sql = (
            "SELECT outbound_data_id, outbound_data, outbound_data_flags, date_created, "
            "date_last_tried FROM outbound_data WHERE (outbound_data_flags&%s)=%s "
            "ORDER BY date_last_tried ASC"
        )
        sql_values = (mask, flags)
        cursor: MySQLdb.cursors.Cursor = self.conn.cursor()
        try:
            cursor.execute(sql, sql_values)
            rows = cursor.fetchall()
            return [OutboundDataRow(row[0], row[1], OutboundDataFlag(row[2]), row[3], row[4]) for row in rows]
        finally:
            self.conn.commit()
            cursor.close()

    def update_outbound_data_last_tried_write(self, entries: list[tuple[OutboundDataFlag, int, int]]) -> None:
        sql = (
            "UPDATE outbound_data SET outbound_data_flags=%s, date_last_tried=%s " "WHERE outbound_data_id=%s"
        )
        cursor: MySQLdb.cursors.Cursor = self.conn.cursor()
        try:
            entries_with_flag_as_int: list[tuple[int, int, int]] = [
                (int(outbound_data_flags), date_last_tried, outbound_data_id)
                for outbound_data_flags, date_last_tried, outbound_data_id in entries
            ]
            cursor.executemany(sql, entries_with_flag_as_int)
            assert cursor.rowcount == len(entries)
        finally:
            self.conn.commit()
            cursor.close()
