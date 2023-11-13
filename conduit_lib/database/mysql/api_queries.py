import logging
from typing import Iterator

import MySQLdb
import typing
from bitcoinx import hash_to_hex_str

from .tables import MySQLTables
from ..db_interface.tip_filter_types import OutputSpendRow
from ..lmdb.lmdb_database import get_full_tx_hash, LMDB_Database
from ...constants import MAX_UINT32, HashXLength
from ...types import (
    TxMetadata,
    TxLocation,
    RestorationFilterQueryResult,
    BlockHeaderRow,
    PushdataMatchFlags,
    OutpointType,
)

if typing.TYPE_CHECKING:
    from .db import MySQLDatabase


class MySQLAPIQueries:
    def __init__(
        self,
        conn: MySQLdb.Connection,
        tables: MySQLTables,
        db: "MySQLDatabase",
    ) -> None:
        self.logger = logging.getLogger("mysql-queries")
        self.logger.setLevel(logging.DEBUG)
        self.conn = conn
        self.tables = tables
        self.db = db

    def get_transaction_metadata_hashX(self, tx_hashX: bytes) -> TxMetadata | None:
        try:
            assert len(tx_hashX) == HashXLength
            sql = f"""
                SELECT CT.tx_hash, CT.tx_block_num, CT.tx_position, HD.block_num, HD.block_hash,
                    HD.block_height FROM confirmed_transactions CT
                INNER JOIN headers HD ON HD.block_num = CT.tx_block_num
                WHERE CT.tx_hash = X'{tx_hashX.hex()}' AND HD.is_orphaned = 0;"""

            self.conn.query(sql)
            result = self.conn.store_result()
            rows = result.fetch_row(0)
            self.conn.commit()

            if len(rows) == 0:
                return None
            elif len(rows) == 1:
                (
                    tx_hash,
                    tx_block_num,
                    tx_position,
                    block_num,
                    block_hash,
                    block_height,
                ) = rows[0]
                return TxMetadata(
                    tx_hash,
                    tx_block_num,
                    tx_position,
                    block_num,
                    block_hash,
                    block_height,
                )
            else:
                raise AssertionError(
                    f"More than a single tx_hashX was returned for hashX: {tx_hashX.hex()} "
                    f"from the MySQL query. This should never happen. It should always give a "
                    f"materialized view of the longest chain where the header's is_orphaned "
                    f"field is set to False"
                )
        finally:
            self.db.commit_transaction()

    def get_header_data(self, block_hash: bytes, raw_header_data: bool = True) -> BlockHeaderRow | None:
        try:
            if raw_header_data:
                sql = f"""
                    SELECT block_num, block_hash, block_height, block_header, block_tx_count, block_size, is_orphaned
                    FROM headers HD
                    WHERE HD.block_hash = X'{block_hash.hex()}'
                    AND HD.is_orphaned = 0;"""
                self.conn.query(sql)
            else:
                sql = f"""
                    SELECT block_num, block_hash, block_height, NULL, block_tx_count, block_size, is_orphaned
                    FROM headers HD
                    WHERE HD.block_hash = X'{block_hash.hex()}'
                    AND HD.is_orphaned = 0;"""
                self.conn.query(sql)
            result = self.conn.store_result()
            rows = result.fetch_row(0)
            self.conn.commit()
            if len(rows) == 0:
                return None
            (
                block_num,
                block_hash,
                block_height,
                block_header,
                block_tx_count,
                block_size,
                is_orphaned,
            ) = rows[0]
            return BlockHeaderRow(
                block_num,
                hash_to_hex_str(block_hash),
                block_height,
                None if not block_header else block_header.hex(),
                block_tx_count,
                block_size,
                is_orphaned,
            )
        finally:
            self.db.commit_transaction()

    def get_pushdata_filter_matches(
        self, pushdata_hashXes: list[str]
    ) -> Iterator[RestorationFilterQueryResult]:
        try:
            query_format_pushdata_hashes = [f"X'{pd_hashX}'" for pd_hashX in pushdata_hashXes]
            sql = f"""
                SELECT PD.pushdata_hash, PD.tx_hash, PD.idx, PD.ref_type, IT.in_tx_hash, IT.in_idx,
                    HD.block_hash, HD.block_num, CT.tx_position
                FROM pushdata PD
                LEFT JOIN inputs_table IT
                    ON PD.tx_hash=IT.out_tx_hash
                    AND PD.idx=IT.out_idx
                    AND PD.ref_type & {PushdataMatchFlags.OUTPUT} = {PushdataMatchFlags.OUTPUT}
                LEFT JOIN confirmed_transactions CT ON CT.tx_hash=PD.tx_hash
                INNER JOIN headers HD ON CT.tx_block_num = HD.block_num
                WHERE PD.pushdata_hash IN ({",".join(query_format_pushdata_hashes)})
                AND HD.is_orphaned = 0
                LIMIT 1000;"""
            self.conn.query(sql)
            result = self.conn.store_result()
            for row in result.fetch_row(0):
                pushdata_hashX: bytes = row[0]
                tx_hash: bytes = row[1]
                idx: int = row[2]
                ref_type: int = row[3]
                in_tx_hash: bytes = row[4]  # Can be null
                in_idx: int = MAX_UINT32
                if row[5] is not None:
                    in_idx = row[5]
                block_hash: bytes = row[6]
                block_num: int = row[7]
                tx_position: int = row[8]
                tx_location = TxLocation(block_hash, block_num, tx_position)
                yield RestorationFilterQueryResult(
                    ref_type=ref_type,
                    pushdata_hashX=pushdata_hashX,
                    transaction_hash=tx_hash,
                    spend_transaction_hash=in_tx_hash,
                    transaction_output_index=idx,
                    spend_input_index=in_idx,
                    tx_location=tx_location,
                )
        finally:
            self.db.commit_transaction()

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
