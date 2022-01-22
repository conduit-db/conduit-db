import logging
from typing import Generator, Optional

import MySQLdb
import typing
from bitcoinx import hash_to_hex_str

from .mysql_tables import MySQLTables
from ...constants import MAX_UINT32
from ...types import TxMetadata, TxLocation, RestorationFilterQueryResult, \
    _get_pushdata_match_flag, BlockHeaderRow

if typing.TYPE_CHECKING:
    from ... import MySQLDatabase


class MySQLAPIQueries:

    def __init__(self, mysql_conn: MySQLdb.Connection, mysql_tables: MySQLTables,
            mysql_db: 'MySQLDatabase') -> None:
        self.logger = logging.getLogger("mysql-queries")
        self.logger.setLevel(logging.DEBUG)
        self.mysql_conn = mysql_conn
        self.mysql_tables = mysql_tables
        self.mysql_db = mysql_db

    def get_transaction_metadata_hashX(self, tx_hashX: bytes) -> Optional[TxMetadata]:
        try:
            sql = f"""
                SELECT CT.tx_hash, CT.tx_block_num, CT.tx_position, HD.block_num, HD.block_hash, 
                    HD.block_height FROM confirmed_transactions CT
                INNER JOIN headers HD
                ON HD.block_num = CT.tx_block_num
                WHERE CT.tx_hash = X'{tx_hashX.hex()}' 
                AND HD.is_orphaned = 0;"""

            self.mysql_conn.query(sql)
            result = self.mysql_conn.store_result()
            rows = result.fetch_row(0)

            self.mysql_conn.commit()

            if len(rows) == 0:
                return None
            elif len(rows) == 1:  # no reorgs
                tx_hash, tx_block_num, tx_position, block_num, block_hash, block_height = rows[0]
                return TxMetadata(tx_hash, tx_block_num, tx_position, block_num, block_hash,
                    block_height)
            else:
                # TODO Investigate why this is happening
                # If it's a duplicate entry let it return
                block_nums = [row[3] for row in rows]
                if all([block_nums[0] == block_num for block_num in block_nums]):
                    self.logger.debug(f"Got duplicate tx row for transaction: {tx_hashX.hex()}")
                    tx_hash, tx_block_num, tx_position, block_num, block_hash, block_height = rows[0]
                    return TxMetadata(tx_hash, tx_block_num, tx_position, block_num, block_hash,
                        block_height)

                raise ValueError(f"More than a single tx_hashX was returned for: {tx_hashX.hex()} "
                    f"from the MySQL query. This should never happen. It should always give a "
                    f"materialized view of the longest chain")  # i.e. WHERE HD.is_orphaned = 0

        finally:
            self.mysql_db.commit_transaction()

    def get_header_data(self, block_hash: bytes, raw_header_data: bool=True) \
            -> Optional[BlockHeaderRow]:
        try:
            if raw_header_data:
                sql = f"""
                    SELECT * 
                    FROM headers HD 
                    WHERE HD.block_hash = X'{block_hash.hex()}' 
                    AND HD.is_orphaned = 0;"""
                self.mysql_conn.query(sql)
            else:
                sql = f"""
                    SELECT block_num, block_hash, block_height, block_tx_count, block_size 
                    FROM headers HD 
                    WHERE HD.block_hash = X'{block_hash.hex()}' 
                    AND HD.is_orphaned = 0;"""
                self.mysql_conn.query(sql)
            result = self.mysql_conn.store_result()
            rows = result.fetch_row(0)
            if len(rows) == 0:
                return None

            if raw_header_data:
                block_num, block_hash, block_height, block_header, block_tx_count, block_size, \
                    is_orphaned = rows[0]
                block_header = block_header.hex()
            else:
                block_header = None
                block_num, block_hash, block_height, block_tx_count, block_size, is_orphaned = \
                    rows[0]

            return BlockHeaderRow(block_num, hash_to_hex_str(block_hash), block_height,
                block_header, block_tx_count, block_size, is_orphaned)
        finally:
            self.mysql_db.commit_transaction()

    def get_pushdata_filter_matches(self, pushdata_hashXes: list[str]) \
            -> Generator[RestorationFilterQueryResult, None, None]:
        try:
            query_format_pushdata_hashes = [f"X'{pd_hashX}'" for pd_hashX in pushdata_hashXes]
            sql = f"""
                SELECT PD.pushdata_hash, PD.tx_hash, PD.idx, PD.ref_type, IT.in_tx_hash, IT.in_idx, 
                    HD.block_height, HD.block_hash, HD.block_num, CT.tx_position
                FROM pushdata PD
                LEFT JOIN inputs_table IT ON PD.tx_hash=IT.out_tx_hash AND PD.idx=IT.out_idx AND PD.ref_type=0
                LEFT JOIN confirmed_transactions CT ON CT.tx_hash=PD.tx_hash
                INNER JOIN headers HD ON CT.tx_block_num = HD.block_num
                WHERE PD.pushdata_hash IN ({",".join(query_format_pushdata_hashes)}) 
                AND HD.is_orphaned = 0
                LIMIT 1000;"""
            self.mysql_conn.query(sql)
            result = self.mysql_conn.store_result()
            for row in result.fetch_row(0):
                pushdata_hashX: bytes = row[0]
                tx_hash: bytes = row[1]
                idx: int = row[2]
                ref_type: int = _get_pushdata_match_flag(row[3])
                in_tx_hash: bytes = row[4]  # Can be null
                in_idx: int = MAX_UINT32
                if row[5] is not None:
                    in_idx = row[5]
                block_height: int = row[6]
                block_hash: bytes = row[7]
                block_num: int = row[8]
                tx_position: int = row[9]
                tx_location = TxLocation(block_hash, block_num, tx_position)
                yield RestorationFilterQueryResult(
                    ref_type=ref_type,
                    pushdata_hashX=pushdata_hashX,
                    transaction_hash=tx_hash,
                    spend_transaction_hash=in_tx_hash,
                    transaction_output_index=idx,
                    spend_input_index=in_idx,
                    block_height=block_height,
                    tx_location=tx_location
                )
        finally:
            self.mysql_db.commit_transaction()
