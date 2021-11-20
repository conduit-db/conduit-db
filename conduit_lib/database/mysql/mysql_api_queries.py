import logging
from typing import Generator

import MySQLdb
import bitcoinx
from MySQLdb import _mysql
from bitcoinx import hash_to_hex_str, hex_str_to_hash

from .mysql_tables import MySQLTables
from ...constants import PROFILING, HashXLength, MAX_UINT32
from ...types import TransactionQueryResult, RestorationFilterRequest, \
    RestorationFilterJSONResponse, le_int_to_char, RestorationFilterResult, TxLocation, \
    RestorationFilterQueryResult, _get_pushdata_match_flag


class MySQLAPIQueries:

    def __init__(self, mysql_conn: _mysql.connection, mysql_tables: MySQLTables, mysql_db):
        self.logger = logging.getLogger("mysql-queries")
        self.logger.setLevel(logging.DEBUG)
        self.mysql_conn = mysql_conn
        self.mysql_tables = mysql_tables
        self.mysql_db = mysql_db

    def get_transaction_metadata(self, tx_hash: bytes) -> TransactionQueryResult:
        self.mysql_conn.query(f"""
            SELECT CT.tx_hash, CT.tx_block_num, CT.tx_position, HD.block_num, HD.block_hash, 
                HD.block_height FROM confirmed_transactions CT
            INNER JOIN headers HD
            ON HD.block_num = CT.tx_block_num
            WHERE CT.tx_hash = X'{tx_hash.hex()}';""")
        result = self.mysql_conn.store_result()
        if result:
            row = result.fetch_row(0)[0]
            tx_hash, tx_block_num, tx_position, block_num, block_hash, block_height = row
            return TransactionQueryResult(tx_hash, tx_block_num, tx_position, block_num, block_hash,
                block_height)

    def _make_restoration_query_result(self, row):
        pushdata_hashX = row[0]
        tx_hash = row[1]
        idx = row[2]
        ref_type = _get_pushdata_match_flag(row[3])
        in_tx_hash = row[4]  # Can be null
        in_idx = MAX_UINT32
        if row[5]:
            in_idx = row[5]
        block_height = row[6]
        block_hash = row[7]
        block_num = row[8]
        tx_position = row[9]
        tx_location = TxLocation(block_hash, block_num, tx_position)
        return RestorationFilterQueryResult(
            ref_type=ref_type,
            pushdata_hashX=pushdata_hashX,
            transaction_hash=tx_hash,
            spend_transaction_hash=in_tx_hash,
            transaction_output_index=idx,
            spend_input_index=in_idx,
            block_height=block_height,
            tx_location=tx_location
        )

    def get_pushdata_filter_matches(self, pushdata_hashXes: list[str]) \
            -> Generator[RestorationFilterJSONResponse, None, None]:
        query_format_pushdata_hashes = [f"X'{pd_hashX}'" for pd_hashX in pushdata_hashXes]
        sql = f"""
            SELECT PD.pushdata_hash, PD.tx_hash, PD.idx, PD.ref_type, IT.in_tx_hash, IT.in_idx, 
                HD.block_height, HD.block_hash, HD.block_num, CT.tx_position
            FROM pushdata PD
            LEFT JOIN inputs_table IT ON PD.tx_hash=IT.out_tx_hash AND PD.idx=IT.out_idx AND PD.ref_type=0
            LEFT JOIN confirmed_transactions CT ON CT.tx_hash=PD.tx_hash
            INNER JOIN headers HD ON CT.tx_block_num = HD.block_num
            WHERE PD.pushdata_hash IN ({",".join(query_format_pushdata_hashes)})"""
        # self.logger.debug(f"sql: {sql}")
        self.mysql_conn.query(sql)
        result = self.mysql_conn.store_result()

        if result:
            for row in result.fetch_row(0):
                yield self._make_restoration_query_result(row)
