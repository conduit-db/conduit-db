import logging
import os
import time
import uuid
from pathlib import Path
from typing import List, Tuple

from MySQLdb import _mysql
from bitcoinx import hash_to_hex_str

from ...constants import PROFILING


class MySQLBulkLoads:

    def __init__(self, mysql_conn: _mysql.connection):
        self.logger = logging.getLogger("mysql-tables")
        self.mysql_conn = mysql_conn
        self.logger.setLevel(PROFILING)

    def _load_data_infile(self, table_name: str, string_rows: List[str],
            column_names: List[str], binary_column_indices: List[int]):
        outfile = Path(str(uuid.uuid4()) + ".csv")
        try:
            with open(outfile, 'w') as csvfile:
                csvfile.writelines(string_rows)

            set_params = []
            for index, column_name in enumerate(column_names):
                if index in binary_column_indices:
                    column_names[index] = "@var" + str(index)
                    set_params.append(column_name + f" = UNHEX({column_names[index]})")
            set_statement = f"SET " + ",".join(set_params)

            query = f"""
                LOAD DATA INFILE '{outfile.absolute().as_posix()}' 
                INTO TABLE {table_name}
                FIELDS TERMINATED BY ','
                LINES TERMINATED BY """ + r"'\r\n'" + "\n" + \
                f"""({", ".join(column_names)})"""
            if len(binary_column_indices) != 0:
                query += f"\n{set_statement}"

            query += ";"
            self.mysql_conn.query(query)
        finally:
            if os.path.exists(outfile):
                os.remove(outfile)

    async def mysql_bulk_load_confirmed_tx_rows(self, tx_rows):
        t0 = time.time()
        outfile = Path(str(uuid.uuid4()) + ".csv")
        try:
            string_rows = ["%s,%s,%s,%s,%s,%s,%s\n" % (row) for row in tx_rows]
            column_names = ['tx_shash', 'tx_hash', 'tx_height', 'tx_position', 'tx_offset_start',
                'tx_offset_end', 'tx_has_collided']
            self._load_data_infile("confirmed_transactions", string_rows, column_names,
                binary_column_indices=[1])
        finally:
            if os.path.exists(outfile):
                os.remove(outfile)
        t1 = time.time() - t0
        self.logger.log(PROFILING,
            f"elapsed time for mysql_bulk_load_confirmed_tx_rows = {t1} seconds for {len(tx_rows)}"
        )

    async def mysql_bulk_load_mempool_tx_rows(self, tx_rows):
        t0 = time.time()
        outfile = Path(str(uuid.uuid4()) + ".csv")
        try:
            string_rows = ["%s,%s,%s,%s,%s\n" % (row) for row in tx_rows]
            column_names = ['mp_tx_shash', 'mp_tx_hash', 'mp_tx_timestamp', 'mp_tx_has_collided',
                'mp_rawtx']
            self._load_data_infile("mempool_transactions", string_rows, column_names,
                binary_column_indices=[1, 4])
        finally:
            if os.path.exists(outfile):
                os.remove(outfile)
        t1 = time.time() - t0

        self.logger.log(PROFILING,
            f"elapsed time for mysql_bulk_load_mempool_tx_rows = {t1} seconds for {len(tx_rows)}"
        )

    async def mysql_bulk_load_output_rows(self, out_rows):
        t0 = time.time()
        outfile = Path(str(uuid.uuid4()) + ".csv")
        try:
            string_rows = ["%s,%s,%s,%s,%s,%s,%s\n" % (row) for row in out_rows]
            column_names = ['out_tx_shash', 'out_idx', 'out_value', 'out_has_collided', 'in_tx_shash',
                'in_idx', 'in_has_collided']
            self._load_data_infile("io_table", string_rows, column_names,
                binary_column_indices=[])
        finally:
            if os.path.exists(outfile):
                os.remove(outfile)
        t1 = time.time() - t0

        self.logger.log(PROFILING,
            f"elapsed time for mysql_bulk_load_output_rows = {t1} seconds for {len(out_rows)}"
        )

    async def mysql_bulk_load_input_rows(self, in_rows):
        # Todo - check for collisions in TxParser then:
        #  1) bulk copy to temp table
        #  2) update io table from temp table (no table joins needed)
        t0 = time.time()
        outfile = Path(str(uuid.uuid4()) + ".csv")
        try:
            string_rows = ["%s,%s,%s,%s,%s\n" % (row) for row in in_rows]
            column_names = ['in_prevout_shash', 'out_idx', 'in_tx_shash', 'in_idx',
                'in_has_collided']
            self._load_data_infile("temp_inputs", string_rows, column_names,
                binary_column_indices=[])
        finally:
            if os.path.exists(outfile):
                os.remove(outfile)

        query = """
        UPDATE io_table
            INNER JOIN temp_inputs
            ON temp_inputs.in_prevout_shash = io_table.out_tx_shash 
                AND temp_inputs.out_idx = io_table.out_idx
        SET io_table.in_tx_shash = temp_inputs.in_tx_shash,
            io_table.in_idx = temp_inputs.in_idx,
            io_table.in_has_collided = temp_inputs.in_has_collided
        WHERE temp_inputs.in_prevout_shash = io_table.out_tx_shash
            AND temp_inputs.out_idx = io_table.out_idx;"""
        self.mysql_conn.query(query)

        t1 = time.time() - t0
        self.logger.log(PROFILING,
            f"elapsed time for mysql_bulk_load_input_rows = {t1} seconds for {len(in_rows)}"
        )

    async def mysql_bulk_load_pushdata_rows(self, pd_rows):
        t0 = time.time()
        outfile = Path(str(uuid.uuid4()) + ".csv")
        try:
            string_rows = ["%s,%s,%s,%s,%s,%s\n" % (row) for row in pd_rows]
            column_names = ['pushdata_shash', 'pushdata_hash', 'tx_shash', 'idx',
                'ref_type', 'pd_tx_has_collided']
            self._load_data_infile("pushdata", string_rows, column_names,
                binary_column_indices=[1])
        finally:
            if os.path.exists(outfile):
                os.remove(outfile)
        t1 = time.time() - t0
        self.logger.log(PROFILING,
            f"elapsed time for mysql_bulk_load_pushdata_rows = {t1} seconds for {len(pd_rows)}"
        )

