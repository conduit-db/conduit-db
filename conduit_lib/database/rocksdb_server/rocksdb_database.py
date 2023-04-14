import asyncio
from pathlib import Path

import bitcoinx
import logging
import os
from concurrent.futures.thread import ThreadPoolExecutor
from typing import Any, Callable, TypeVar

from rocksdict import Rdict

from .rocksdb_api_queries import RocksDbAPIQueries
from .rocksdb_bulk_loads import RocksDbBulkLoads
from .rocksdb_queries import RocksDbQueries
from .rocksdb_tables import RocksDbTables
from conduit_lib.database.types import PushdataRow, InputRow, OutputRow, ConfirmedTransactionRow, MempoolTransactionRow, \
    MinedTxHashes
from ...utils import get_log_level


T1 = TypeVar("T1")


class RocksDbServer:

    def __init__(self) -> None:
        MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
        database_dir = os.getenv('ROCKSDB_DIR', str(MODULE_DIR / "rocksdb_data"))
        self.db = Rdict(database_dir)
        self.tables = RocksDbTables(self.db)
        self.bulk_loads = RocksDbBulkLoads(self.db, self)
        self.queries = RocksDbQueries(self.db, self.tables, self.bulk_loads, self)
        self.api_queries = RocksDbAPIQueries(self.db, self.tables, self)

        self.start_transaction()
        self.set_myrocks_settings()
        self.commit_transaction()

        self.executor = ThreadPoolExecutor(max_workers=1)
        self.logger = logging.getLogger("rocksdb-database")
        self.logger.setLevel(get_log_level('conduit_index'))

    def set_myrocks_settings(self) -> None:
        # SET global rocksdb_max_subcompactions=8
        settings = f"""SET session rocksdb_max_row_locks={100_000_000};
            SET global rocksdb_write_disable_wal=0;
            SET global rocksdb_wal_recovery_mode=0;
            SET global rocksdb_max_background_jobs=16;
            SET global rocksdb_block_cache_size={1024 ** 3 * 8};"""
        for sql in settings.splitlines(keepends=False):
            self.db.query(sql)

    async def run_in_executor(self, func: Callable[..., T1], *args: Any) -> T1:
        # Note: One must be very careful with combining remote logging over TCP and spinning up
        # Executor functions that initialise a new logger instance... it is very costly and slow.
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self.executor, func, *args)

    def close(self) -> None:
        self.db.close()

    def start_transaction(self) -> None:
        self.db.query(
            """START TRANSACTION;"""
        )

    def commit_transaction(self) -> None:
        self.db.query(
            """COMMIT;"""
        )

    def rollback_transaction(self) -> None:
        self.db.query(
            """ROLLBACK;"""
        )

    # TABLES
    def rocksdb_drop_tables(self) -> None:
        self.tables.rocksdb_drop_tables()

    def rocksdb_drop_temp_mined_tx_hashes(self) -> None:
        self.tables.rocksdb_drop_temp_mined_tx_hashes()

    def rocksdb_drop_temp_inbound_tx_hashes(self, inbound_tx_table_name: str) -> None:
        self.tables.rocksdb_drop_temp_inbound_tx_hashes(inbound_tx_table_name)

    def rocksdb_create_temp_mined_tx_hashes_table(self) -> None:
        self.tables.rocksdb_create_temp_mined_tx_hashes_table()

    def rocksdb_create_temp_inbound_tx_hashes_table(self, inbound_tx_table_name: str) -> None:
        self.tables.rocksdb_create_temp_inbound_tx_hashes_table(inbound_tx_table_name)

    # QUERIES
    def rocksdb_load_temp_mined_tx_hashes(self, mined_tx_hashes: list[MinedTxHashes]) -> None:
        self.queries.rocksdb_load_temp_mined_tx_hashes(mined_tx_hashes)

    def rocksdb_load_temp_inbound_tx_hashes(self,
            inbound_tx_hashes: list[tuple[str]], inbound_tx_table_name: str) -> None:
        self.queries.rocksdb_load_temp_inbound_tx_hashes(inbound_tx_hashes, inbound_tx_table_name)

    def rocksdb_get_unprocessed_txs(self, is_reorg: bool,
            new_tx_hashes: list[tuple[str]], inbound_tx_table_name: str) -> set[bytes]:
        return self.queries.rocksdb_get_unprocessed_txs(is_reorg, new_tx_hashes,
            inbound_tx_table_name)

    def rocksdb_invalidate_mempool_rows(self) -> None:
        self.queries.rocksdb_invalidate_mempool_rows()

    def rocksdb_update_checkpoint_tip(self, checkpoint_tip: bitcoinx.Header) -> None:
        self.queries.rocksdb_update_checkpoint_tip(checkpoint_tip)

    # BULK LOADS
    def rocksdb_bulk_load_confirmed_tx_rows(self, tx_rows: list[ConfirmedTransactionRow]) -> None:
        self.bulk_loads.rocksdb_bulk_load_confirmed_tx_rows(tx_rows)

    def rocksdb_bulk_load_mempool_tx_rows(self, tx_rows: list[MempoolTransactionRow]) -> None:
        self.bulk_loads.rocksdb_bulk_load_mempool_tx_rows(tx_rows)

    def rocksdb_bulk_load_output_rows(self, out_rows: list[OutputRow]) -> None:
        self.bulk_loads.rocksdb_bulk_load_output_rows(out_rows)

    def rocksdb_bulk_load_input_rows(self, in_rows: list[InputRow]) -> None:
        self.bulk_loads.rocksdb_bulk_load_input_rows(in_rows)

    def rocksdb_bulk_load_pushdata_rows(self, pd_rows: list[PushdataRow]) -> None:
        self.bulk_loads.rocksdb_bulk_load_pushdata_rows(pd_rows)
