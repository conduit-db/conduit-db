import asyncio
import logging
import os
import sys
from concurrent.futures.thread import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Callable, Generator, Iterator, Sequence, TypeVar

import bitcoinx
import zmq
from zmq import Socket

from ..types import PushdataRow, InputRow, OutputRow, ConfirmedTransactionRow, \
    MempoolTransactionRow, MinedTxHashes
from ...utils import get_log_level
from ...zmq_sockets import connect_non_async_zmq_socket
from ...types import BlockHeaderRow, TxMetadata, RestorationFilterQueryResult, ChainHashes

T1 = TypeVar("T1")


class RocksDbClient:

    def __init__(self, zmq_socket: Socket[bytes], worker_id: int | None = None) -> None:
        self.zmq_socket = zmq_socket
        self.worker_id = worker_id
        self.tables = RocksDbTables(zmq_socket)
        self.bulk_loads = RocksDbBulkLoads(zmq_socket)
        self.queries = RocksDbQueries(zmq_socket, self.tables, self.bulk_loads)
        self.api_queries = RocksDbAPIQueries(zmq_socket, self.tables, self)

        self.start_transaction()
        # self.bulk_loads.set_rocks_db_bulk_load_off()
        self.set_myrocks_settings()
        self.commit_transaction()

        self.executor = ThreadPoolExecutor(max_workers=1)
        self.logger = logging.getLogger("mysql-database")
        self.logger.setLevel(get_log_level('conduit_index'))

    def set_myrocks_settings(self) -> None:
        pass

    async def run_in_executor(self, func: Callable[..., T1], *args: Any) -> T1:
        # Note: One must be very careful with combining remote logging over TCP and spinning up
        # Executor functions that initialise a new logger instance... it is very costly and slow.
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self.executor, func, *args)

    def close(self) -> None:
        pass

    def start_transaction(self) -> None:
        pass

    def commit_transaction(self) -> None:
        pass

    def rollback_transaction(self) -> None:
        pass

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

    def rocksdb_load_temp_inbound_tx_hashes(self, inbound_tx_hashes: list[tuple[str]],
            inbound_tx_table_name: str) -> None:
        self.queries.rocksdb_load_temp_inbound_tx_hashes(inbound_tx_hashes, inbound_tx_table_name)

    def rocksdb_get_unprocessed_txs(self, is_reorg: bool, new_tx_hashes: list[tuple[str]],
            inbound_tx_table_name: str) -> set[bytes]:
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


def get_connection(context: zmq.Context[zmq.Socket[bytes]]) -> Socket[bytes]:
    host = os.environ.get('ROCKSDB_HOST', '127.0.0.1')
    port = int(os.environ.get('ROCKSDB_PORT', 49977))  # not 3306 because of docker issues
    uri = f"tcp://{host}:{port}"
    return connect_non_async_zmq_socket(context, uri, zmq.SocketType.REQ)


def rocksdb_connect(context: zmq.Context[zmq.Socket[bytes]], worker_id: int | None = None) \
        -> RocksDbClient:
    zmq_socket = get_connection(context)
    return RocksDbClient(zmq_socket, worker_id=worker_id)


load_rocksdb_database = rocksdb_connect


class RocksDbTables:

    def __init__(self, zmq_socket: zmq.Socket[bytes]) -> None:
        self.logger = logging.getLogger("mysql-tables")
        self.zmq_socket = zmq_socket

    def start_transaction(self) -> None:
        pass

    def commit_transaction(self) -> None:
        pass

    def get_tables(self) -> Sequence[tuple[str]]:
        pass

    def rocksdb_drop_tables(self) -> None:
        pass

    def rocksdb_drop_indices(self) -> None:
        pass

    def rocksdb_drop_mempool_table(self) -> None:
        pass

    def rocksdb_drop_temp_mined_tx_hashes(self) -> None:
        pass

    def rocksdb_drop_temp_inbound_tx_hashes(self, inbound_tx_table_name: str) -> None:
        pass

    def rocksdb_drop_temp_mempool_removals(self) -> None:
        pass

    def rocksdb_drop_temp_mempool_additions(self) -> None:
        pass

    def rocksdb_drop_temp_orphaned_txs(self) -> None:
        pass

    def rocksdb_drop_rocksdb_unsafe_txs(self) -> None:
        pass

    def rocksdb_create_mempool_table(self) -> None:
        pass

    # Todo - make all offsets BINARY(5) and tx_position BINARY(5) because this gives enough capacity
    #  for 1 TB block sizes.
    def rocksdb_create_permanent_tables(self) -> None:
        pass

    def rocksdb_create_temp_mined_tx_hashes_table(self) -> None:
        pass

    def rocksdb_create_temp_mempool_removals_table(self) -> None:
        pass

    def rocksdb_create_temp_mempool_additions_table(self) -> None:
        pass

    def rocksdb_create_temp_orphaned_txs_table(self) -> None:
        pass

    def rocksdb_create_temp_inbound_tx_hashes_table(self, inbound_tx_table_name: str) -> None:
        pass

    def initialise_checkpoint_state(self) -> None:
        pass



class RocksDbAPIQueries:

    def __init__(self, zmq_socket: zmq.Socket[bytes], rocksdb_tables: RocksDbTables,
            rocksdb_db: 'RocksDbClient') -> None:
        self.logger = logging.getLogger("rocksdb-queries")
        self.logger.setLevel(logging.DEBUG)
        self.zmq_socket = zmq_socket
        self.rocksdb_tables = rocksdb_tables
        self.rocksdb_db = rocksdb_db

    def get_transaction_metadata_hashX(self, tx_hashX: bytes) -> TxMetadata | None:
        pass

    def get_header_data(self, block_hash: bytes, raw_header_data: bool=True) \
            -> BlockHeaderRow | None:
        pass

    def get_pushdata_filter_matches(self, pushdata_hashXes: list[str]) \
            -> Generator[RestorationFilterQueryResult, None, None]:
        pass


class RocksDbBulkLoads:

    def __init__(self, zmq_socket: zmq.Socket[bytes]) -> None:
        self.zmq_socket = zmq_socket
        self.worker_id = self.rocksdb_db.worker_id
        if self.worker_id:
            self.logger = logging.getLogger(f"rocksdb-tables-{self.worker_id}")
        else:
            self.logger = logging.getLogger(f"rocksdb-tables")

        self.logger.setLevel(get_log_level('conduit_index'))
        self.total_db_time = 0.
        self.total_rows_flushed_since_startup = 0  # for current controller
        self.newline_symbol = r"'\r\n'" if sys.platform == 'win32' else r"'\n'"
        self.TEMP_FILES_DIR = Path(os.environ["DATADIR_SSD"]) / 'temp_files'

    def set_local_infile_on(self) -> None:
        pass

    def _load_data_infile_batched(self, table_name: str, string_rows: list[str],
            column_names: list[str], binary_column_indices: list[int]) \
                -> None:
        pass

    def _load_data_infile(self, table_name: str, string_rows: list[str],
            column_names: list[str], binary_column_indices: list[int], have_retried: bool=False) \
                -> None:
        pass

    def handle_coinbase_dup_tx_hash(self, tx_rows: list[ConfirmedTransactionRow]) -> None:
        pass

    def rocksdb_bulk_load_confirmed_tx_rows(self, tx_rows: list[ConfirmedTransactionRow]) -> None:
        pass

    def rocksdb_bulk_load_mempool_tx_rows(self, tx_rows: list[MempoolTransactionRow]) -> None:
        pass

    def rocksdb_bulk_load_output_rows(self, out_rows: list[OutputRow]) -> None:
        pass

    def rocksdb_bulk_load_input_rows(self, in_rows: list[InputRow]) -> None:
        pass

    def rocksdb_bulk_load_pushdata_rows(self, pd_rows: list[PushdataRow]) -> None:
        pass

    def rocksdb_bulk_load_temp_unsafe_txs(self, unsafe_tx_rows: list[str]) -> None:
        pass

    def rocksdb_bulk_load_headers(self, block_header_rows: list[BlockHeaderRow]) -> None:
        """block_num, block_hash, block_height, block_header"""
        pass


class RocksDbQueries:

    def __init__(self, zmq_socket: zmq.Socket[bytes], rocksdb_tables: RocksDbTables, bulk_loads:
            RocksDbBulkLoads) -> None:
        self.logger = logging.getLogger("mysql-queries")
        self.logger.setLevel(logging.DEBUG)
        self.zmq_socket = zmq_socket
        self.rocksdb_tables = rocksdb_tables
        self.bulk_loads = bulk_loads

    def rocksdb_load_temp_mined_tx_hashes(self, mined_tx_hashes: list[MinedTxHashes]) -> None:
        pass

    def rocksdb_load_temp_inbound_tx_hashes(self, inbound_tx_hashes: list[tuple[str]],
            inbound_tx_table_name: str) -> None:
        pass

    def rocksdb_get_unprocessed_txs(self, is_reorg: bool, new_tx_hashes: list[tuple[str]],
            inbound_tx_table_name: str) -> set[bytes]:
        pass

    def rocksdb_invalidate_mempool_rows(self) -> None:
        pass

    def rocksdb_load_temp_mempool_removals(self, removals_from_mempool: set[bytes]) -> None:
        pass

    def rocksdb_load_temp_mempool_additions(self, additions_to_mempool: set[bytes]) -> None:
        pass

    def rocksdb_load_temp_orphaned_tx_hashes(self, orphaned_tx_hashes: set[bytes]) -> None:
        pass

    def rocksdb_remove_from_mempool(self) -> None:
        pass

    def rocksdb_add_to_mempool(self) -> None:
        pass

    def rocksdb_update_checkpoint_tip(self, checkpoint_tip: bitcoinx.Header) -> None:
        pass

    def rocksdb_get_checkpoint_state(self) -> tuple[int, bytes, bool, bytes, bytes, bytes,
            bytes] | None:
        """We 'allocate' work up to a given block hash and then we set the new checkpoint only
        after every block in the batch is successfully flushed"""
        pass
    def rocksdb_get_txids_above_last_good_block_num(self, last_good_block_num: int) -> Iterator[Any]:
        """This query will do a full table scan and so will not scale - instead would need to
        pull txids by blockhash from merkleproof to get the set of txids..."""
        assert isinstance(last_good_block_num, int)
        pass

    def rocksdb_delete_transaction_rows(self, tx_hash_hexes: list[str]) -> None:
        pass

    def rocksdb_get_duplicate_tx_hashes(self, tx_rows: list[ConfirmedTransactionRow]) \
            -> list[ConfirmedTransactionRow]:
        pass

    def rocksdb_update_orphaned_headers(self, block_hashes: list[bytes]) -> None:
        pass

    def update_allocated_state(self, reorg_was_allocated: bool, first_allocated: bitcoinx.Header,
            last_allocated: bitcoinx.Header, old_hashes: ChainHashes | None,
            new_hashes: ChainHashes | None) -> None:
        pass

    def get_mempool_size(self) -> int:
        pass
