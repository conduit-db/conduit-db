import logging
import os
import shutil
import stat
import mmap
import sys
import threading
from pathlib import Path
from typing import Callable

import bitcoinx
from bitcoinx import Headers

from .database.lmdb.lmdb_database import LMDB_Database
from .database.mysql.mysql_database import MySQLDatabase, load_mysql_database, mysql_connect
from .constants import REGTEST
from .networks import HeadersRegTestMod, NetworkConfig

MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
MMAP_SIZE = 2_000_000  # count of headers

logger = logging.getLogger("storage")


class Storage:
    """High-level Interface to database (postgres at present)"""

    def __init__(self, headers: Headers, block_headers: Headers,
            mysql_database: MySQLDatabase | None, lmdb: LMDB_Database | None) -> None:
        self.mysql_database = mysql_database
        self.headers = headers
        self.headers_lock = threading.RLock()
        self.block_headers = block_headers
        self.block_headers_lock = threading.RLock()
        self.lmdb = lmdb

    async def close(self) -> None:
        if self.mysql_database:
            self.mysql_database.close()

    # External API

    def get_header_for_hash(self, block_hash: bytes) -> bitcoinx.Header:
        with self.headers_lock:
            header, chain = self.headers.lookup(block_hash)
            return header


def setup_headers_store(net_config: NetworkConfig, mmap_filename: str | Path) -> bitcoinx.Headers:
    bitcoinx_chain_logger = logging.getLogger('chain')
    bitcoinx_chain_logger.setLevel(logging.WARNING)

    Headers.max_cache_size = MMAP_SIZE
    HeadersRegTestMod.max_cache_size = MMAP_SIZE

    if net_config.NET == REGTEST:
        headers = HeadersRegTestMod(net_config.BITCOINX_COIN, str(mmap_filename),
            net_config.CHECKPOINT)
    else:
        headers = Headers(net_config.BITCOINX_COIN, str(mmap_filename), net_config.CHECKPOINT)
    return headers


def reset_headers(headers_path: Path, block_headers_path: Path) -> None:
    if sys.platform == 'win32':
        if os.path.exists(headers_path):
            with open(str(headers_path), 'w+') as f:
                mm = mmap.mmap(f.fileno(), MMAP_SIZE)
                mm.seek(0)
                mm.write(b'\00' * mm.size())

        # remove block headers - memory-mapped so need to do it this way to free memory immediately
        if os.path.exists(block_headers_path):
            with open(block_headers_path, 'w+') as f:
                mm = mmap.mmap(f.fileno(), MMAP_SIZE)
                mm.seek(0)
                mm.write(b'\00' * mm.size())
    else:
        try:
            if os.path.exists(headers_path):
                os.remove(headers_path)
            if os.path.exists(block_headers_path):
                os.remove(block_headers_path)
        except FileNotFoundError:
            pass


def reset_datastore(headers_path: Path, block_headers_path: Path) -> None:
    # remove headers - memory-mapped so need to do it this way to free memory immediately...

    reset_headers(headers_path, block_headers_path)

    # remove postgres tables
    if os.environ['SERVER_TYPE'] == "ConduitIndex":
        mysql_database = mysql_connect()
        try:
            mysql_database.tables.mysql_drop_indices()
            mysql_database.mysql_drop_tables()
        finally:
            mysql_database.close()

    if os.environ['SERVER_TYPE'] == "ConduitRaw":
        def remove_readonly(func: Callable[[Path], None], path: Path,
                excinfo: BaseException | None) -> None:
            lmdb_db = LMDB_Database()
            lmdb_db.close()
            os.chmod(path, stat.S_IWRITE)
            func(path)

        lmdb_path = Path(MODULE_DIR).parent.parent.parent.joinpath('lmdb_data')
        if os.path.exists(lmdb_path):
            shutil.rmtree(lmdb_path, onerror=remove_readonly)

        LMDB_DATABASE_DIR: str = os.environ["LMDB_DATABASE_DIR"]
        if os.path.exists(LMDB_DATABASE_DIR):
            shutil.rmtree(LMDB_DATABASE_DIR, onerror=remove_readonly)

        RAW_BLOCKS_DIR = os.environ["RAW_BLOCKS_DIR"]
        if os.path.exists(RAW_BLOCKS_DIR):
            shutil.rmtree(RAW_BLOCKS_DIR, onerror=remove_readonly)

        MERKLE_TREES_DIR = os.environ["MERKLE_TREES_DIR"]
        if os.path.exists(MERKLE_TREES_DIR):
            shutil.rmtree(MERKLE_TREES_DIR, onerror=remove_readonly)

        TX_OFFSETS_DIR = os.environ["TX_OFFSETS_DIR"]
        if os.path.exists(TX_OFFSETS_DIR):
            shutil.rmtree(TX_OFFSETS_DIR, onerror=remove_readonly)


def setup_storage(net_config: NetworkConfig, headers_dir: Path) -> Storage:
    if not headers_dir.exists():
        os.makedirs(headers_dir, exist_ok=True)
    headers_dir = headers_dir
    headers_path = headers_dir.joinpath("headers.mmap")
    block_headers_path = headers_dir.joinpath("block_headers.mmap")

    if int(os.environ.get('RESET_CONDUIT_RAW', 0)) == 1 or \
            int(os.environ.get('RESET_CONDUIT_INDEX', 0)) == 1:
        reset_datastore(headers_path, block_headers_path)

    headers = setup_headers_store(net_config, headers_path)
    block_headers = setup_headers_store(net_config, block_headers_path)

    if os.environ['SERVER_TYPE'] == "ConduitIndex":
        mysql_database = load_mysql_database()
    else:
        mysql_database = None

    if os.environ['SERVER_TYPE'] == "ConduitRaw":
        lmdb_db = LMDB_Database(lock=True)
    else:
        lmdb_db = None

    storage = Storage(headers, block_headers, mysql_database, lmdb_db)
    return storage
