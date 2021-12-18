import logging
import os
import shutil
import stat
import mmap
import sys
import threading
import time
from pathlib import Path
from typing import Optional, Dict

import bitcoinx
from bitcoinx import Headers

from .database.lmdb.lmdb_database import LMDB_Database
from .database.mysql.mysql_database import MySQLDatabase, load_mysql_database, mysql_connect
from .constants import REGTEST
from .networks import HeadersRegTestMod

MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
MMAP_SIZE = 2_000_000  # count of headers

logger = logging.getLogger("storage")


class Storage:
    """High-level Interface to database (postgres at present)"""

    def __init__(self, headers: Headers, block_headers: Headers,
            mysql_database: Optional[MySQLDatabase], lmdb: Optional[LMDB_Database], ):
        self.mysql_database = mysql_database
        self.headers = headers
        self.headers_lock = threading.RLock()
        self.block_headers = block_headers
        self.block_headers_lock = threading.RLock()
        self.lmdb = lmdb

    async def close(self):
        if self.mysql_database:
            self.mysql_database.close()

    # External API

    def get_header_for_hash(self, block_hash: bytes) -> bitcoinx.Header:
        with self.headers_lock:
            header, chain = self.headers.lookup(block_hash)
            return header


def setup_headers_store(net_config, mmap_filename):
    bitcoinx_chain_logger = logging.getLogger('chain')
    bitcoinx_chain_logger.setLevel(logging.WARNING)

    Headers.max_cache_size = MMAP_SIZE
    HeadersRegTestMod.max_cache_size = MMAP_SIZE

    if net_config.NET == REGTEST:
        headers = HeadersRegTestMod(net_config.BITCOINX_COIN, mmap_filename,
            net_config.CHECKPOINT)
    else:
        headers = Headers(net_config.BITCOINX_COIN, mmap_filename, net_config.CHECKPOINT)
    return headers


def reset_headers(headers_path: Path, block_headers_path: Path):
    if sys.platform == 'win32':
        if os.path.exists(headers_path):
            with open(headers_path, 'w+') as f:
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


def reset_datastore(headers_path: Path, block_headers_path: Path, config: Dict):
    # remove headers - memory-mapped so need to do it this way to free memory immediately...

    reset_headers(headers_path, block_headers_path)

    # remove postgres tables
    if config['server_type'] == "ConduitIndex":
        mysql_database = mysql_connect()
        try:
            mysql_database.mysql_drop_tables()
        finally:
            mysql_database.close()

    if config['server_type'] == "ConduitRaw":
        def remove_readonly(func, path, excinfo):
            lmdb_db = LMDB_Database()
            lmdb_db.close()
            os.chmod(path, stat.S_IWRITE)
            func(path)

        lmdb_path = Path(MODULE_DIR).parent.parent.parent.joinpath('lmdb_data')
        if os.path.exists(lmdb_path):
            shutil.rmtree(lmdb_path, onerror=remove_readonly)

        LMDB_DATABASE_PATH_DEFAULT = Path(MODULE_DIR).parent.parent / 'lmdb_data'
        LMDB_DATABASE_PATH: str = os.environ.get("LMDB_DATABASE_PATH",
            str(LMDB_DATABASE_PATH_DEFAULT))
        if os.path.exists(LMDB_DATABASE_PATH):
            shutil.rmtree(LMDB_DATABASE_PATH, onerror=remove_readonly)

        RAW_BLOCKS_DIR_DEFAULT = Path(MODULE_DIR).parent / 'raw_blocks'
        RAW_BLOCKS_DIR = os.environ.get("RAW_BLOCKS_DIR", str(RAW_BLOCKS_DIR_DEFAULT))
        if os.path.exists(RAW_BLOCKS_DIR):
            shutil.rmtree(RAW_BLOCKS_DIR, onerror=remove_readonly)

        MERKLE_TREES_DIR_DEFAULT = Path(MODULE_DIR).parent / 'merkle_trees'
        MERKLE_TREES_DIR = os.environ.get("MERKLE_TREES_DIR", str(MERKLE_TREES_DIR_DEFAULT))
        if os.path.exists(MERKLE_TREES_DIR):
            shutil.rmtree(MERKLE_TREES_DIR, onerror=remove_readonly)

        TX_OFFSETS_DIR_DEFAULT = Path(MODULE_DIR).parent / 'tx_offsets'
        TX_OFFSETS_DIR = os.environ.get("TX_OFFSETS_DIR", str(TX_OFFSETS_DIR_DEFAULT))
        if os.path.exists(TX_OFFSETS_DIR):
            shutil.rmtree(TX_OFFSETS_DIR, onerror=remove_readonly)


def setup_storage(config, net_config, headers_dir: Optional[Path] = None) -> Storage:
    if not headers_dir:
        headers_dir = MODULE_DIR.parent
        headers_path = headers_dir.joinpath("headers.mmap")
        block_headers_path = headers_dir.joinpath("block_headers.mmap")
    else:
        headers_dir = headers_dir
        headers_path = headers_dir.joinpath("headers.mmap")
        block_headers_path = headers_dir.joinpath("block_headers.mmap")

    if config.get('reset'):
        reset_datastore(headers_path, block_headers_path, config)

    headers = setup_headers_store(net_config, headers_path)
    block_headers = setup_headers_store(net_config, block_headers_path)

    if config['server_type'] == "ConduitIndex":
        mysql_database = load_mysql_database()
    else:
        mysql_database = None

    if config['server_type'] == "ConduitRaw":  # comment out until we have gRPC wrapper
        lmdb_db = LMDB_Database()
    else:
        lmdb_db = None

    storage = Storage(headers, block_headers, mysql_database, lmdb_db)
    return storage
