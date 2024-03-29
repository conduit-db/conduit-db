# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.

import bitcoinx
from bitcoinx import Headers
import logging
import os
import shutil
import mmap
import sys
import threading
from pathlib import Path

from .database.lmdb.lmdb_database import LMDB_Database
from .database.db_interface.db import DBInterface
from .constants import REGTEST
from .networks import HeadersRegTestMod, NetworkConfig
from .utils import remove_readonly

MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
MMAP_SIZE = 2_000_000  # count of headers

logger = logging.getLogger("storage")


class Storage:
    """High-level Interface to database (postgres at present)"""

    def __init__(
        self,
        headers: Headers,
        block_headers: Headers,
        db: DBInterface | None,
        lmdb: LMDB_Database | None,
    ) -> None:
        self.db = db
        self.headers = headers
        self.headers_lock = threading.RLock()
        self.block_headers = block_headers
        self.block_headers_lock = threading.RLock()
        self.lmdb = lmdb

    async def close(self) -> None:
        if self.db:
            self.db.close()

    # External API

    def get_header_for_hash(self, block_hash: bytes) -> bitcoinx.Header:
        with self.headers_lock:
            header, chain = self.headers.lookup(block_hash)
            return header


def setup_headers_store(net_config: NetworkConfig, mmap_filename: str | Path) -> bitcoinx.Headers:
    bitcoinx_chain_logger = logging.getLogger("chain")
    bitcoinx_chain_logger.setLevel(logging.WARNING)

    Headers.max_cache_size = MMAP_SIZE
    HeadersRegTestMod.max_cache_size = MMAP_SIZE

    if net_config.NET == REGTEST:
        headers = HeadersRegTestMod(net_config.BITCOINX_COIN, str(mmap_filename), net_config.CHECKPOINT)
    else:
        headers = Headers(net_config.BITCOINX_COIN, str(mmap_filename), net_config.CHECKPOINT)
    return headers


def reset_headers(headers_path: Path, block_headers_path: Path) -> None:
    os.makedirs(headers_path.parent, exist_ok=True)
    os.makedirs(block_headers_path.parent, exist_ok=True)
    if sys.platform == "win32":
        if os.path.exists(headers_path):
            with open(str(headers_path), "w+") as f:
                mm = mmap.mmap(f.fileno(), MMAP_SIZE)
                mm.seek(0)
                mm.write(b"\00" * mm.size())

        # remove block headers - memory-mapped so need to do it this way to free memory immediately
        if os.path.exists(block_headers_path):
            with open(block_headers_path, "w+") as f:
                mm = mmap.mmap(f.fileno(), MMAP_SIZE)
                mm.seek(0)
                mm.write(b"\00" * mm.size())
    else:
        try:
            if os.path.exists(headers_path):
                os.remove(headers_path)
            if os.path.exists(block_headers_path):
                os.remove(block_headers_path)
        except FileNotFoundError:
            pass


def remove_contents(dir_path: Path) -> None:
    for item in dir_path.iterdir():
        if item.is_dir():
            shutil.rmtree(item, onerror=remove_readonly)
        else:
            item.unlink()


def reset_datastore(headers_path: Path, block_headers_path: Path) -> None:
    if os.environ["SERVER_TYPE"] == "ConduitIndex" and int(os.environ.get("RESET_CONDUIT_INDEX", 0)) == 1:
        db = DBInterface.load_db(worker_id="controller")
        try:
            db.drop_tables()
            if int(os.environ.get("RESET_EXTERNAL_API_DATABASE_TABLES", 0)) == 1:
                assert db.tip_filter_api is not None
                db.tip_filter_api.drop_tables()
                db.tip_filter_api.create_tables()
            db.create_permanent_tables()
        finally:
            db.close()

    DATADIR_SSD = Path(os.environ["DATADIR_SSD"])
    DATADIR_HDD = Path(os.environ["DATADIR_HDD"])
    if os.environ["SERVER_TYPE"] == "ConduitRaw" and int(os.environ.get("RESET_CONDUIT_RAW", 0)) == 1:
        if DATADIR_SSD.exists():
            remove_contents(DATADIR_SSD)

        if DATADIR_HDD.exists():
            remove_contents(DATADIR_HDD)

    os.makedirs(DATADIR_SSD, exist_ok=True)
    os.makedirs(DATADIR_HDD, exist_ok=True)
    reset_headers(headers_path, block_headers_path)


def setup_storage(net_config: NetworkConfig, headers_dir: Path) -> Storage:
    if not headers_dir.exists():
        os.makedirs(headers_dir, exist_ok=True)
    headers_dir = headers_dir
    headers_path = headers_dir.joinpath("headers.mmap")
    block_headers_path = headers_dir.joinpath("block_headers.mmap")

    if os.environ["SERVER_TYPE"] == "ConduitIndex" and int(os.environ.get("RESET_CONDUIT_INDEX", 0)) == 1:
        reset_datastore(headers_path, block_headers_path)

    if os.environ["SERVER_TYPE"] == "ConduitRaw" and int(os.environ.get("RESET_CONDUIT_RAW", 0)) == 1:
        reset_datastore(headers_path, block_headers_path)

    headers = setup_headers_store(net_config, headers_path)
    block_headers = setup_headers_store(net_config, block_headers_path)

    if os.environ["SERVER_TYPE"] == "ConduitIndex":
        db = DBInterface.load_db(worker_id="main-process")
    else:
        db = None

    if os.environ["SERVER_TYPE"] == "ConduitRaw":
        lmdb_db = LMDB_Database(lock=True, integrity_check_raw_blocks=True, worker_id="main-process")
    else:
        lmdb_db = None

    storage = Storage(headers, block_headers, db, lmdb_db)
    return storage
