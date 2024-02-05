# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.
from dataclasses import dataclass
import logging
import os
from pathlib import Path

from conduit_p2p import HeadersStore

from .database.lmdb.lmdb_database import LMDB_Database
from .database.db_interface.db import DBInterface

MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
MMAP_SIZE = 2_000_000  # count of headers

logger = logging.getLogger("storage")


@dataclass
class Storage:
    """High-level Interface to database (postgres at present)"""

    db: DBInterface | None
    headers: HeadersStore
    block_headers: HeadersStore
    lmdb: LMDB_Database | None

    async def close(self) -> None:
        if self.db:
            self.db.close()


def setup_storage(network_type: str, headers_dir: Path) -> Storage:
    if not headers_dir.exists():
        os.makedirs(headers_dir, exist_ok=True)
    headers_dir = headers_dir
    headers_path = headers_dir.joinpath("headers")
    block_headers_path = headers_dir.joinpath("block_headers")

    headers = HeadersStore(str(headers_path), network_type)
    block_headers = HeadersStore(str(block_headers_path), network_type)

    if os.environ["SERVER_TYPE"] == "ConduitIndex":
        db = DBInterface.load_db(worker_id="main-process")
    else:
        db = None

    if os.environ["SERVER_TYPE"] == "ConduitRaw":
        lmdb_db = LMDB_Database(lock=True, integrity_check_raw_blocks=True, worker_id="main-process")
    else:
        lmdb_db = None

    storage = Storage(headers=headers, block_headers=block_headers, db=db, lmdb=lmdb_db)
    return storage
