import logging
import os
import shutil
import stat
import time
import mmap
from pathlib import Path

from bitcoinx import Headers

from .database.postgres_database import load_pg_database, PG_Database, pg_connect
from .constants import REGTEST
from .networks import HeadersRegTestMod


MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
MMAP_SIZE = 2_000_000


class Storage:
    """High-level Interface to database (postgres at present)"""
    def __init__(
        self,
        headers: Headers,
        block_headers: Headers,
        pg_database: PG_Database,
        redis=None,
    ):
        self.pg_database = pg_database
        self.logger = logging.getLogger("storage")
        self.headers: Headers = headers
        self.block_headers: Headers = block_headers
        self.redis = redis  # NotImplemented

    async def close(self):
        await self.pg_database.close()
    # External API


def setup_headers_store(net_config, mmap_filename):
    Headers.max_cache_size = MMAP_SIZE
    HeadersRegTestMod.max_cache_size = MMAP_SIZE

    if net_config.NET == REGTEST:
        headers = HeadersRegTestMod.from_file(
            net_config.BITCOINX_COIN, mmap_filename, net_config.CHECKPOINT
        )
    else:
        headers = Headers.from_file(
            net_config.BITCOINX_COIN, mmap_filename, net_config.CHECKPOINT
        )
    return headers


async def reset_datastore():
    # remove headers - memory-mapped so need to do it this way to free memory immediately...
    headers_path = MODULE_DIR.parent.joinpath('headers.mmap')
    if os.path.exists(headers_path):
        with open(headers_path, 'w+') as f:
            mm = mmap.mmap(f.fileno(), MMAP_SIZE)
            mm.seek(0)
            mm.write(b'\00' * mm.size())

    # remove block headers - memory-mapped so need to do it this way to free memory immediately...
    block_headers_path = MODULE_DIR.parent.joinpath('block_headers.mmap')
    if os.path.exists(block_headers_path):
        with open(block_headers_path, 'w+') as f:
            mm = mmap.mmap(f.fileno(), MMAP_SIZE)
            mm.seek(0)
            mm.write(b'\00' * mm.size())

    # remove postgres tables
    pg_database = await pg_connect()
    await pg_database.pg_drop_tables()
    await pg_database.close()

    # remove lmdb database
    def remove_readonly(func, path, excinfo):
        os.chmod(path, stat.S_IWRITE)
        func(path)

    lmdb_path = MODULE_DIR.joinpath('database/lmdb_data')
    if os.path.exists(lmdb_path):
        shutil.rmtree(MODULE_DIR.joinpath('database/lmdb_data'), onerror=remove_readonly)


async def setup_storage(config, net_config) -> Storage:
    if config.get('reset'):
        await reset_datastore()

    headers = setup_headers_store(net_config, "headers.mmap")
    block_headers = setup_headers_store(net_config, "block_headers.mmap")

    # Postgres db
    pg_database = await load_pg_database()

    # Redis
    # -- NotImplemented
    storage = Storage(headers, block_headers, pg_database, None)
    return storage
