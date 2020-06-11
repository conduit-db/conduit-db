import asyncpg
from bitcoinx import Headers

from .database import load_pg_database, PG_Database
from .constants import REGTEST
from .networks import HeadersRegTestMod
from .logs import logs


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
        self.logger = logs.get_logger("storage")
        self.headers: Headers = headers
        self.block_headers: Headers = block_headers
        self.redis = redis  # NotImplemented

    # External API



def setup_headers_store(net_config, mmap_filename):
    Headers.max_cache_size = 2_000_000
    HeadersRegTestMod.max_cache_size = 2_000_000
    if net_config.NET == REGTEST:
        headers = HeadersRegTestMod.from_file(
            net_config.BITCOINX_COIN, mmap_filename, net_config.CHECKPOINT
        )
    else:
        headers = Headers.from_file(
            net_config.BITCOINX_COIN, mmap_filename, net_config.CHECKPOINT
        )
    return headers


async def setup_storage(net_config) -> Storage:
    headers = setup_headers_store(net_config, "headers.mmap")
    block_headers = setup_headers_store(net_config, "block_headers.mmap")

    # Postgres db
    pg_database = await load_pg_database()

    # Redis
    # -- NotImplemented
    storage = Storage(headers, block_headers, pg_database, None)
    return storage
