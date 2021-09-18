import logging
import os
import shutil
import stat
import mmap
import sys
from pathlib import Path
from typing import Optional, Dict

import bitcoinx
from bitcoinx import Headers
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import KafkaException

from .database.lmdb.lmdb_database import LMDB_Database
from .database.mysql.mysql_database import load_mysql_database, MySQLDatabase, mysql_connect
from .constants import REGTEST
from .networks import HeadersRegTestMod
from .utils import is_docker

MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
MMAP_SIZE = 2_000_000

logger = logging.getLogger("storage")


class Storage:
    """High-level Interface to database (postgres at present)"""
    def __init__(
        self,
        headers: Headers,
        block_headers: Headers,
        mysql_database: Optional[MySQLDatabase],
        lmdb: Optional[LMDB_Database],
    ):
        self.mysql_database = mysql_database
        self.headers = headers
        self.block_headers = block_headers
        self.lmdb = lmdb

    async def close(self):
        if self.mysql_database:
            self.mysql_database.close()

    # External API

    def get_header_for_hash(self, block_hash: bytes) -> bitcoinx.Header:
        header, chain = self.headers.lookup(block_hash)
        return header


def setup_headers_store(net_config, mmap_filename):
    bitcoinx_chain_logger = logging.getLogger('chain')
    bitcoinx_chain_logger.setLevel(logging.WARNING)

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


def reset_headers(headers_path: Path, block_headers_path: Path, config: Dict):
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
                os.remove(headers_path)
        except FileNotFoundError:
            pass


def reset_datastore(headers_path: Path, block_headers_path: Path, config: Dict):
    # remove headers - memory-mapped so need to do it this way to free memory immediately...

    reset_headers(headers_path, block_headers_path, config)

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

    if config['server_type'] == "ConduitRaw":
        def reset_kafka_topics():
            kafka_broker = {
                'bootstrap.servers': os.environ.get('KAFKA_HOST', "127.0.0.1:26638"),
            }
            logger.debug("deleting kafka topics...")
            admin_client = AdminClient(kafka_broker)
            futures_dict = admin_client.delete_topics(['logging', 'conduit-raw-headers-state'],
                operation_timeout=30)

            # Wait for operation to finish.
            for topic, f in futures_dict.items():
                try:
                    f.result()  # The result itself is None
                    logger.debug("Topic {} deleted".format(topic))
                except KafkaException as e:
                    logger.debug("Failed to delete topic {}: {}".format(topic, e))

        if not is_docker():
            reset_kafka_topics()


def setup_storage(config, net_config, headers_dir: Optional[Path]=None) -> Storage:
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
