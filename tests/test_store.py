import logging
import os
from pathlib import Path

import MySQLdb
import bitcoinx
import pytest

from conduit_lib.database.mysql.mysql_database import MySQLDatabase, load_mysql_database
from conduit_lib.networks import NetworkConfig
from conduit_lib.store import setup_storage, Storage
from conduit_lib.utils import get_network_type

MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))


class TestStorage:
    # Todo - create a test database to not interfere with running servers
    config = {
        "database_name": "conduittestdb",
        "network": "regtest",
        "database_username": "conduitadmin",
        "database_host": "127.0.0.1",
        "database_port": 5432,
        "database_password": "conduitpass",
        "server_type": "ConduitRaw"
    }

    net_config = NetworkConfig(get_network_type(), node_host='127.0.0.1', node_port=18444)
    headers = bitcoinx.Headers(
        net_config.BITCOINX_COIN, MODULE_DIR / "data/headers.mmap", net_config.CHECKPOINT
    )
    block_headers = bitcoinx.Headers(
        net_config.BITCOINX_COIN, MODULE_DIR / "data/block_headers.mmap", net_config.CHECKPOINT
    )
    # storage = setup_storage(config, net_config)
    # redis = None  # NotImplemented
    # mysql_db: MySQLDatabase
    # mysql_available: bool
    # logger = logging.getLogger("TestStorage")

    @classmethod
    def setup_class(klass) -> None:
        pass
        # try:
        #     klass.mysql_db: MySQLDatabase = load_mysql_database()
        #     klass.mysql_available = True
        # except MySQLdb.OperationalError:
        #     klass.mysql_available = False
        #     return
        #
        # klass.storage = Storage(
        #     klass.headers, klass.block_headers, klass.mysql_db, klass.redis
        # )
        # klass.storage.mysql_database.tables.mysql_drop_tables()
        # klass.storage.mysql_database.tables.mysql_create_permanent_tables()

    def setup_method(self) -> None:
        pass
        # if not self.mysql_available:
        #     pytest.skip("mysql unavailable")
        # self.mysql_db.tables.mysql_create_permanent_tables()

    def teardown_method(self) -> None:
        pass
        # if not self.mysql_available:
        #     pytest.skip("mysql unavailable")
        # self.storage.mysql_database.tables.mysql_create_permanent_tables()

    @classmethod
    def teardown_class(klass) -> None:
        pass
        # if not klass.mysql_available:
        #     return
        # klass.mysql_db.close()

    def test_storage_init(klass):
        assert True