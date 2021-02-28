import logging
import bitcoinx

from conduit_raw.conduit.database.mysql.mysql_database import MySQLDatabase, load_mysql_database
from conduit_raw.conduit.networks import NetworkConfig
from conduit_raw.conduit.store import setup_storage, Storage


class TestStorage:
    config = {
        "database_name": "conduittestdb",
        "network": "regtest",
        "database_username": "conduitadmin",
        "database_host": "127.0.0.1",
        "database_port": 5432,
        "database_password": "conduitpass",
    }

    net_config = NetworkConfig(config.get("network"))
    headers = bitcoinx.Headers.from_file(
        net_config.BITCOINX_COIN, "headers.mmap", net_config.CHECKPOINT
    )
    block_headers = bitcoinx.Headers.from_file(
        net_config.BITCOINX_COIN, "block_headers.mmap", net_config.CHECKPOINT
    )
    storage = setup_storage(config, net_config)
    mysql_db: MySQLDatabase = load_mysql_database()
    redis = None  # NotImplemented
    logger = logging.getLogger("TestStorage")

    @classmethod
    def setup_class(klass) -> None:
        klass.storage = Storage(
            klass.headers, klass.block_headers, klass.mysql_db, klass.redis
        )
        klass.storage.mysql_database.tables.mysql_drop_tables()
        klass.storage.mysql_database.tables.mysql_create_permanent_tables()

    def setup_method(self) -> None:
        self.mysql_db.tables.mysql_create_permanent_tables()

    def teardown_method(self) -> None:
        self.storage.mysql_database.tables.mysql_create_permanent_tables()

    @classmethod
    def teardown_class(klass) -> None:
        klass.mysql_db.close()

    def test_empty_test(klass):
        assert True
