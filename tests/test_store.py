import logging
import os
import shutil
from pathlib import Path

import bitcoinx
from conduit import database
from conduit.networks import NetworkConfig, NETWORKS
from conduit.store import Storage


class TestStorage:
    env_vars = {
        "database_name": "test_conduitdb",
        "network": "regtest",
        "database_username": "conduitadmin",
        "database_host": "127.0.0.1",
        "database_port": 5432,
        "database_password": "conduitpass",
    }
    db = database.load(env_vars)

    net_config = NetworkConfig(env_vars.get("network"))
    headers = bitcoinx.Headers.from_file(
        net_config.BITCOINX_COIN, "headers.mmap", net_config.CHECKPOINT
    )
    block_headers = bitcoinx.Headers.from_file(
        net_config.BITCOINX_COIN, "block_headers.mmap", net_config.CHECKPOINT
    )
    pg_db = database
    redis = None  # NotImplemented
    logger = logging.getLogger("TestStorage")

    @classmethod
    def setup_class(klass) -> None:
        klass.db.connect()
        klass.storage = Storage(
            klass.headers, klass.block_headers, klass.pg_db, klass.redis
        )
        klass.storage.pg_database.pg_drop_tables([database.Transaction])
        klass.storage.pg_database.create_tables([database.Transaction])

    def setup_method(self) -> None:
        with self.storage.pg_database.atomic():
            self.storage.pg_database.create_tables([database.Transaction])

    def teardown_method(self) -> None:
        # pylint: disable=no-value-for-parameter
        with self.storage.pg_database.atomic():
            self.storage.pg_database.pg_drop_tables([database.Transaction])

    @classmethod
    def teardown_class(klass) -> None:
        klass.db.close()

    def test_insert_single_tx(self):
        expected_inserts = [
            (b"1", 1, b"deadbeef"),
            (b"2", 2, b"deadbeef"),
            (b"3", 3, b"deadbeef"),
        ]

        for row in expected_inserts:
            self.storage.insert_tx(row[0], row[1], row[2])

        tx_hashes = [tx_hash for tx_hash, height, rawtx in expected_inserts]
        txs_mvs = self.storage.get_many_txs_mvs(tx_hashes)
        txs_values = self.storage.get_many_txs_data(tx_hashes)

        assert all(
            isinstance(tx_hash, memoryview) for tx_hash, height, rawtx in txs_mvs
        )
        assert txs_values == expected_inserts

    def test_insert_multiple_txs(self):
        expected_inserts = [
            (b"1", 1, b"deadbeef"),
            (b"2", 2, b"deadbeef"),
            (b"3", 3, b"deadbeef"),
        ]

        tx_hashes = [tx_hash for tx_hash, height, rawtx in expected_inserts]

        self.storage.insert_many_txs(expected_inserts)
        txs_mvs = self.storage.get_many_txs_mvs(tx_hashes)
        txs_values = self.storage.get_many_txs_data(tx_hashes)

        assert all(
            isinstance(tx_hash, memoryview) for tx_hash, height, rawtx in txs_mvs
        )
        assert txs_values == expected_inserts
