# import bitcoinx
# import os
# from pathlib import Path
#
# from conduit_lib.networks import NetworkConfig
# from conduit_lib.utils import get_network_type
#
# MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
#
#
# class TestStorage:
#     # Todo - create a test database to not interfere with running servers
#     config = {
#         "database_name": "conduittestdb",
#         "network": "regtest",
#         "database_username": "conduitadmin",
#         "database_host": "127.0.0.1",
#         "database_port": 5432,
#         "database_password": "conduitpass",
#         "server_type": "ConduitRaw",
#     }
#
#     net_config = NetworkConfig(get_network_type(), node_host="127.0.0.1", node_port=18444)
#     headers = bitcoinx.Headers(
#         net_config.BITCOINX_COIN,
#         MODULE_DIR / "data/headers.mmap",
#         net_config.CHECKPOINT,
#     )
#     block_headers = bitcoinx.Headers(
#         net_config.BITCOINX_COIN,
#         MODULE_DIR / "data/block_headers.mmap",
#         net_config.CHECKPOINT,
#     )
#     # storage = setup_storage(config, net_config)
#     # redis = None  # NotImplemented
#     # db: DBInterface
#     # db_available: bool
#     # logger = logging.getLogger("TestStorage")
#
#     @classmethod
#     def setup_class(klass) -> None:
#         pass
#         # try:
#         #     klass.db: DBInterface = load_database()
#         #     klass.db_available = True
#         # except MySQLdb.OperationalError:
#         #     klass.db_available = False
#         #     return
#         #
#         # klass.storage = Storage(
#         #     klass.headers, klass.block_headers, klass.db, klass.redis
#         # )
#         # klass.storage.db.drop_tables()
#         # klass.storage.db.create_permanent_tables()
#
#     def setup_method(self) -> None:
#         pass
#         # if not self.db_available:
#         #     pytest.skip("db unavailable")
#         # self.db.create_permanent_tables()
#
#     def teardown_method(self) -> None:
#         pass
#         # if not self.db_available:
#         #     pytest.skip("db unavailable")
#         # self.storage.db.create_permanent_tables()
#
#     @classmethod
#     def teardown_class(klass) -> None:
#         pass
#         # if not klass.db_available:
#         #     return
#         # klass.db.close()
#
#     def test_storage_init(klass):
#         assert True
