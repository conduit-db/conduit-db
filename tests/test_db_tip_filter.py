# import pytest
#
# from conduit_lib.database.db_interface.db import DatabaseType, DBInterface
# from conduit_lib.database.db_interface.tip_filter import TipFilterQueryAPI
#
#
# class TestTipFilterQueryAPI:
#
#     @pytest.fixture(scope="class", params=[DatabaseType.MySQL, DatabaseType.ScyllaDB])
#     def db(self, request) -> TipFilterQueryAPI:
#         db = DBInterface.load_db(worker_id=1, db_type=request.param)
#         tfapi = TipFilterQueryAPI.from_db(db)
#         db.drop_tables()
#         db.drop_temp_mined_tx_hashes()
#         db.drop_temp_orphaned_txs()
#         if hasattr(db, 'cache'):
#             db.cache.r.flushall()
#         db.drop_tables()
#         db.create_permanent_tables()
#         yield db
#         if hasattr(db, 'cache'):
#             db.cache.r.flushall()
#         db.drop_tables()
#         db.close()
#     def create_account_write(self, external_account_id: int) -> int:
#         pass
#
#     def read_account_metadata(self, account_ids: list[int]) -> list[AccountMetadata]:
#         pass
#
#     def read_account_id_for_external_account_id(self, external_account_id: int) -> int:
#         pass
#
#     def create_tip_filter_registrations_write(
#             self,
#             external_account_id: int,
#             date_created: int,
#             registration_entries: list[TipFilterRegistrationEntry],
#     ) -> bool:
#         pass
#
#     def read_tip_filter_registrations(
#             self,
#             account_id: Optional[int] = None,
#             date_expires: Optional[int] = None,
#             # These defaults include all rows no matter the flag value.
#             expected_flags: IndexerPushdataRegistrationFlag = IndexerPushdataRegistrationFlag.NONE,
#             mask: IndexerPushdataRegistrationFlag = IndexerPushdataRegistrationFlag.NONE,
#     ) -> list[TipFilterRegistrationEntry]:
#         """
#         Load the non-expired tip filter registrations from the database especially to populate the
#         tip filter.
#         """
#         pass
#
#     def read_indexer_filtering_registrations_for_notifications(
#             self, pushdata_hashes: list[bytes], account_id: int | None = None
#     ) -> list[FilterNotificationRow]:
#         """
#         These are the matches that in either a new mempool transaction or a block which were
#         present (correctly or falsely) in the common cuckoo filter.
#         """
#         pass
#
#     def update_tip_filter_registrations_flags_write(
#             self,
#             external_account_id: int,
#             pushdata_hashes: list[bytes],
#             update_flags: IndexerPushdataRegistrationFlag = IndexerPushdataRegistrationFlag.NONE,
#             update_mask: Optional[IndexerPushdataRegistrationFlag] = None,
#             filter_flags: IndexerPushdataRegistrationFlag = IndexerPushdataRegistrationFlag.NONE,
#             filter_mask: Optional[IndexerPushdataRegistrationFlag] = None,
#             require_all: bool = False,
#     ) -> None:
#         """There is a MySQL and a ScyllaDB version of the update_tip_filter_registrations_flags_write
#         because ScyllaDB cannot handle bitwise operations in queries and updates."""
#         pass
#
#     def expire_tip_filter_registrations(self, date_expires: int) -> list[bytes]:
#         """
#         Atomic call to locate expired registrations and to delete them. It will return the keys for
#         all the rows that were deleted.
#
#         Returns `[ (account_id, pushdata_hash), pass ]`
#         Raises no known exceptions.
#         """
#         pass
#
#     def delete_tip_filter_registrations_write(
#             self,
#             external_account_id: int,
#             pushdata_hashes: list[bytes],
#             # These defaults include all rows no matter the existing flag value.
#             expected_flags: IndexerPushdataRegistrationFlag = IndexerPushdataRegistrationFlag.NONE,
#             mask: IndexerPushdataRegistrationFlag = IndexerPushdataRegistrationFlag.NONE,
#     ) -> None:
#         pass
#
#     def create_outbound_data_write(self, creation_row: OutboundDataRow) -> int:
#         pass
#
#     def read_pending_outbound_datas(
#             self, flags: OutboundDataFlag, mask: OutboundDataFlag, account_id: int | None = None
#     ) -> list[OutboundDataRow]:
#         pass
#
#     def update_outbound_data_last_tried_write(self,
#             entries: list[tuple[OutboundDataFlag, int, int]]) -> None:
#         pass
