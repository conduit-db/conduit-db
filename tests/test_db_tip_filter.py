# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.

import time
import uuid

import pytest

from conduit_lib.database.db_interface.db import DatabaseType, DBInterface
from conduit_lib.database.db_interface.tip_filter import TipFilterQueryAPI
from conduit_lib.database.db_interface.tip_filter_types import (
    AccountMetadata,
    TipFilterRegistrationEntry,
    IndexerPushdataRegistrationFlag,
    OutboundDataRow,
    OutboundDataFlag,
)


class TestTipFilterQueryAPI:
    @pytest.fixture(scope="class", params=[DatabaseType.MySQL, DatabaseType.ScyllaDB])
    def db(self, request) -> TipFilterQueryAPI:
        db = DBInterface.load_db(worker_id=1, db_type=request.param)
        db.drop_tables()
        db.tip_filter_api.drop_tables()
        if hasattr(db, 'cache'):
            db.cache.r.flushall()
        db.drop_tables()
        db.create_permanent_tables()
        db.tip_filter_api.create_tables()
        yield db
        if hasattr(db, 'cache'):
            db.cache.r.flushall()
        db.drop_tables()
        db.close()

    def test_acounts_table(self, db: DBInterface) -> None:
        external_account_id = 1
        account_id = db.tip_filter_api.create_account_write(external_account_id)
        assert isinstance(account_id, str)

        account_metadata_list: list[AccountMetadata] = db.tip_filter_api.read_account_metadata([account_id])
        assert AccountMetadata(account_id, external_account_id) == account_metadata_list[0]

        account_id_actual = db.tip_filter_api.read_account_id_for_external_account_id(external_account_id)
        assert account_id == account_id_actual

    def test_tip_filter_registrations(self, db: DBInterface) -> None:
        external_account_id = 1
        date_created = int(time.time())

        pd_hash1 = bytes.fromhex("aa" * 32)
        pd_hash2 = bytes.fromhex("bb" * 32)
        pushdata_hash_list = [pd_hash1, pd_hash2]

        entry1 = TipFilterRegistrationEntry(pushdata_hash=pd_hash1, duration_seconds=3600 * 24 * 7)
        entry2 = TipFilterRegistrationEntry(pushdata_hash=pd_hash2, duration_seconds=60)
        registration_entries = [entry1, entry2]
        success = db.tip_filter_api.create_tip_filter_registrations_write(
            external_account_id, date_created, registration_entries
        )
        assert success is True

        # Should read all of them with default flags
        tip_filter_registrations = db.tip_filter_api.read_tip_filter_registrations()
        tip_filter_registrations.sort(key=lambda x: x.pushdata_hash.hex())
        assert tip_filter_registrations[0].pushdata_hash == entry1.pushdata_hash
        assert tip_filter_registrations[0].duration_seconds <= entry1.duration_seconds
        assert tip_filter_registrations[1].pushdata_hash == entry2.pushdata_hash
        assert tip_filter_registrations[1].duration_seconds <= entry2.duration_seconds

        # Should only show the second entry
        tip_filter_registrations = db.tip_filter_api.read_tip_filter_registrations(
            date_expires=int(time.time()) + 3600
        )
        assert tip_filter_registrations[0].pushdata_hash == entry2.pushdata_hash
        assert tip_filter_registrations[0].duration_seconds <= entry2.duration_seconds

        # Should not show any finalised (for deletion) entries yet
        tip_filter_registrations = db.tip_filter_api.read_tip_filter_registrations(
            date_expires=None,
            expected_flags=IndexerPushdataRegistrationFlag.FINALISED
            | IndexerPushdataRegistrationFlag.DELETING,
            mask=IndexerPushdataRegistrationFlag.FINALISED | IndexerPushdataRegistrationFlag.DELETING,
        )
        assert len(tip_filter_registrations) == 0

        list_registrations = db.tip_filter_api.read_indexer_filtering_registrations_for_notifications(
            [pd_hash1, pd_hash2]
        )
        assert len(list_registrations) == 2
        list_registrations.sort(key=lambda x: x.pushdata_hash)
        assert list_registrations[0].pushdata_hash == pd_hash1
        assert list_registrations[1].pushdata_hash == pd_hash2

        # Should not raise
        db.tip_filter_api.update_tip_filter_registrations_flags_write(
            external_account_id,
            pushdata_hash_list,
            IndexerPushdataRegistrationFlag.DELETING,
            None,
            IndexerPushdataRegistrationFlag.FINALISED,
            IndexerPushdataRegistrationFlag.FINALISED | IndexerPushdataRegistrationFlag.DELETING,
            True,
        )

        # All entries should have both the finalised_flag and deleting_flag set to 1 now.
        # This is a safety mechanism to ensure that state is being managed correctly by the client
        # and so that the cuckoo filter does not get corrupted for their account or affect other
        # user accounts
        tip_filter_registrations = db.tip_filter_api.read_tip_filter_registrations(
            date_expires=None,
            expected_flags=IndexerPushdataRegistrationFlag.FINALISED
            | IndexerPushdataRegistrationFlag.DELETING,
            mask=IndexerPushdataRegistrationFlag.FINALISED | IndexerPushdataRegistrationFlag.DELETING,
        )
        assert len(tip_filter_registrations) == 2
        assert tip_filter_registrations[0].pushdata_hash == entry1.pushdata_hash
        assert tip_filter_registrations[1].pushdata_hash == entry2.pushdata_hash

        # Delete one of the registration rows but leave the other in the "DELETING" state
        db.tip_filter_api.delete_tip_filter_registrations_write(
            external_account_id,
            pushdata_hashes=[pd_hash1],
            expected_flags=IndexerPushdataRegistrationFlag.FINALISED,
            mask=IndexerPushdataRegistrationFlag.FINALISED,
        )

        tip_filter_registrations = db.tip_filter_api.read_tip_filter_registrations()
        assert len(tip_filter_registrations) == 1
        assert tip_filter_registrations[0].pushdata_hash == entry2.pushdata_hash

        # The DELETING state row should be filtered out
        list_registrations = db.tip_filter_api.read_indexer_filtering_registrations_for_notifications(
            [pd_hash1, pd_hash2]
        )
        assert len(list_registrations) == 0

    def test_outbound_data_table(self, db: DBInterface) -> None:
        db.tip_filter_api.drop_tables()
        db.tip_filter_api.create_tables()
        date_created = int(time.time())
        outbound_data_row1 = OutboundDataRow(
            str(uuid.uuid4()),
            bytes.fromhex("aa" * 32),
            OutboundDataFlag.TIP_FILTER_NOTIFICATIONS,
            date_created,
            date_created,
        )
        outbound_data_row2 = OutboundDataRow(
            str(uuid.uuid4()),
            bytes.fromhex("bb" * 32),
            OutboundDataFlag.TIP_FILTER_NOTIFICATIONS,
            date_created,
            date_created,
        )

        db.tip_filter_api.create_outbound_data_write(outbound_data_row1)
        db.tip_filter_api.create_outbound_data_write(outbound_data_row2)

        rows = db.tip_filter_api.read_pending_outbound_datas(
            OutboundDataFlag.NONE,
            OutboundDataFlag.DISPATCHED_SUCCESSFULLY,
        )
        assert len(rows) == 2

        updated_flags = outbound_data_row1.outbound_data_flags
        updated_flags |= OutboundDataFlag.DISPATCHED_SUCCESSFULLY
        assert outbound_data_row1.outbound_data_id is not None
        delivery_updates = [(updated_flags, int(time.time()), outbound_data_row1.outbound_data_id)]
        db.tip_filter_api.update_outbound_data_last_tried_write(entries=delivery_updates)

        rows = db.tip_filter_api.read_pending_outbound_datas(
            OutboundDataFlag.NONE,
            OutboundDataFlag.DISPATCHED_SUCCESSFULLY,
        )
        assert len(rows) == 1
