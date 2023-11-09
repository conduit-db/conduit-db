import time

import pytest

from conduit_lib import DBInterface
from conduit_lib.constants import HashXLength
from conduit_lib.database.db_interface.db import DatabaseType
from conduit_lib.database.db_interface.types import MinedTxHashes, MempoolTransactionRow, \
    ConfirmedTransactionRow, InputRow, OutputRow, PushdataRow, HeadersRow
from conduit_lib.types import BlockHeaderRow


class TestDBInterface:

    @pytest.fixture(scope="class", params=[DatabaseType.MySQL, DatabaseType.ScyllaDB])
    def db(self, request) -> DBInterface:
        # This could be MySQL or ScyllaDB
        db = DBInterface.load_db(worker_id=1, db_type=request.param)
        if hasattr(db, 'cache'):
            db.cache.r.flushall()
        db.drop_tables()
        db.create_permanent_tables()
        yield db
        if hasattr(db, 'cache'):
            db.cache.r.flushall()
        db.drop_tables()
        db.close()

    def test_connection(self, db: DBInterface) -> None:
        db.ping()
        assert True

    def test_checkpoint_state_table(self, db: DBInterface) -> None:
        db.initialise_checkpoint_state()
        if db.db_type == DatabaseType.ScyllaDB:
            result = db.session.execute("SELECT * FROM checkpoint_state WHERE id=0")
            id = result[0].id
            best_flushed_block_height = result[0].best_flushed_block_height
            best_flushed_block_hash = result[0].best_flushed_block_hash
            first_allocated_block_hash = result[0].first_allocated_block_hash
            last_allocated_block_hash = result[0].last_allocated_block_hash
            new_hashes_array = result[0].new_hashes_array
            old_hashes_array = result[0].old_hashes_array
            reorg_was_allocated = result[0].reorg_was_allocated
        else:
            db.conn.query("SELECT * FROM checkpoint_state WHERE id=0")
            result = db.conn.store_result()
            row = result.fetch_row(0)[0]
            id = row[0]
            best_flushed_block_height = row[1]
            best_flushed_block_hash = row[2]
            reorg_was_allocated = row[3]
            first_allocated_block_hash = row[4]
            last_allocated_block_hash = row[5]
            new_hashes_array = row[6]
            old_hashes_array = row[7]

        assert id == 0
        assert best_flushed_block_height == 0
        assert best_flushed_block_hash == None
        assert reorg_was_allocated == False
        assert first_allocated_block_hash == None
        assert last_allocated_block_hash == None
        assert new_hashes_array == None
        assert old_hashes_array == None

    def test_get_tables(self, db: DBInterface) -> None:
        if db.db_type == DatabaseType.ScyllaDB:
            tables = db.get_tables()
            assert tables == [
                ('checkpoint_state',), ('confirmed_transactions',), ('headers',), ('inputs_table',),
                ('pushdata',), ('txo_table',)
            ]
        else:
            tables = db.get_tables()
            assert tables == (
                ('checkpoint_state',), ('confirmed_transactions',), ('headers',),
                ('inputs_table',), ('mempool_transactions',), ('pushdata',),
                ('temp_mempool_additions',), ('temp_mempool_removals',), ('txo_table',)
            )

    def test_temp_mined_tx_hashes_table(self, db: DBInterface) -> None:
        db.create_temp_mined_tx_hashes_table()

        txid1 = "aa"*32
        txid2 = "bb"*32
        inserted_rows = [MinedTxHashes(txid1, 1), MinedTxHashes(txid2, 2)]
        db.drop_temp_mined_tx_hashes()
        db.load_temp_mined_tx_hashes(inserted_rows)
        if db.db_type == DatabaseType.ScyllaDB:
            assert db.cache.get_in_namespace(b"temp_mined_tx_hashes", bytes.fromhex(txid1)) == b'1'
            assert db.cache.get_in_namespace(b"temp_mined_tx_hashes", bytes.fromhex(txid2)) == b'2'
            db.drop_temp_mined_tx_hashes()
            assert db.cache.get_in_namespace(b"temp_mined_tx_hashes", bytes.fromhex(txid1)) is None
            assert db.cache.get_in_namespace(b"temp_mined_tx_hashes", bytes.fromhex(txid2)) is None
        else:
            db.conn.query("SELECT * FROM temp_mined_tx_hashes")
            result = db.conn.store_result()
            rows = result.fetch_row(0)
            for row in rows:
                row = MinedTxHashes(row[0].hex(), row[1])
                assert row in inserted_rows
            assert 'temp_mined_tx_hashes' in [row[0] for row in db.get_tables()]
            db.drop_temp_mined_tx_hashes()
            assert 'temp_mined_tx_hashes' not in [row[0] for row in db.get_tables()]

    def test_mempool_table(self, db: DBInterface) -> None:
        db.bulk_load_mempool_tx_rows(tx_rows=[])
        txid1 = 'aa'*32
        txid2 = 'bb'*32
        timestamp = int(time.time())
        row1 = MempoolTransactionRow(mp_tx_hash=txid1, mp_tx_timestamp=timestamp)
        row2 = MempoolTransactionRow(mp_tx_hash=txid2, mp_tx_timestamp=timestamp)

        # Empty
        if db.db_type == DatabaseType.ScyllaDB:
            assert db.cache.get_in_namespace(b"mempool", bytes.fromhex(txid1)) is None
            assert db.cache.get_in_namespace(b"mempool", bytes.fromhex(txid2)) is None
        else:
            db.conn.query("SELECT * FROM mempool_transactions")
            result = db.conn.store_result()
            rows = result.fetch_row(0)
            assert len(rows) == 0

        # Filled
        db.bulk_load_mempool_tx_rows(tx_rows=[row1, row2])
        if db.db_type == DatabaseType.ScyllaDB:
            assert int(db.cache.get_in_namespace(b"mempool", bytes.fromhex(txid1)).decode()) == timestamp
            assert int(db.cache.get_in_namespace(b"mempool", bytes.fromhex(txid2)).decode()) == timestamp
        else:
            db.conn.query("SELECT * FROM mempool_transactions")
            result = db.conn.store_result()
            rows = result.fetch_row(0)
            assert rows[0][0].hex() == txid1
            assert rows[0][1] == timestamp
            assert rows[1][0].hex() == txid2
            assert rows[1][1] == timestamp

        # DROP
        if db.db_type == DatabaseType.ScyllaDB:
            db.drop_mempool_table()  # does a redis "flushall"
            all_keys = db.cache.scan_in_namespace(b"mempool")
            assert all_keys == []
        else:
            assert 'mempool_transactions' in [row[0] for row in db.get_tables()]
            db.drop_mempool_table()
            assert 'mempool_transactions' not in [row[0] for row in db.get_tables()]

    def test_confirmed_transactions_table(self, db: DBInterface) -> None:
        db.bulk_load_confirmed_tx_rows(tx_rows=[])
        txid1 = 'aa'*HashXLength
        txid2 = 'bb'*HashXLength
        row1 = ConfirmedTransactionRow(tx_hash=txid1, tx_block_num=1, tx_position=1)
        row2 = ConfirmedTransactionRow(tx_hash=txid2, tx_block_num=2, tx_position=2)

        # Empty
        if db.db_type == DatabaseType.ScyllaDB:
            db.session.execute("SELECT * FROM confirmed_transactions")
        else:
            db.conn.query("SELECT * FROM confirmed_transactions")
            result = db.conn.store_result()
            rows = result.fetch_row(0)
            assert len(rows) == 0

        # Filled
        db.bulk_load_confirmed_tx_rows(tx_rows=[row1, row2])
        if db.db_type == DatabaseType.ScyllaDB:
            pass
        else:
            db.conn.query("SELECT * FROM confirmed_transactions")
            result = db.conn.store_result()
            rows = result.fetch_row(0)
            assert rows[0][0].hex() == txid1
            assert rows[0][1] == 1
            assert rows[0][2] == 1
            assert rows[1][0].hex() == txid2
            assert rows[1][1] == 2
            assert rows[1][2] == 2

    def test_inputs_table(self, db: DBInterface) -> None:
        db.bulk_load_input_rows(in_rows=[])
        txid1 = 'aa' * HashXLength
        txid2 = 'bb' * HashXLength
        row1 = InputRow(out_tx_hash=txid1, out_idx=1, in_tx_hash=txid1, in_idx=1)
        row2 = InputRow(out_tx_hash=txid2, out_idx=2, in_tx_hash=txid2, in_idx=2)

        # Empty
        if db.db_type == DatabaseType.ScyllaDB:
            pass
        else:
            db.conn.query("SELECT * FROM inputs_table")
            result = db.conn.store_result()
            rows = result.fetch_row(0)
            assert len(rows) == 0

        # Filled
        db.bulk_load_input_rows(in_rows=[row1, row2])
        if db.db_type == DatabaseType.ScyllaDB:
            pass
        else:
            db.conn.query("SELECT * FROM inputs_table")
            result = db.conn.store_result()
            rows = result.fetch_row(0)
            assert rows[0][0].hex() == txid1
            assert rows[0][1] == 1
            assert rows[0][2].hex() == txid1
            assert rows[0][3] == 1
            assert rows[1][0].hex() == txid2
            assert rows[1][1] == 2
            assert rows[1][2].hex() == txid2
            assert rows[1][3] == 2

    def test_txo_table(self, db: DBInterface) -> None:
        db.bulk_load_output_rows(out_rows=[])
        txid1 = 'aa' * HashXLength
        txid2 = 'bb' * HashXLength
        row1 = OutputRow(out_tx_hash=txid1, out_idx=1, out_value=1)
        row2 = OutputRow(out_tx_hash=txid2, out_idx=2, out_value=2)

        # Empty
        if db.db_type == DatabaseType.ScyllaDB:
            pass
        else:
            db.conn.query("SELECT * FROM txo_table")
            result = db.conn.store_result()
            rows = result.fetch_row(0)
            assert len(rows) == 0

        # Filled
        db.bulk_load_output_rows(out_rows=[row1, row2])
        if db.db_type == DatabaseType.ScyllaDB:
            pass
        else:
            db.conn.query("SELECT * FROM txo_table")
            result = db.conn.store_result()
            rows = result.fetch_row(0)
            assert rows[0][0].hex() == txid1
            assert rows[0][1] == 1
            assert rows[0][2] == 1
            assert rows[1][0].hex() == txid2
            assert rows[1][1] == 2
            assert rows[1][2] == 2

    def test_pushdata_table(self, db: DBInterface) -> None:
        db.bulk_load_pushdata_rows(pd_rows=[])
        txid1 = 'aa' * HashXLength
        txid2 = 'bb' * HashXLength
        row1 = PushdataRow(pushdata_hash=txid1, tx_hash=txid1, idx=1, ref_type=0)
        row2 = PushdataRow(pushdata_hash=txid2, tx_hash=txid1, idx=1, ref_type=1)

        # Empty
        if db.db_type == DatabaseType.ScyllaDB:
            pass
        else:
            db.conn.query("SELECT * FROM pushdata")
            result = db.conn.store_result()
            rows = result.fetch_row(0)
            assert len(rows) == 0

        # Filled
        db.bulk_load_pushdata_rows(pd_rows=[row1, row2])
        if db.db_type == DatabaseType.ScyllaDB:
            pass
        else:
            db.conn.query("SELECT * FROM pushdata")
            result = db.conn.store_result()
            rows = result.fetch_row(0)
            assert rows[0][0].hex() == txid1
            assert rows[0][1].hex() == txid1
            assert rows[0][2] == 1
            assert rows[0][3] == 0
            assert rows[1][0].hex() == txid2
            assert rows[1][1].hex() == txid1
            assert rows[1][2] == 1
            assert rows[1][3] == 1

    def test_headers_table(self, db: DBInterface) -> None:
        db.bulk_load_headers(block_header_rows=[])

        block_hash1 = 'aa' * 32
        block_hash2 = 'bb' * 32
        block_header1 = 'aa' * 80
        block_header2 = 'bb' * 80

        row1 = BlockHeaderRow(block_num=1, block_hash=block_hash1, block_height=1, block_header=block_header1,
            block_tx_count=1, block_size=1, is_orphaned=1)
        row2 = BlockHeaderRow(block_num=2, block_hash=block_hash2,block_height=2, block_header=block_header2,
            block_tx_count=2, block_size=2, is_orphaned=0)

        # Empty
        if db.db_type == DatabaseType.ScyllaDB:
            pass
        else:
            db.conn.query("SELECT * FROM headers")
            result = db.conn.store_result()
            rows = result.fetch_row(0)
            assert len(rows) == 0

        # Filled
        db.bulk_load_headers(block_header_rows=[row1, row2])
        if db.db_type == DatabaseType.ScyllaDB:
            pass
        else:
            db.conn.query("SELECT * FROM headers")
            result = db.conn.store_result()
            rows = result.fetch_row(0)
            assert rows[0][0] == 1
            assert rows[0][1].hex() == block_hash1
            assert rows[0][2] == 1
            assert rows[0][3].hex() == block_header1
            assert rows[0][4] == 1
            assert rows[0][5] == 1
            assert rows[0][6] == 1

            assert rows[1][0] == 2
            assert rows[1][1].hex() == block_hash2
            assert rows[1][2] == 2
            assert rows[1][3].hex() == block_header2
            assert rows[1][4] == 2
            assert rows[1][5] == 2
            assert rows[1][6] == 0

    def test_temp_orphaned_txs_table(self, db: DBInterface) -> None:
        db.load_temp_orphaned_tx_hashes(orphaned_tx_hashes=set())
        db.drop_temp_orphaned_txs()

    def test_temp_mempool_removals_table(self, db: DBInterface) -> None:
        db.load_temp_mempool_removals(removals_from_mempool=set())

    def test_temp_mempool_additions_table(self, db: DBInterface) -> None:
        db.load_temp_mempool_additions(additions_to_mempool=set())

    def test_temp_inbound_tx_hashes_table(self, db: DBInterface) -> None:
        db.create_temp_inbound_tx_hashes_table('worker1')
        db.load_temp_inbound_tx_hashes(inbound_tx_hashes=[], inbound_tx_table_name='worker1')
        db.drop_temp_inbound_tx_hashes(inbound_tx_table_name='worker1')

    def test_drop_indices(self, db: DBInterface) -> None:
        db.drop_indices()
