import os
import time

import bitcoinx
import pytest
from bitcoinx import pack_le_uint32

from conduit_lib import DBInterface
from conduit_lib.constants import HashXLength, MAX_UINT32
from conduit_lib.database.db_interface.db import DatabaseType
from conduit_lib.database.db_interface.types import (
    MinedTxHashes,
    MempoolTransactionRow,
    ConfirmedTransactionRow,
    InputRow,
    OutputRow,
    PushdataRow,
)
from conduit_lib.types import BlockHeaderRow, TxMetadata, RestorationFilterQueryResult, TxLocation, \
    PushdataMatchFlags


class TestDBInterface:
    @pytest.fixture(scope="class", params=[DatabaseType.MySQL, DatabaseType.ScyllaDB])
    def db(self, request) -> DBInterface:
        db = DBInterface.load_db(worker_id=1, db_type=request.param)
        db.drop_tables()
        db.drop_temp_mined_tx_hashes()
        db.drop_temp_orphaned_txs()
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
        # INITIAL STATE
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
        assert best_flushed_block_hash is None
        assert reorg_was_allocated == 0
        assert first_allocated_block_hash is None
        assert last_allocated_block_hash is None
        assert new_hashes_array is None
        assert old_hashes_array is None

        # UPDATED STATE
        new_tip_header = bitcoinx.Header(
            version=1,
            prev_hash=bytes.fromhex("ff" * 32),
            merkle_root=bytes.fromhex("ff" * 32),
            timestamp=time.time(),
            bits=pack_le_uint32(486604799),
            nonce=1000,
            hash=bytes.fromhex("ff" * 32),
            raw=os.urandom(80),
            height=800000,
        )
        db.update_checkpoint_tip(new_tip_header)

        start_header = bitcoinx.Header(
            version=1,
            prev_hash=bytes.fromhex("aa" * 32),
            merkle_root=bytes.fromhex("bb" * 32),
            timestamp=time.time(),
            bits=pack_le_uint32(486604799),
            nonce=1000,
            hash=bytes.fromhex("cc" * 32),
            raw=os.urandom(80),
            height=800001,
        )
        stop_header = bitcoinx.Header(
            version=1,
            prev_hash=bytes.fromhex("dd" * 32),
            merkle_root=bytes.fromhex("ee" * 32),
            timestamp=time.time(),
            bits=pack_le_uint32(486604799),
            nonce=1000,
            hash=bytes.fromhex("ff" * 32),
            raw=os.urandom(80),
            height=800002,
        )
        old_hashes = [bytes.fromhex("aabb" * 16), bytes.fromhex("aacc" * 16)]
        new_hashes = [bytes.fromhex("aadd" * 16), bytes.fromhex("aaee" * 16)]
        db.update_allocated_state(
            reorg_was_allocated=False,
            first_allocated=start_header,
            last_allocated=stop_header,
            old_hashes=old_hashes,
            new_hashes=new_hashes,
        )

        if db.db_type == DatabaseType.ScyllaDB:
            result = db.session.execute("SELECT * FROM checkpoint_state WHERE id=0")
            id = result[0].id
            best_flushed_block_height = result[0].best_flushed_block_height
            best_flushed_block_hash = result[0].best_flushed_block_hash
            first_allocated_block_hash = result[0].first_allocated_block_hash
            last_allocated_block_hash = result[0].last_allocated_block_hash
            old_hashes_array = result[0].old_hashes_array
            new_hashes_array = result[0].new_hashes_array
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
            old_hashes_array = row[6]
            new_hashes_array = row[7]

        assert id == 0
        assert best_flushed_block_height == 800000
        assert best_flushed_block_hash == bytes.fromhex('ff' * 32)
        assert reorg_was_allocated == 0
        assert first_allocated_block_hash == bytes.fromhex('cc' * 32)
        assert last_allocated_block_hash == bytes.fromhex('ff' * 32)
        assert new_hashes_array == b"".join(new_hashes)
        assert old_hashes_array == b"".join(old_hashes)

    def test_get_tables(self, db: DBInterface) -> None:
        if db.db_type == DatabaseType.ScyllaDB:
            tables = db.get_tables()
            assert tables == [
                ('checkpoint_state',),
                ('confirmed_transactions',),
                ('headers',),
                ('inputs_table',),
                ('pushdata',),
                ('txo_table',),
            ]
        else:
            tables = db.get_tables()
            assert tables == (
                ('checkpoint_state',),
                ('confirmed_transactions',),
                ('headers',),
                ('inputs_table',),
                ('mempool_transactions',),
                ('pushdata',),
                ('txo_table',),
            )

    def test_temp_mined_tx_hashes_table(self, db: DBInterface) -> None:
        db.load_temp_mined_tx_hashes([])

        txid1 = "aa" * 32
        txid2 = "bb" * 32
        inserted_rows = [MinedTxHashes(txid1, 1), MinedTxHashes(txid2, 2)]
        db.drop_temp_mined_tx_hashes()
        db.load_temp_mined_tx_hashes(inserted_rows)
        if db.db_type == DatabaseType.ScyllaDB:
            assert db.cache.r.smembers('temp_mined_tx_hashes') == {bytes.fromhex(txid1), bytes.fromhex(txid2)}
            db.drop_temp_mined_tx_hashes()
            assert db.cache.r.smembers('temp_mined_tx_hashes') == set()
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

    def test_confirmed_transactions_table(self, db: DBInterface) -> None:
        db.bulk_load_confirmed_tx_rows(tx_rows=[])
        txid1 = 'aa' * HashXLength
        txid2 = 'bb' * HashXLength
        row1 = ConfirmedTransactionRow(tx_hash=txid1, tx_block_num=1, tx_position=1)
        row2 = ConfirmedTransactionRow(tx_hash=txid2, tx_block_num=2, tx_position=2)

        # Empty
        if db.db_type == DatabaseType.ScyllaDB:
            result = db.session.execute(
                "SELECT tx_hash, tx_block_num, tx_position FROM confirmed_transactions"
            )
            assert len(result.all()) == 0
        else:
            db.conn.query("SELECT tx_hash, tx_block_num, tx_position FROM confirmed_transactions")
            result = db.conn.store_result()
            rows = result.fetch_row(0)
            assert len(rows) == 0

        # Filled
        db.bulk_load_confirmed_tx_rows(tx_rows=[row1, row2])
        if db.db_type == DatabaseType.ScyllaDB:
            result = db.session.execute(
                "SELECT tx_hash, tx_block_num, tx_position FROM confirmed_transactions"
            )
            rows = result.all()
            rows.sort()
        else:
            db.conn.query("SELECT tx_hash, tx_block_num, tx_position FROM confirmed_transactions")
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
            result = db.session.execute("SELECT out_tx_hash, out_idx, in_tx_hash, out_idx FROM inputs_table")
            rows = result.all()
            assert len(rows) == 0
        else:
            db.conn.query("SELECT out_tx_hash, out_idx, in_tx_hash, out_idx FROM inputs_table")
            result = db.conn.store_result()
            rows = result.fetch_row(0)
            assert len(rows) == 0

        # Filled
        db.bulk_load_input_rows(in_rows=[row1, row2])
        if db.db_type == DatabaseType.ScyllaDB:
            result = db.session.execute("SELECT out_tx_hash, out_idx, in_tx_hash, out_idx FROM inputs_table")
            rows = result.all()
            rows.sort()
        else:
            db.conn.query("SELECT out_tx_hash, out_idx, in_tx_hash, out_idx FROM inputs_table")
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
            result = db.session.execute("SELECT out_tx_hash, out_idx, out_value FROM txo_table")
            rows = result.all()
            assert len(rows) == 0
        else:
            db.conn.query("SELECT out_tx_hash, out_idx, out_value FROM txo_table")
            result = db.conn.store_result()
            rows = result.fetch_row(0)
            assert len(rows) == 0

        # Filled
        db.bulk_load_output_rows(out_rows=[row1, row2])
        if db.db_type == DatabaseType.ScyllaDB:
            result = db.session.execute("SELECT out_tx_hash, out_idx, out_value FROM txo_table")
            rows = result.all()
            rows.sort()
        else:
            db.conn.query("SELECT out_tx_hash, out_idx, out_value FROM txo_table")
            result = db.conn.store_result()
            rows = result.fetch_row(0)
        assert rows[0][0].hex() == txid1
        assert rows[0][1] == 1
        assert rows[0][2] == 1
        assert rows[1][0].hex() == txid2
        assert rows[1][1] == 2
        assert rows[1][2] == 2

    def test_pushdata_table(self, db: DBInterface) -> None:
        db.drop_tables()
        db.create_permanent_tables()
        db.bulk_load_pushdata_rows(pd_rows=[])
        txid1 = 'aa' * HashXLength
        txid2 = 'bb' * HashXLength
        pushdata_hash1 = 'ee' * HashXLength
        pushdata_hash2 = 'ff' * HashXLength
        row1 = PushdataRow(pushdata_hash=pushdata_hash1, tx_hash=txid1, idx=1, ref_type=int(PushdataMatchFlags.OUTPUT))
        row2 = PushdataRow(pushdata_hash=pushdata_hash2, tx_hash=txid2, idx=2, ref_type=int(PushdataMatchFlags.OUTPUT))

        # Empty
        if db.db_type == DatabaseType.ScyllaDB:
            result = db.session.execute("SELECT pushdata_hash, tx_hash, idx, ref_type FROM pushdata")
            rows = result.all()
            assert len(rows) == 0
        else:
            db.conn.query("SELECT pushdata_hash, tx_hash, idx, ref_type FROM pushdata")
            result = db.conn.store_result()
            rows = result.fetch_row(0)
            assert len(rows) == 0

        # Filled
        db.bulk_load_pushdata_rows(pd_rows=[row1, row2])
        if db.db_type == DatabaseType.ScyllaDB:
            result = db.session.execute("SELECT pushdata_hash, tx_hash, idx, ref_type FROM pushdata")
            rows = result.all()
            rows.sort()
        else:
            db.conn.query("SELECT pushdata_hash, tx_hash, idx, ref_type FROM pushdata")
            result = db.conn.store_result()
            rows = result.fetch_row(0)
        assert rows[0][0].hex() == pushdata_hash1
        assert rows[0][1].hex() == txid1
        assert rows[0][2] == 1
        assert rows[0][3] == int(PushdataMatchFlags.OUTPUT)
        assert rows[1][0].hex() == pushdata_hash2
        assert rows[1][1].hex() == txid2
        assert rows[1][2] == 2
        assert rows[1][3] == int(PushdataMatchFlags.OUTPUT)

    def test_headers_table(self, db: DBInterface) -> None:
        db.bulk_load_headers(block_header_rows=[])

        block_hash1 = 'aa' * 32
        block_hash2 = 'bb' * 32
        block_header1 = 'aa' * 80
        block_header2 = 'bb' * 80

        row1 = BlockHeaderRow(
            block_num=1,
            block_hash=block_hash1,
            block_height=1,
            block_header=block_header1,
            block_tx_count=1,
            block_size=1,
            is_orphaned=1,
        )
        row2 = BlockHeaderRow(
            block_num=2,
            block_hash=block_hash2,
            block_height=2,
            block_header=block_header2,
            block_tx_count=2,
            block_size=2,
            is_orphaned=0,
        )

        # Empty
        if db.db_type == DatabaseType.ScyllaDB:
            result = db.session.execute(
                "SELECT block_num, block_hash, block_height, block_header,"
                "block_tx_count, block_size, is_orphaned FROM headers"
            )
            rows = result.all()
            assert len(rows) == 0
        else:
            db.conn.query(
                "SELECT block_num, block_hash, block_height, block_header,"
                "block_tx_count, block_size, is_orphaned FROM headers"
            )
            result = db.conn.store_result()
            rows = result.fetch_row(0)
            assert len(rows) == 0

        # Filled
        db.bulk_load_headers(block_header_rows=[row1, row2])
        if db.db_type == DatabaseType.ScyllaDB:
            result = db.session.execute(
                "SELECT block_num, block_hash, block_height, block_header,"
                "block_tx_count, block_size, is_orphaned FROM headers"
            )
            rows = result.all()
            rows.sort()
        else:
            db.conn.query(
                "SELECT block_num, block_hash, block_height, block_header,"
                "block_tx_count, block_size, is_orphaned FROM headers"
            )
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

    def test_mempool_table(self, db: DBInterface) -> None:
        db.bulk_load_mempool_tx_rows(tx_rows=[])
        txid1 = 'aa' * 32
        txid2 = 'bb' * 32
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
            rows = list(result.fetch_row(0))
            rows.sort()
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

    def test_temp_orphaned_txs_table(self, db: DBInterface) -> None:
        db.create_permanent_tables()
        db.load_temp_orphaned_tx_hashes(orphaned_tx_hashes=set())
        txid1 = 'aa' * 32
        txid2 = 'bb' * 32
        row1 = bytes.fromhex(txid1)
        row2 = bytes.fromhex(txid2)

        # Empty
        if db.db_type == DatabaseType.ScyllaDB:
            assert db.cache.r.smembers("temp_orphaned_txs") == set()
        else:
            db.conn.query("SELECT * FROM temp_orphaned_txs")
            result = db.conn.store_result()
            rows = result.fetch_row(0)
            assert len(rows) == 0

        # Filled
        db.load_temp_orphaned_tx_hashes(orphaned_tx_hashes={row1, row2})
        if db.db_type == DatabaseType.ScyllaDB:
            assert db.cache.r.smembers("temp_orphaned_txs") == {row1, row2}
        else:
            db.conn.query("SELECT * FROM temp_orphaned_txs")
            result = db.conn.store_result()
            rows = list(result.fetch_row(0))
            rows.sort()
            assert rows[0][0].hex() == txid1, rows
            assert rows[1][0].hex() == txid2, rows

        # DROP
        if db.db_type == DatabaseType.ScyllaDB:
            db.drop_temp_orphaned_txs()
            assert db.cache.r.smembers("temp_orphaned_txs") == set()
        else:
            assert 'temp_orphaned_txs' in [row[0] for row in db.get_tables()]
            db.drop_temp_orphaned_txs()
            assert 'temp_orphaned_txs' not in [row[0] for row in db.get_tables()]

    def test_temp_mempool_removals_table(self, db: DBInterface) -> None:
        db.drop_temp_mempool_removals()
        db.load_temp_mempool_removals(removals_from_mempool=set())
        txid1 = 'aa' * HashXLength
        txid2 = 'bb' * HashXLength
        row1 = bytes.fromhex(txid1)
        row2 = bytes.fromhex(txid2)

        # Empty
        if db.db_type == DatabaseType.ScyllaDB:
            assert db.cache.r.smembers("temp_mempool_removals") == set()
        else:
            db.conn.query("SELECT * FROM temp_mempool_removals")
            result = db.conn.store_result()
            rows = result.fetch_row(0)
            assert len(rows) == 0

        # Filled
        db.load_temp_mempool_removals(removals_from_mempool={row1, row2})
        if db.db_type == DatabaseType.ScyllaDB:
            assert db.cache.r.smembers("temp_mempool_removals") == {row1, row2}
        else:
            db.conn.query("SELECT * FROM temp_mempool_removals")
            result = db.conn.store_result()
            rows = list(result.fetch_row(0))
            rows.sort()
            assert rows[0][0].hex() == txid1, rows
            assert rows[1][0].hex() == txid2, rows

        # DROP
        if db.db_type == DatabaseType.ScyllaDB:
            db.drop_temp_mempool_removals()
            assert db.cache.r.smembers("temp_mempool_removals") == set()
        else:
            assert 'temp_mempool_removals' in [row[0] for row in db.get_tables()]
            db.drop_temp_mempool_removals()
            assert 'temp_mempool_removals' not in [row[0] for row in db.get_tables()]

    def test_temp_mempool_additions_table(self, db: DBInterface) -> None:
        db.drop_temp_mempool_additions()
        db.load_temp_mempool_additions(additions_to_mempool=set())
        txid1 = 'aa' * HashXLength
        txid2 = 'bb' * HashXLength
        row1 = bytes.fromhex(txid1)
        row2 = bytes.fromhex(txid2)

        # Empty
        if db.db_type == DatabaseType.ScyllaDB:
            assert db.cache.r.smembers("temp_mempool_additions") == set()
        else:
            db.conn.query("SELECT * FROM temp_mempool_additions")
            result = db.conn.store_result()
            rows = result.fetch_row(0)
            assert len(rows) == 0

        # Filled
        db.load_temp_mempool_additions(additions_to_mempool={row1, row2})
        if db.db_type == DatabaseType.ScyllaDB:
            assert db.cache.r.smembers("temp_mempool_additions") == {row1, row2}
        else:
            db.conn.query("SELECT * FROM temp_mempool_additions")
            result = db.conn.store_result()
            rows = list(result.fetch_row(0))
            rows.sort()
            assert rows[0][0].hex() == txid1, rows
            assert rows[1][0].hex() == txid2, rows

        # DROP
        if db.db_type == DatabaseType.ScyllaDB:
            db.drop_temp_mempool_additions()
            assert db.cache.r.smembers("temp_mempool_additions") == set()
        else:
            assert 'temp_mempool_additions' in [row[0] for row in db.get_tables()]
            db.drop_temp_mempool_additions()
            assert 'temp_mempool_additions' not in [row[0] for row in db.get_tables()]

    def test_temp_inbound_tx_hashes_table(self, db: DBInterface) -> None:
        worker_id = 1
        inbound_tx_table_name = f'inbound_tx_table_{worker_id}'
        db.drop_temp_inbound_tx_hashes(inbound_tx_table_name)
        db.load_temp_inbound_tx_hashes([], inbound_tx_table_name=inbound_tx_table_name)
        txid1 = 'aa' * 32
        txid2 = 'bb' * 32
        row1 = (txid1,)
        row2 = (txid2,)

        # Empty
        if db.db_type == DatabaseType.ScyllaDB:
            assert db.cache.r.smembers(inbound_tx_table_name) == set()
        else:
            db.conn.query(f"SELECT * FROM {inbound_tx_table_name}")
            result = db.conn.store_result()
            rows = result.fetch_row(0)
            assert len(rows) == 0

        # Filled
        db.load_temp_inbound_tx_hashes(
            inbound_tx_hashes=[row1, row2], inbound_tx_table_name=inbound_tx_table_name
        )
        if db.db_type == DatabaseType.ScyllaDB:
            assert db.cache.r.smembers(inbound_tx_table_name) == {bytes.fromhex(txid1), bytes.fromhex(txid2)}
        else:
            db.conn.query(f"SELECT * FROM {inbound_tx_table_name}")
            result = db.conn.store_result()
            rows = list(result.fetch_row(0))
            rows.sort()
            assert rows[0][0].hex() == txid1, rows
            assert rows[1][0].hex() == txid2, rows

        # DROP
        if db.db_type == DatabaseType.ScyllaDB:
            db.drop_temp_inbound_tx_hashes(inbound_tx_table_name=inbound_tx_table_name)
            assert db.cache.r.smembers(inbound_tx_table_name) == set()
        else:
            assert inbound_tx_table_name in [row[0] for row in db.get_tables()]
            db.drop_temp_inbound_tx_hashes(inbound_tx_table_name=inbound_tx_table_name)
            assert inbound_tx_table_name not in [row[0] for row in db.get_tables()]

    def test_drop_tables(self, db: DBInterface) -> None:
        if db.db_type == DatabaseType.MySQL:
            db.create_permanent_tables()
            sql = """
            SELECT 
                TABLE_NAME, 
                INDEX_NAME, 
                GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS COLUMNS
            FROM 
                INFORMATION_SCHEMA.STATISTICS
            WHERE 
                TABLE_SCHEMA = 'conduitdb'
            GROUP BY 
                TABLE_NAME, 
                INDEX_NAME;
            """
            # Pre-drop
            db.conn.query(sql)
            result = db.conn.store_result()
            rows = list(result.fetch_row(0))
            rows.sort()
            for table_name, index_name, columns in rows:
                print(f"[Pre-drop] Table: {table_name}, Index: {index_name}, Columns: {columns}")
            assert len(rows) != 0
            # Post-drop
            db.drop_tables()
            db.conn.query(sql)
            result = db.conn.store_result()
            rows = list(result.fetch_row(0))
            rows.sort()
            for table_name, index_name, columns in rows:
                print(f"[Post-drop] Table: {table_name}, Index: {index_name}, Columns: {columns}")
            assert len(rows) == 0

    def test_get_header_data(self, db: DBInterface) -> None:
        db.create_permanent_tables()
        block_hash = "aa" * 32
        block_hash_not_exists = b"bb"

        block_header = os.urandom(80).hex()
        row1 = BlockHeaderRow(
            block_num=1,
            block_hash=block_hash,
            block_height=1,
            block_header=block_header,
            block_tx_count=1000,
            block_size=1000000,
            is_orphaned=0,
        )
        db.bulk_load_headers([row1])
        assert db.get_header_data(bytes.fromhex(block_hash), raw_header_data=True) == row1
        assert db.get_header_data(bytes.fromhex(block_hash), raw_header_data=False) == row1._replace(
            block_header=None
        )
        assert db.get_header_data(block_hash_not_exists) is None

    def test_get_transaction_metadata_hashX(self, db: DBInterface) -> None:
        db.drop_tables()
        db.create_permanent_tables()
        txid_reorged_tx = "aa" * HashXLength
        # Two transactions with the same txid . The second block is on the longest chain
        # (height 2 > 1) so orphaned the first block
        txrow1 = ConfirmedTransactionRow(tx_hash=txid_reorged_tx, tx_block_num=1, tx_position=418)
        txrow2 = ConfirmedTransactionRow(tx_hash=txid_reorged_tx, tx_block_num=3, tx_position=369)
        tx_rows = [txrow1, txrow2]

        block_hash1 = "aa" * 32
        block_hash2 = "bb" * 32
        block_hash3 = "cc" * 32
        # block_num=1 and block_num=3 both contain a shared, reorged transaction.
        # The later block is on the longest chain (height 2 > 1) so it orphaned the first block

        # Orphaned chain
        block_header_row1 = BlockHeaderRow(
            block_num=1,
            block_hash=block_hash1,
            block_height=1,
            block_header=os.urandom(80).hex(),
            block_tx_count=1000,
            block_size=1000000,
            is_orphaned=1,
        )

        # Reorg chain
        block_header_row2 = BlockHeaderRow(
            block_num=2,
            block_hash=block_hash2,
            block_height=1,
            block_header=os.urandom(80).hex(),
            block_tx_count=1000,
            block_size=1000000,
            is_orphaned=0,
        )
        block_header_row3 = BlockHeaderRow(
            block_num=3,
            block_hash=block_hash3,
            block_height=2,
            block_header=os.urandom(80).hex(),
            block_tx_count=1000,
            block_size=1000000,
            is_orphaned=0,
        )
        block_header_rows = [block_header_row1, block_header_row2, block_header_row3]

        db.bulk_load_headers(block_header_rows=block_header_rows)
        db.bulk_load_confirmed_tx_rows(tx_rows=tx_rows)

        assert db.get_header_data(bytes.fromhex(block_hash1)) is None  # orphaned is not visible
        assert db.get_header_data(bytes.fromhex(block_hash2)) == block_header_row2
        assert db.get_header_data(bytes.fromhex(block_hash3)) == block_header_row3

        tx_row = db.get_transaction_metadata_hashX(bytes.fromhex(txid_reorged_tx))
        assert tx_row == TxMetadata(
            tx_hashX=b'\xaa\xaa\xaa\xaa\xaa\xaa\xaa\xaa\xaa\xaa\xaa\xaa\xaa\xaa',
            tx_block_num=3,
            tx_position=369,
            block_num=3,
            block_hash=b'\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc'
            b'\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc',
            block_height=2,
        )

    def test_get_pushdata_filter_matches(self, db: DBInterface) -> None:
        db.drop_tables()
        db.create_permanent_tables()
        db.get_pushdata_filter_matches(pushdata_hashXes=[])

        pushdata_hash1 = 'ee' * HashXLength
        pushdata_hash2 = 'ff' * HashXLength
        out_tx_hash_reorged = 'aa' * HashXLength
        out_tx_hash2 = 'bb' * HashXLength
        in_tx_hash_reorged = 'cc' * HashXLength
        in_tx_hash2 = 'dd' * HashXLength

        # Pushdata Rows
        pd_row1 = PushdataRow(pushdata_hash=pushdata_hash1, tx_hash=out_tx_hash_reorged, idx=1,
            ref_type=int(PushdataMatchFlags.OUTPUT))
        pd_row2 = PushdataRow(pushdata_hash=pushdata_hash2, tx_hash=out_tx_hash2, idx=2,
            ref_type=int(PushdataMatchFlags.OUTPUT))
        db.bulk_load_pushdata_rows(pd_rows=[pd_row1, pd_row2])

        # Input Rows
        in_row1 = InputRow(
            out_tx_hash=out_tx_hash_reorged, out_idx=1, in_tx_hash=in_tx_hash_reorged, in_idx=1
        )
        # Removed this input row intentionally to simulate this being a UTXO
        # in_row2 = InputRow(out_tx_hash=out_tx_hash2, out_idx=2, in_tx_hash=in_tx_hash2, in_idx=2)
        db.bulk_load_input_rows(in_rows=[in_row1])

        # Confirmed Transaction Rows
        tx_row1 = ConfirmedTransactionRow(tx_hash=out_tx_hash_reorged, tx_block_num=1, tx_position=418)
        tx_row2 = ConfirmedTransactionRow(tx_hash=out_tx_hash_reorged, tx_block_num=3, tx_position=369)
        tx_row3 = ConfirmedTransactionRow(tx_hash=out_tx_hash2, tx_block_num=3, tx_position=400)
        db.bulk_load_confirmed_tx_rows(tx_rows=[tx_row1, tx_row2, tx_row3])

        # Header Rows
        block_hash1 = "aa" * 32
        block_hash2 = "bb" * 32
        block_hash3 = "cc" * 32
        # Orphaned chain
        block_header_row1 = BlockHeaderRow(
            block_num=1,
            block_hash=block_hash1,
            block_height=1,
            block_header=os.urandom(80).hex(),
            block_tx_count=1000,
            block_size=1000000,
            is_orphaned=1,
        )

        # Reorg chain
        block_header_row2 = BlockHeaderRow(
            block_num=2,
            block_hash=block_hash2,
            block_height=1,
            block_header=os.urandom(80).hex(),
            block_tx_count=1000,
            block_size=1000000,
            is_orphaned=0,
        )
        block_header_row3 = BlockHeaderRow(
            block_num=3,
            block_hash=block_hash3,
            block_height=2,
            block_header=os.urandom(80).hex(),
            block_tx_count=1000,
            block_size=1000000,
            is_orphaned=0,
        )
        block_header_rows = [block_header_row1, block_header_row2, block_header_row3]
        db.bulk_load_headers(block_header_rows)

        matches = db.get_pushdata_filter_matches(pushdata_hashXes=[pushdata_hash1, pushdata_hash2])
        result = [x for x in matches]
        assert result == [
            RestorationFilterQueryResult(
                ref_type=int(PushdataMatchFlags.OUTPUT),
                pushdata_hashX=b'\xee\xee\xee\xee\xee\xee\xee\xee\xee\xee\xee\xee\xee\xee',
                transaction_hash=b'\xaa\xaa\xaa\xaa\xaa\xaa\xaa\xaa\xaa\xaa\xaa\xaa\xaa\xaa',
                spend_transaction_hash=bytes.fromhex(in_tx_hash_reorged),
                transaction_output_index=1,
                spend_input_index=1,
                tx_location=TxLocation(
                    block_hash=b'\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc',
                    block_num=3,
                    tx_position=369,
                ),
            ),
            RestorationFilterQueryResult(
                ref_type=int(PushdataMatchFlags.OUTPUT),
                pushdata_hashX=b'\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff\xff',
                transaction_hash=b'\xbb\xbb\xbb\xbb\xbb\xbb\xbb\xbb\xbb\xbb\xbb\xbb\xbb\xbb',
                spend_transaction_hash=None,
                transaction_output_index=2,
                spend_input_index=MAX_UINT32,
                tx_location=TxLocation(
                    block_hash=b'\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc\xcc',
                    block_num=3,
                    tx_position=400,
                ),
            ),
        ]

    def test_get_unprocessed_txs(self, db: DBInterface) -> None:
        # db.get_unprocessed_txs()
        pass

    def test_invalidate_mempool_rows(self, db: DBInterface) -> None:
        # db.invalidate_mempool_rows()
        pass

    # DB REPAIR / UNDO BLOCKS ABOVE CHECKPOINT
    def test_delete_pushdata_rows(self, db: DBInterface) -> None:
        pass

    def test_delete_output_rows(self, db: DBInterface) -> None:
        pass

    def test_delete_input_rows(self, db: DBInterface) -> None:
        pass

    def test_delete_header_row(self, db: DBInterface) -> None:
        pass
