import array
import asyncio
import os
import shutil
import threading
import time
from pathlib import Path

from bitcoinx import double_sha256

from conduit_lib.conduit_raw_api_client import ConduitRawAPIClient
from conduit_lib.database.lmdb.lmdb_database import LMDB_Database
from conduit_raw.conduit_raw.grpc_server.grpc_server import run_server_thread


MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
LMDB_STORAGE_PATH = MODULE_DIR / "test_db"


class TestLMDBDatabase:

    loop = asyncio.get_event_loop()
    threading.Thread(target=run_server_thread, args=(loop, LMDB_STORAGE_PATH), daemon=True).start()

    def setup_class(self):
        self.lmdb = LMDB_Database(storage_path=str(LMDB_STORAGE_PATH))
        self.grpc_client = ConduitRawAPIClient()

    def teardown_class(self):
        self.grpc_client.stop()  # stops server
        time.sleep(0.5)  # allow time for server to stop (and close lmdb handle)
        self.grpc_client.close()  # closes client channel
        self.lmdb.close()
        if os.path.exists(LMDB_STORAGE_PATH):
            shutil.rmtree(LMDB_STORAGE_PATH)

    def test_block_storage(self):

        expected_block_num = 1
        expected_block_size = 128
        block_hash = bytes.fromhex("ff" * 64)
        expected_raw_block = bytes.fromhex("ff" * 128)
        start_block_offset = 0
        end_block_offset = 128

        batched_blocks = [(block_hash, start_block_offset, end_block_offset)]
        self.lmdb.put_blocks(batched_blocks, memoryview(expected_raw_block))

        actual_block_num = self.lmdb.get_block_num(block_hash)
        actual_grpc_block_num = self.grpc_client.get_block_num(block_hash)
        assert expected_block_num == actual_block_num
        assert expected_block_num == actual_grpc_block_num

        actual_block_size = self.lmdb.get_block_metadata(block_hash)
        actual_grpc_block_size = self.grpc_client.get_block_metadata(block_hash)
        assert expected_block_size == actual_block_size
        assert expected_block_size == actual_grpc_block_size

        actual_raw_block = self.lmdb.get_block(expected_block_num)
        actual_grpc_raw_block = self.grpc_client.get_block(expected_block_num)
        assert expected_raw_block == actual_raw_block
        assert expected_raw_block == actual_grpc_raw_block

    def test_merkle_tree_storage(self):

        block_hash = bytes.fromhex("ff" * 64)
        tx1 = bytes.fromhex("aa" * 32)
        tx2 = bytes.fromhex("bb" * 32)
        base_level = [tx1, tx2]
        merkle_root = double_sha256(base_level[0] + base_level[1])
        merkle_tree = {1: base_level, 0: [merkle_root]}
        self.lmdb.put_merkle_tree(block_hash, merkle_tree)

        expected_base_level = tx1 + tx2
        expected_merkle_root = merkle_root

        actual_base_level = self.lmdb.get_mtree_row(block_hash, 1)
        actual_grpc_base_level = self.grpc_client.get_mtree_row(block_hash, 1)
        assert expected_base_level == actual_base_level
        assert expected_base_level == actual_grpc_base_level

        actual_merkle_root = self.lmdb.get_mtree_row(block_hash, 0)
        actual_grpc_merkle_root = self.grpc_client.get_mtree_row(block_hash, 0)
        assert expected_merkle_root == actual_merkle_root
        assert expected_merkle_root == actual_grpc_merkle_root

    def test_tx_offset_storage(self):

        block_hash = bytes.fromhex("ff" * 64)
        tx_offsets = array.array("Q", [1, 2, 3])
        self.lmdb.put_tx_offsets(block_hash, tx_offsets)

        actual_tx_offsets = self.lmdb.get_tx_offsets(block_hash)
        actual_grpc_tx_offsets = self.grpc_client.get_tx_offsets(block_hash)
        assert actual_tx_offsets == tx_offsets
        assert actual_grpc_tx_offsets == tx_offsets
