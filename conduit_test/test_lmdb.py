import array
import asyncio
import os
import shutil
import struct
import threading
import time
from pathlib import Path

from bitcoinx import double_sha256

from conduit_index.conduit_index.sync_state import CONDUIT_RAW_HOST, CONDUIT_RAW_PORT
from conduit_lib.ipc_sock_client import IPCSocketClient
from conduit_lib.database.lmdb.lmdb_database import LMDB_Database
from conduit_lib.types import BlockSliceRequestType, Slice
from conduit_raw.conduit_raw.sock_server.ipc_sock_server import ThreadedTCPServer, \
    ThreadedTCPRequestHandler

MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
LMDB_STORAGE_PATH = MODULE_DIR / "test_db"


def ipc_sock_server_thread():
    from conduit_lib.store import setup_headers_store
    from conduit_lib.networks import RegTestNet
    HOST, PORT = "127.0.0.1", 34586
    block_headers = setup_headers_store(RegTestNet(), "test_headers.mmap")
    ipc_sock_server = ThreadedTCPServer(addr=(HOST, PORT),
        handler=ThreadedTCPRequestHandler, storage_path=LMDB_STORAGE_PATH,
        block_headers=block_headers)
    ipc_sock_server.serve_forever()


class TestLMDBDatabase:

    def setup_class(self):
        self.lmdb = LMDB_Database(storage_path=str(LMDB_STORAGE_PATH))
        self.ipc_sock_server_thread = threading.Thread(target=ipc_sock_server_thread)
        self.ipc_sock_server_thread.start()
        os.environ['CONDUIT_RAW_API_HOST'] = 'localhost:34586'
        self.ipc_sock_client = IPCSocketClient()

    def teardown_class(self):
        self.ipc_sock_client.stop()
        time.sleep(5)  # allow time for server to stop (and close lmdb handle)
        self.ipc_sock_client.close()  # closes client channel
        self.lmdb.close()

        if os.path.exists(LMDB_STORAGE_PATH):
            shutil.rmtree(LMDB_STORAGE_PATH)

    def test_block_storage(self):
        with open("./data/block413567.raw", "rb") as f:
            raw_block = array.array('B', f.read())

        expected_block_num = 1
        expected_block_size = len(raw_block)
        block_hash = double_sha256(raw_block[0:80])
        expected_raw_block = raw_block
        start_block_offset = 0
        end_block_offset = len(raw_block)

        batched_blocks = [(block_hash, start_block_offset, end_block_offset)]
        self.lmdb.put_blocks(batched_blocks, memoryview(expected_raw_block.tobytes()))

        actual_block_num = self.lmdb.get_block_num(block_hash)
        actual_block_num = self.ipc_sock_client.block_number_batched([block_hash]).block_numbers[0]
        assert expected_block_num == actual_block_num
        assert expected_block_num == actual_block_num

        actual_block_size = self.lmdb.get_block_metadata(block_hash).block_size
        actual_grpc_block_size = self.ipc_sock_client \
                                     .block_metadata_batched([block_hash]) \
                                     .block_metadata_batch[0].block_size
        assert expected_block_size == actual_block_size
        assert expected_block_size == actual_grpc_block_size

        actual_raw_block = self.lmdb.get_block(expected_block_num)
        block_requests: list[BlockSliceRequestType] = list([BlockSliceRequestType(expected_block_num, Slice(0, 0))])
        batched_block_slices = self.ipc_sock_client.block_batched(block_requests)

        block_num, len_slice = struct.unpack_from(f"<IQ", batched_block_slices, 0)
        _block_num, _len_slice, raw_block_slice = struct.unpack_from(f"<IQ{len_slice}s", batched_block_slices, 0)
        actual_grpc_raw_block = raw_block_slice

        assert expected_raw_block.tobytes() == actual_raw_block
        assert expected_raw_block.tobytes() == actual_grpc_raw_block

    def test_merkle_tree_storage(self):

        block_hash = bytes.fromhex("ff" * 32)
        tx1 = bytes.fromhex("aa" * 32)
        tx2 = bytes.fromhex("bb" * 32)
        base_level = [tx1, tx2]
        merkle_root = double_sha256(base_level[0] + base_level[1])
        merkle_tree = {1: base_level, 0: [merkle_root]}
        self.lmdb.put_merkle_tree(block_hash, merkle_tree)

        expected_base_level = tx1 + tx2
        expected_merkle_root = merkle_root

        actual_base_level = self.lmdb.get_mtree_row(block_hash, 1)
        actual_grpc_base_level = self.ipc_sock_client.merkle_tree_row(block_hash, 1).mtree_row
        assert expected_base_level == actual_base_level
        assert expected_base_level == actual_grpc_base_level

        actual_merkle_root = self.lmdb.get_mtree_row(block_hash, 0)
        actual_grpc_merkle_root = self.ipc_sock_client.merkle_tree_row(block_hash, 0).mtree_row
        assert expected_merkle_root == actual_merkle_root
        assert expected_merkle_root == actual_grpc_merkle_root
    #
    # def test_tx_offset_storage(self):
    #
    #     block_hash = bytes.fromhex("ff" * 64)
    #     tx_offsets = array.array("Q", [1, 2, 3])
    #     self.lmdb.put_tx_offsets(block_hash, tx_offsets)
    #
    #     actual_tx_offsets = self.lmdb.get_tx_offsets(block_hash)
    #     actual_grpc_tx_offsets = self.grpc_client.get_tx_offsets(block_hash)
    #     assert actual_tx_offsets == tx_offsets
    #     assert actual_grpc_tx_offsets == tx_offsets
