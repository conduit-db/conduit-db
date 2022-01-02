import array
import asyncio
import os
import shutil
import struct
import threading
import time
from pathlib import Path

import bitcoinx
from bitcoinx import double_sha256


from conduit_lib.ipc_sock_client import IPCSocketClient
from conduit_lib.database.lmdb.lmdb_database import LMDB_Database
from conduit_lib.types import BlockSliceRequestType, Slice, TxLocation
from conduit_raw.conduit_raw.sock_server.ipc_sock_server import ThreadedTCPServer, \
    ThreadedTCPRequestHandler
from tests.data.offsets import TX_OFFSETS

MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
LMDB_STORAGE_PATH = MODULE_DIR / "test_db"


def ipc_sock_server_thread():
    from conduit_lib.store import setup_headers_store
    from conduit_lib.networks import RegTestNet
    HOST, PORT = "127.0.0.1", 34586
    block_headers = setup_headers_store(RegTestNet(), "test_headers.mmap")
    block_headers_lock = threading.RLock()
    ipc_sock_server = ThreadedTCPServer(addr=(HOST, PORT),
        handler=ThreadedTCPRequestHandler, storage_path=LMDB_STORAGE_PATH,
        block_headers=block_headers, block_headers_lock=block_headers_lock)
    ipc_sock_server.serve_forever()


class TestLMDBDatabase:

    def setup_class(self):
        self.lmdb = LMDB_Database(storage_path=str(LMDB_STORAGE_PATH))
        self.ipc_sock_server_thread = threading.Thread(target=ipc_sock_server_thread)
        self.ipc_sock_server_thread.start()
        os.environ['CONDUIT_RAW_API_HOST'] = 'localhost'
        os.environ['CONDUIT_RAW_API_PORT'] = '34586'
        self.ipc_sock_client = IPCSocketClient()

    def teardown_class(self):
        self.ipc_sock_client.stop()
        time.sleep(5)  # allow time for server to stop (and close lmdb handle)
        self.ipc_sock_client.close()  # closes client channel
        self.lmdb.close()

        if os.path.exists(LMDB_STORAGE_PATH):
            shutil.rmtree(LMDB_STORAGE_PATH)

    def test_block_storage(self):
        with open(MODULE_DIR / "data/block413567.raw", "rb") as f:
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

        # TODO - test writing multiple blocks in the batch
        #  (which will be added to the same .dat file) - this complicates the offset calculations
        #  So needs tests to ensure no regressions

    def test_merkle_tree_storage(self):
        block_hash = bytes.fromhex("ff" * 32)
        tx1 = bytes.fromhex("aa" * 32)
        tx2 = bytes.fromhex("bb" * 32)
        tx3 = bytes.fromhex("cc" * 32)
        base_level = [tx1, tx2, tx3, tx3]
        middle_level = [double_sha256(base_level[0] + base_level[1]), double_sha256(base_level[2] + base_level[2])]
        merkle_root = double_sha256(middle_level[0] + middle_level[1])
        merkle_tree = {2: base_level, 1: middle_level, 0: [merkle_root]}

        write_batch = [(block_hash, merkle_tree, len(base_level))]
        self.lmdb.put_merkle_trees(write_batch)

        expected_base_level = tx1 + tx2 + tx3 + tx3
        expected_mid_level = middle_level[0] + middle_level[1]
        expected_merkle_root = merkle_root

        actual_base_level = self.lmdb.get_mtree_row(block_hash, 2)
        actual_ipc_base_level = self.ipc_sock_client.merkle_tree_row(block_hash, 2).mtree_row
        assert actual_base_level == expected_base_level
        assert actual_ipc_base_level == expected_base_level

        actual_mid_level = self.lmdb.get_mtree_row(block_hash, 1)
        actual_ipc_mid_level = self.ipc_sock_client.merkle_tree_row(block_hash, 1).mtree_row
        assert actual_mid_level == expected_mid_level
        assert actual_ipc_mid_level == expected_mid_level

        actual_merkle_root = self.lmdb.get_mtree_row(block_hash, 0)
        actual_ipc_merkle_root = self.ipc_sock_client.merkle_tree_row(block_hash, 0).mtree_row
        assert expected_merkle_root == actual_merkle_root
        assert expected_merkle_root == actual_ipc_merkle_root

        array = self.lmdb.get_merkle_tree_data(block_hash, start_offset=0, end_offset=0)
        assert array == expected_base_level + expected_mid_level + expected_merkle_root

        merkle_root = self.lmdb.get_merkle_tree_data(block_hash,
            start_offset=len(expected_base_level + expected_mid_level),
            end_offset=len(expected_base_level + expected_mid_level + expected_merkle_root))
        assert expected_merkle_root == merkle_root

        with self.lmdb.env.begin(db=self.lmdb.mtree_db, write=False, buffers=True) as txn:
            cursor = txn.cursor()
            mtree_merkle_root_node = self.lmdb.get_mtree_node(block_hash, 0, position=0, cursor=cursor)
            assert expected_merkle_root == mtree_merkle_root_node

            left_mid_node = self.lmdb.get_mtree_node(block_hash, 1, position=0, cursor=cursor)
            assert expected_mid_level[0:32] == left_mid_node

            right_mid_node = self.lmdb.get_mtree_node(block_hash, 1, position=1, cursor=cursor)
            assert expected_mid_level[32:64] == right_mid_node

            zeroth_base_node = self.lmdb.get_mtree_node(block_hash, 2, position=0, cursor=cursor)
            assert expected_base_level[0:32] == zeroth_base_node

            first_index_base_node = self.lmdb.get_mtree_node(block_hash, 2, position=1, cursor=cursor)
            assert expected_base_level[32:64] == first_index_base_node

            second_index_base_node = self.lmdb.get_mtree_node(block_hash, 2, position=2, cursor=cursor)
            assert expected_base_level[64:96] == second_index_base_node

            third_index_base_node = self.lmdb.get_mtree_node(block_hash, 2, position=3, cursor=cursor)
            assert expected_base_level[96:128] == third_index_base_node

            # TODO - test writing multiple merkle trees in the batch
            #  (to be added to the same .dat file) - this complicates the offset calculations
            #  So needs tests to ensure no regressions


    def test_tx_offset_storage(self):
        block_hash = bytes.fromhex("ff" * 64)
        tx_offsets = array.array("Q", [1, 2, 3])
        batched_tx_offsets = [(block_hash, tx_offsets)]
        self.lmdb.put_tx_offsets(batched_tx_offsets)

        actual_tx_offsets = self.lmdb.get_tx_offsets(block_hash)
        assert tx_offsets.tobytes() == actual_tx_offsets
        actual_grpc_tx_offsets = self.ipc_sock_client.transaction_offsets_batched([block_hash])
        for tx_os in actual_grpc_tx_offsets:
            assert tx_os == tx_offsets

        # Block 413567 on mainnet
        block_hash = bitcoinx.hex_str_to_hash(
            "0000000000000000025aff8be8a55df8f89c77296db6198f272d6577325d4069")
        tx_offsets = array.array('Q', TX_OFFSETS)
        self.lmdb.put_tx_offsets([(block_hash, tx_offsets)])
        block_num = self.lmdb.get_block_num(block_hash)
        tx_loc = TxLocation(block_hash=block_hash, block_num=block_num, tx_position=0)
        coinbase_start_offset, coinbase_end_offset = self.lmdb.get_single_tx_offset(tx_loc)
        assert coinbase_start_offset == 83
        assert coinbase_end_offset == 268

        tx_loc = TxLocation(block_hash=block_hash, block_num=block_num, tx_position=1)
        coinbase_start_offset, coinbase_end_offset = self.lmdb.get_single_tx_offset(tx_loc)
        assert coinbase_start_offset == 268
        assert coinbase_end_offset == 494

        tx_loc = TxLocation(block_hash=block_hash, block_num=block_num, tx_position=2)
        coinbase_start_offset, coinbase_end_offset = self.lmdb.get_single_tx_offset(tx_loc)
        assert coinbase_start_offset == 494
        assert coinbase_end_offset == 720
