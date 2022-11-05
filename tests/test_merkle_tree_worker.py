import json

import MySQLdb.connections
import bitcoinx
import electrumsv_node
from bitcoinx import hash_to_hex_str, hex_str_to_hash
import io
import multiprocessing
import os
from pathlib import Path
import unittest.mock
from unittest.mock import MagicMock

from conduit_lib import LMDB_Database, MySQLDatabase
from conduit_lib.bitcoin_p2p_types import BlockChunkData
from conduit_lib.handlers import pack_block_chunk_message_for_worker
from conduit_lib.types import TxMetadata, BlockHeaderRow, BlockMetadata
from conduit_raw.conduit_raw.aiohttp_api.handlers import _get_tsc_merkle_proof
from conduit_raw.conduit_raw.workers import MTreeCalculator
from .conftest import TEST_RAW_BLOCK_413567
from .data.block413567_offsets import TX_OFFSETS

from conduit_lib.algorithms import unpack_varint, preprocessor



MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
os.environ['GENESIS_ACTIVATION_HEIGHT'] = "0"

LMDB_STORAGE_PATH = MODULE_DIR / "test_db"
RAW_BLOCKS_LOCKFILE = os.environ['RAW_BLOCKS_LOCKFILE'] = "raw_blocks_ffdb.lock"
MERKLE_TREES_LOCKFILE = os.environ['MERKLE_TREES_LOCKFILE'] = "merkle_trees_ffdb.lock"
TX_OFFSETS_LOCKFILE = os.environ['TX_OFFSETS_LOCKFILE'] = "tx_offsets_ffdb.lock"
RAW_BLOCKS_DIR = Path(os.environ["RAW_BLOCKS_DIR"])
MERKLE_TREES_DIR = Path(os.environ["MERKLE_TREES_DIR"])
TX_OFFSETS_DIR = Path(os.environ["TX_OFFSETS_DIR"])
os.environ["TEMP_FILES_DIR"] = "temp_files_dir"

# BITCOINX
full_block = TEST_RAW_BLOCK_413567
tx_count, offset = unpack_varint(full_block[80:89], 0)
adjustment = 80 + offset
bitcoinx_tx_offsets = [adjustment]
BITCOINX_TX_HASHES = []
stream = io.BytesIO(full_block[80 + offset:])
for i in range(tx_count):
    tx = bitcoinx.Tx.read(stream.read)
    offset += (len(tx.to_bytes()))
    bitcoinx_tx_offsets.append(offset)
    BITCOINX_TX_HASHES.append(tx.hash())

assert len(bitcoinx_tx_offsets) == tx_count + 1
assert len(BITCOINX_TX_HASHES) == tx_count


def test_preprocessor_whole_block_as_a_single_chunk():
    worker_ack_queue_mtree = multiprocessing.Queue()
    worker = MTreeCalculator(worker_id=1, worker_ack_queue_mtree=worker_ack_queue_mtree)
    lmdb = LMDB_Database(storage_path=str(LMDB_STORAGE_PATH))
    mock_mysql_conn = unittest.mock.Mock(MySQLdb.connections.Connection)
    mysql_db: MySQLDatabase = MySQLDatabase(mock_mysql_conn, worker_id=1)
    full_block = bytearray(TEST_RAW_BLOCK_413567)
    tx_count, offset = unpack_varint(full_block[80:89], 0)
    assert tx_count == 1557
    raw_header = full_block[0:80]
    block_hash = bitcoinx.double_sha256(raw_header)
    block_hash_hex = hash_to_hex_str(block_hash)
    assert block_hash_hex == "0000000000000000025aff8be8a55df8f89c77296db6198f272d6577325d4069"
    block_header_row = BlockHeaderRow(0, block_hash, 413567, raw_header.hex(), tx_count,
        len(full_block), 0)
    mysql_db.api_queries.get_header_data = MagicMock(return_value=block_header_row)
    lmdb.get_block_metadata = MagicMock(return_value=BlockMetadata(len(full_block), tx_count))

    # Whole block as a single chunk
    tx_offsets_all = []
    remainder = b""
    last_tx_offset_in_chunk = None
    num_chunks = 1
    chunk_num = 1
    block_hash = bitcoinx.double_sha256(full_block[0:80])

    block_chunks = []
    for idx, chunk in enumerate([full_block]):
        if idx == 0:
            tx_count, var_int_size = unpack_varint(full_block[80:89], 0)
            adjustment = 0
            offset = 80 + var_int_size
        else:
            adjustment = last_tx_offset_in_chunk
            offset = 0
        modified_chunk = remainder + chunk
        tx_offsets_for_chunk, last_tx_offset_in_chunk = preprocessor(modified_chunk,
            offset, adjustment)
        tx_offsets_all.extend(tx_offsets_for_chunk)
        len_slice = last_tx_offset_in_chunk - adjustment
        remainder = modified_chunk[len_slice:]

        # `tx_offsets_for_chunk` corresponds exactly to `slice_for_worker`
        slice_for_worker = modified_chunk[:len_slice]


        block_chunk_data = BlockChunkData(chunk_num, num_chunks, block_hash, slice_for_worker,
            tx_offsets_for_chunk)
        block_chunks.append(block_chunk_data)
    assert tx_offsets_all == TX_OFFSETS
    assert last_tx_offset_in_chunk == len(full_block)

    # Up until here, this was just setup to obtain the block chunks for testing.
    for block_chunk_data in block_chunks:
        packed_msg_for_zmq = pack_block_chunk_message_for_worker(block_chunk_data)
        worker.process_merkle_tree_batch([packed_msg_for_zmq], lmdb)

    ack = worker.worker_ack_queue_mtree.get()
    assert ack == block_hash
    assert len(worker.batched_merkle_trees) == 0
    assert len(worker.batched_acks) == 0
    assert worker.tx_count_map.get(block_hash) is None
    assert worker.tx_hashes_map.get(block_hash) is None

    with open(MODULE_DIR / 'data' / 'block413567_tsc_merkle_proofs', 'r') as file:
        data = file.read()
        CORRECT_MERKLE_PROOF_MAP = json.loads(data)

    for idx, tx_hash in enumerate(BITCOINX_TX_HASHES):
        tx_metadata = TxMetadata(tx_hashX=tx_hash[0:14], tx_block_num=0, tx_position=idx,
            block_num=0, block_hash=block_hash, block_height=413567)
        result = _get_tsc_merkle_proof(tx_metadata, mysql_db, lmdb, include_full_tx=False,
            target_type='hash')
        txid = hash_to_hex_str(tx_hash)
        tsc_merkle_proof = CORRECT_MERKLE_PROOF_MAP[txid]
        assert result == tsc_merkle_proof


def test_preprocessor_with_block_divided_into_four_chunks():
    worker_ack_queue_mtree = multiprocessing.Queue()
    worker = MTreeCalculator(worker_id=1, worker_ack_queue_mtree=worker_ack_queue_mtree)
    lmdb = LMDB_Database(storage_path=str(LMDB_STORAGE_PATH))
    mock_mysql_conn = unittest.mock.Mock(MySQLdb.connections.Connection)
    mysql_db: MySQLDatabase = MySQLDatabase(mock_mysql_conn, worker_id=1)
    full_block = bytearray(TEST_RAW_BLOCK_413567)
    tx_count, offset = unpack_varint(full_block[80:89], 0)
    assert tx_count == 1557
    raw_header = full_block[0:80]
    block_hash = bitcoinx.double_sha256(raw_header)
    block_hash_hex = hash_to_hex_str(block_hash)
    assert block_hash_hex == "0000000000000000025aff8be8a55df8f89c77296db6198f272d6577325d4069"
    block_header_row = BlockHeaderRow(0, block_hash, 413567, raw_header.hex(), tx_count,
        len(full_block), 0)
    mysql_db.api_queries.get_header_data = MagicMock(return_value=block_header_row)
    lmdb.get_block_metadata = MagicMock(return_value=BlockMetadata(len(full_block), tx_count))


    # Same block processed in 4 chunks
    chunks = [
        full_block[0:250_000],
        full_block[250_000:500_000],
        full_block[500_000:750_000],
        full_block[750_000:]
    ]

    tx_offsets_all = []
    remainder = b""
    last_tx_offset_in_chunk = None
    num_chunks = len(chunks)
    for idx, chunk in enumerate(chunks):
        chunk_num = idx + 1
        if idx == 0:
            tx_count, var_int_size = unpack_varint(full_block[80:89], 0)
            adjustment = 0
            offset = 80 + var_int_size
        else:
            offset = 0
            adjustment = last_tx_offset_in_chunk
        modified_chunk = remainder + chunk
        tx_offsets_for_chunk, last_tx_offset_in_chunk = preprocessor(modified_chunk,
            offset, adjustment)
        tx_offsets_all.extend(tx_offsets_for_chunk)
        len_slice = last_tx_offset_in_chunk - adjustment
        remainder = modified_chunk[len_slice:]

        # `tx_offsets_for_chunk` corresponds exactly to `slice_for_worker`
        slice_for_worker = modified_chunk[:len_slice]


        block_chunk_data = BlockChunkData(chunk_num, num_chunks, block_hash, slice_for_worker,
            tx_offsets_for_chunk)

        packed_msg_for_zmq = pack_block_chunk_message_for_worker(block_chunk_data)
        worker.process_merkle_tree_batch([packed_msg_for_zmq], lmdb)

        if idx == 0:
            assert tx_offsets_for_chunk[0] == 83
            assert tx_offsets_for_chunk[-1] == 248948
        if idx == 1:
            assert tx_offsets_for_chunk[0] == 249138
            assert tx_offsets_for_chunk[-1] == 423160
        if idx == 2:
            assert tx_offsets_for_chunk[0] == 482309
            assert tx_offsets_for_chunk[-1] == 749719
        if idx == 3:
            assert tx_offsets_for_chunk[0] == 749909
            assert tx_offsets_for_chunk[-1] == 999367
            assert last_tx_offset_in_chunk == 999887
            assert last_tx_offset_in_chunk == len(TEST_RAW_BLOCK_413567)

    assert len(tx_offsets_all) == 1557
    assert tx_offsets_all == TX_OFFSETS

    ack = worker.worker_ack_queue_mtree.get()
    assert ack == block_hash
    assert len(worker.batched_merkle_trees) == 0
    assert len(worker.batched_acks) == 0
    assert worker.tx_count_map.get(block_hash) is None
    assert worker.tx_hashes_map.get(block_hash) is None

    with open(MODULE_DIR / 'data' / 'block413567_tsc_merkle_proofs', 'r') as file:
        data = file.read()
        CORRECT_MERKLE_PROOF_MAP = json.loads(data)

    for idx, tx_hash in enumerate(BITCOINX_TX_HASHES):
        tx_metadata = TxMetadata(tx_hashX=tx_hash[0:14], tx_block_num=0, tx_position=idx,
            block_num=0, block_hash=block_hash, block_height=413567)
        result = _get_tsc_merkle_proof(tx_metadata, mysql_db, lmdb, include_full_tx=False,
            target_type='hash')
        txid = hash_to_hex_str(tx_hash)
        tsc_merkle_proof = CORRECT_MERKLE_PROOF_MAP[txid]
        assert result == tsc_merkle_proof
