import json
import shutil
from modulefinder import Module
from typing import Callable, Iterator

import bitcoinx
import pytest
from bitcoinx import hash_to_hex_str
import io
import multiprocessing
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

from pytest_asyncio.plugin import FixtureFunction

from conduit_lib import LMDB_Database, DBInterface
from conduit_lib.bitcoin_p2p_types import BlockChunkData
from conduit_lib.database.mysql.db import MySQLDatabase
from conduit_lib.database.scylladb.db import ScyllaDB
from conduit_lib.handlers import pack_block_chunk_message_for_worker
from conduit_lib.types import TxMetadata, BlockHeaderRow, BlockMetadata
from conduit_lib.utils import remove_readonly
from conduit_raw.conduit_raw.aiohttp_api.handlers_restoration import (
    _get_tsc_merkle_proof,
)
from conduit_raw.conduit_raw.workers import MTreeCalculator
from conduit_raw.conduit_raw.workers.merkle_tree import (
    process_merkle_tree_batch,
)
from .conftest import TEST_RAW_BLOCK_413567
from .data.block413567_offsets import TX_OFFSETS

from conduit_lib.algorithms import unpack_varint, preprocessor


MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
os.environ["GENESIS_ACTIVATION_HEIGHT"] = "0"
DATADIR_HDD = os.environ["DATADIR_HDD"] = str(MODULE_DIR / "test_datadir_hdd")
DATADIR_SSD = os.environ["DATADIR_SSD"] = str(MODULE_DIR / "test_datadir_ssd")

os.environ['DEFAULT_DB_TYPE'] = os.getenv('DEFAULT_DB_TYPE', 'SCYLLADB')

# BITCOINX
full_block_bx = TEST_RAW_BLOCK_413567
tx_count_bx, offset_bx = unpack_varint(full_block_bx[80:89], 0)
adjustment_bx = 80 + offset_bx
bitcoinx_tx_offsets = [adjustment_bx]
BITCOINX_TX_HASHES = []
stream = io.BytesIO(full_block_bx[80 + offset_bx :])
for i in range(tx_count_bx):
    tx = bitcoinx.Tx.read(stream.read)
    offset_bx += len(tx.to_bytes())
    bitcoinx_tx_offsets.append(offset_bx)
    BITCOINX_TX_HASHES.append(tx.hash())

assert len(bitcoinx_tx_offsets) == tx_count_bx + 1
assert len(BITCOINX_TX_HASHES) == tx_count_bx


def teardown_module(module: Module) -> None:
    if Path(DATADIR_HDD).exists():
        shutil.rmtree(DATADIR_HDD, onerror=remove_readonly)
    if Path(DATADIR_SSD).exists():
        shutil.rmtree(DATADIR_SSD, onerror=remove_readonly)


@pytest.fixture
def mock_get_header_data() -> Iterator[MagicMock]:
    full_block = bytearray(TEST_RAW_BLOCK_413567)
    tx_count, offset = unpack_varint(full_block[80:89], 0)
    assert tx_count == 1557
    raw_header = full_block[0:80]
    block_hash = bitcoinx.double_sha256(raw_header)
    block_hash_hex = hash_to_hex_str(block_hash)
    assert block_hash_hex == "0000000000000000025aff8be8a55df8f89c77296db6198f272d6577325d4069"
    block_header_row = BlockHeaderRow(0, block_hash, 413567, raw_header.hex(), tx_count, len(full_block), 0)
    # Note: We must patch the concrete implementation `MySQLDatabase` not the interface DBInterface
    if os.environ['DEFAULT_DB_TYPE'] == 'MYSQL':
        with patch.object(MySQLDatabase, 'get_header_data') as mock:
            mock.return_value = block_header_row
            yield mock
    elif os.environ['DEFAULT_DB_TYPE'] == 'SCYLLADB':
        with patch.object(ScyllaDB, 'get_header_data') as mock:
            mock.return_value = block_header_row
            yield mock
    else:
        raise ValueError(f"Unsupported DB type: {os.environ['DEFAULT_DB_TYPE']}")

@pytest.fixture
def mock_get_block_metadata() -> Iterator[MagicMock]:
    full_block = bytearray(TEST_RAW_BLOCK_413567)
    tx_count, offset = unpack_varint(full_block[80:89], 0)
    assert tx_count == 1557
    with patch.object(LMDB_Database, 'get_block_metadata') as mock:
        mock.return_value = BlockMetadata(len(full_block), tx_count)
        yield mock


@pytest.fixture
def lmdb() -> LMDB_Database:
    lmdb = LMDB_Database(lock=True)
    yield lmdb
    lmdb.close()


def test_preprocessor_whole_block_as_a_single_chunk(
    mock_get_header_data: Iterator[MagicMock],
    mock_get_block_metadata: Iterator[MagicMock],
    lmdb: LMDB_Database
) -> None:
    worker_ack_queue_mtree: multiprocessing.Queue[bytes] = multiprocessing.Queue()
    worker = MTreeCalculator(worker_id=1, worker_ack_queue_mtree=worker_ack_queue_mtree)
    db: DBInterface = DBInterface.load_db(worker_id=1)
    full_block = bytearray(TEST_RAW_BLOCK_413567)
    tx_count, offset = unpack_varint(full_block[80:89], 0)
    assert tx_count == 1557
    raw_header = full_block[0:80]
    block_hash = bitcoinx.double_sha256(raw_header)
    block_hash_hex = hash_to_hex_str(block_hash)
    assert block_hash_hex == "0000000000000000025aff8be8a55df8f89c77296db6198f272d6577325d4069"

    # Whole block as a single chunk
    tx_offsets_all: list[int] = []
    remainder = b""
    last_tx_offset_in_chunk = 0
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
        tx_offsets_for_chunk, last_tx_offset_in_chunk = preprocessor(modified_chunk, offset, adjustment)
        tx_offsets_all.extend(tx_offsets_for_chunk)
        len_slice = last_tx_offset_in_chunk - adjustment
        remainder = modified_chunk[len_slice:]

        # `tx_offsets_for_chunk` corresponds exactly to `slice_for_worker`
        slice_for_worker = modified_chunk[:len_slice]

        block_chunk_data = BlockChunkData(
            chunk_num,
            num_chunks,
            block_hash,
            slice_for_worker,
            tx_offsets_for_chunk,
        )
        block_chunks.append(block_chunk_data)
    assert tx_offsets_all == TX_OFFSETS
    assert last_tx_offset_in_chunk == len(full_block)

    # Up until here, this was just setup to obtain the block chunks for testing.
    for block_chunk_data in block_chunks:
        packed_msg_for_zmq = pack_block_chunk_message_for_worker(block_chunk_data)
        process_merkle_tree_batch(worker, [packed_msg_for_zmq], lmdb)

    ack = worker.worker_ack_queue_mtree.get()
    assert ack == block_hash
    assert len(worker.batched_merkle_trees) == 0
    assert len(worker.batched_acks) == 0
    assert worker.tx_count_map.get(block_hash) is None
    assert worker.tx_hashes_map.get(block_hash) is None

    with open(MODULE_DIR / "data" / "block413567_tsc_merkle_proofs", "r") as file:
        data = file.read()
        CORRECT_MERKLE_PROOF_MAP = json.loads(data)

    for idx, tx_hash in enumerate(BITCOINX_TX_HASHES):
        tx_metadata = TxMetadata(
            tx_hashX=tx_hash[0:14],
            tx_block_num=0,
            tx_position=idx,
            block_num=0,
            block_hash=block_hash,
            block_height=413567,
        )
        result = _get_tsc_merkle_proof(
            tx_metadata,
            db,
            lmdb,
            include_full_tx=False,
            target_type="hash",
        )
        txid = hash_to_hex_str(tx_hash)
        tsc_merkle_proof = CORRECT_MERKLE_PROOF_MAP[txid]
        assert result == tsc_merkle_proof

    worker.close()
    del worker


def test_preprocessor_with_block_divided_into_four_chunks(
        mock_get_header_data: MagicMock,
        mock_get_block_metadata: MagicMock,
        lmdb: LMDB_Database
) -> None:
    worker_ack_queue_mtree: multiprocessing.Queue[bytes] = multiprocessing.Queue()
    worker = MTreeCalculator(worker_id=1, worker_ack_queue_mtree=worker_ack_queue_mtree)
    db: DBInterface = DBInterface.load_db(worker_id=1)
    full_block = bytearray(TEST_RAW_BLOCK_413567)
    tx_count, offset = unpack_varint(full_block[80:89], 0)
    assert tx_count == 1557
    raw_header = full_block[0:80]
    block_hash = bitcoinx.double_sha256(raw_header)
    block_hash_hex = hash_to_hex_str(block_hash)
    assert block_hash_hex == "0000000000000000025aff8be8a55df8f89c77296db6198f272d6577325d4069"
    block_header_row = BlockHeaderRow(0, block_hash, 413567, raw_header.hex(), tx_count, len(full_block), 0)

    mock_get_header_data.return_value = block_header_row
    mock_get_block_metadata.return_value = BlockMetadata(len(full_block), tx_count)

    # Same block processed in 4 chunks
    chunks = [
        full_block[0:250_000],
        full_block[250_000:500_000],
        full_block[500_000:750_000],
        full_block[750_000:],
    ]

    tx_offsets_all: list[int] = []
    remainder = b""
    last_tx_offset_in_chunk = 0
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
        tx_offsets_for_chunk, last_tx_offset_in_chunk = preprocessor(modified_chunk, offset, adjustment)
        tx_offsets_all.extend(tx_offsets_for_chunk)
        len_slice = last_tx_offset_in_chunk - adjustment
        remainder = modified_chunk[len_slice:]

        # `tx_offsets_for_chunk` corresponds exactly to `slice_for_worker`
        slice_for_worker = modified_chunk[:len_slice]

        block_chunk_data = BlockChunkData(
            chunk_num,
            num_chunks,
            block_hash,
            slice_for_worker,
            tx_offsets_for_chunk,
        )

        packed_msg_for_zmq = pack_block_chunk_message_for_worker(block_chunk_data)
        process_merkle_tree_batch(worker, [packed_msg_for_zmq], lmdb)

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

    with open(MODULE_DIR / "data" / "block413567_tsc_merkle_proofs", "r") as file:
        data = file.read()
        CORRECT_MERKLE_PROOF_MAP = json.loads(data)

    for idx, tx_hash in enumerate(BITCOINX_TX_HASHES):
        tx_metadata = TxMetadata(
            tx_hashX=tx_hash[0:14],
            tx_block_num=0,
            tx_position=idx,
            block_num=0,
            block_hash=block_hash,
            block_height=413567,
        )
        result = _get_tsc_merkle_proof(
            tx_metadata,
            db,
            lmdb,
            include_full_tx=False,
            target_type="hash",
        )
        txid = hash_to_hex_str(tx_hash)
        tsc_merkle_proof = CORRECT_MERKLE_PROOF_MAP[txid]
        assert result == tsc_merkle_proof

    worker.close()
    del worker
