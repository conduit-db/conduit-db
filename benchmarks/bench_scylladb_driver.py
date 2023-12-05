# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.

import os
from functools import partial
from typing import Iterator

import pytest
from _pytest.fixtures import FixtureRequest
from cassandra.cluster import BatchStatement
from cassandra.concurrent import execute_concurrent_with_args, execute_concurrent
import time

from cassandra.query import BatchType

from conduit_lib.constants import HashXLength
from conduit_lib.database.db_interface.db import DatabaseType, DBInterface
from conduit_lib.database.db_interface.types import ConfirmedTransactionRow, MempoolTransactionRow, \
    MinedTxHashes

N = 100_000


def benchmark(query_function, *args):
    start_time = time.time()
    query_function(*args)
    end_time = time.time()
    return end_time - start_time


def concurrency_with_batch(db: DBInterface, tx_rows: list[ConfirmedTransactionRow],
        batch_size: int=1000, concurrency: int=100, batch_count_max: int=100) -> None:
    batches = [BatchStatement(batch_type=BatchType.UNLOGGED) for _ in range(len(tx_rows) // batch_size)]
    insert_statement = db.session.prepare(
        "INSERT INTO confirmed_transactions (tx_hash, tx_block_num, tx_position) VALUES (?, ?, ?)"
    )

    for i, row in enumerate(tx_rows):
        batches[i // batch_size].add(insert_statement,
            (bytes.fromhex(row.tx_hash), row.tx_block_num, row.tx_position))

    while len(batches) > 0:
        results = execute_concurrent(
            db.session,
            [(batch, ()) for batch in batches[0:batch_count_max]],
            concurrency=concurrency,
            raise_on_first_error=True,
            results_generator=False,
        )
        batches = batches[batch_count_max:]

def pure_concurrency(db: DBInterface, tx_rows: list[ConfirmedTransactionRow],
        concurrency: int=200) -> None:
    insert_statement = db.session.prepare(
        "INSERT INTO confirmed_transactions (tx_hash, tx_block_num, tx_position) VALUES (?, ?, ?)"
    )
    insert_rows = [(bytes.fromhex(row.tx_hash), row.tx_block_num, row.tx_position) for row in
        tx_rows]
    execute_concurrent_with_args(
        db.session,
        insert_statement,
        insert_rows,
        concurrency=concurrency,
        raise_on_first_error=True,
    )


def pure_batching(db: DBInterface, tx_rows: list[ConfirmedTransactionRow],
        batch_size: int=1000) -> None:
    insert_statement = db.session.prepare(
        "INSERT INTO confirmed_transactions (tx_hash, tx_block_num, tx_position) VALUES (?, ?, ?)"
    )
    for i in range(0, len(tx_rows), batch_size):
        batch = BatchStatement()
        for row in tx_rows[i:i+batch_size]:
            batch.add(insert_statement, (bytes.fromhex(row.tx_hash), row.tx_block_num, row.tx_position))
        db.session.execute(batch)


@pytest.fixture(scope="module", params=[DatabaseType.ScyllaDB])
def db(request: FixtureRequest) -> Iterator[DBInterface]:
    db = DBInterface.load_db(worker_id=1, db_type=request.param)
    db.drop_tables()
    db.create_permanent_tables()
    yield db
    db.drop_tables()
    db.close()


@pytest.fixture(scope="module")
def tx_rows(db: DBInterface):
    tx_rows = []
    for i in range(N):
        txid = os.urandom(HashXLength).hex()
        tx_block_num = i
        tx_position = int.from_bytes(os.urandom(4), byteorder='little')
        tx_rows.append(ConfirmedTransactionRow(tx_hash=txid, tx_block_num=tx_block_num,
            tx_position=tx_position))
    return tx_rows


@pytest.fixture(scope="module")
def mempool_tx_rows(db: DBInterface):
    tx_rows = []
    for i in range(N):
        txid = os.urandom(HashXLength).hex()
        tx_rows.append(MempoolTransactionRow(mp_tx_hash=txid))
    return tx_rows


@pytest.fixture(scope="module")
def mined_tx_hashes(db: DBInterface):
    tx_rows = []
    for i in range(N):
        txid = os.urandom(HashXLength).hex()
        tx_rows.append(MinedTxHashes(txid=txid, block_number=i))
    return tx_rows


# This is very slow 3600 rows/sec
# @pytest.mark.parametrize("concurrency", [500])
# def test_pure_concurrency(db: DBInterface, tx_rows: list[ConfirmedTransactionRow], concurrency) -> None:
#     time_interval = benchmark(pure_concurrency, db, tx_rows, concurrency)
#     print(f"Pure Concurrency: {time_interval} seconds")
#     print(f"Pure Concurrency: {N/time_interval} rows/second")
#     if db.db_type == DatabaseType.ScyllaDB:
#         assert db.session is not None
#         result = db.session.execute(
#             "SELECT tx_hash, tx_block_num, tx_position FROM confirmed_transactions"
#         )
#         rows = result.all()
#         assert len(rows) == N


# Decent performance up to over 25000 rows/second on a single core
# @pytest.mark.parametrize("batch_size", [1000, 2000])
# def test_pure_batching(db: DBInterface, tx_rows: list[ConfirmedTransactionRow],
#         batch_size) -> None:
#     time_interval = benchmark(pure_batching, db, tx_rows, batch_size)
#     print(f"Pure Batching: {time_interval} seconds")
#     print(f"Pure Batching: {N/time_interval} rows/second")
#     if db.db_type == DatabaseType.ScyllaDB:
#         assert db.session is not None
#         result = db.session.execute(
#             "SELECT tx_hash, tx_block_num, tx_position FROM confirmed_transactions"
#         )
#         rows = result.all()
#         assert len(rows) == N


# Concurrency of 200 -> 120K rows/second for a single cpu core
# With 8 worker processes this will exceed 960K rows/second which will saturate ScyllaDB
# These numbers are on a laptop with ScyllaDB running in docker for windows behind a NAT which
# disables the shard-awareness which will give poorer performance.
@pytest.mark.parametrize("batch_size", [100])
@pytest.mark.parametrize("concurrency", [100, 200])
@pytest.mark.parametrize("batch_count_max", [100])
def test_concurrency_with_batch(db: DBInterface, tx_rows: list[ConfirmedTransactionRow],
        batch_size, concurrency, batch_count_max) -> None:
    db.drop_tables()
    db.create_permanent_tables()
    time_interval = benchmark(concurrency_with_batch, db, tx_rows, batch_size, concurrency, batch_count_max)
    print(f"Concurrency with Batching: {time_interval} seconds")
    print(f"Concurrency with Batching: {N/time_interval} rows/second")
    if db.db_type == DatabaseType.ScyllaDB:
        assert db.session is not None
        result = db.session.execute(
            "SELECT tx_hash, tx_block_num, tx_position FROM confirmed_transactions"
        )
        rows = result.all()
        assert len(rows) == N


# Bulk load redis mempool set: 1196774.0232689194 rows/second
def test_bulk_load_mempool_tx_rows(db: DBInterface, mempool_tx_rows: list[MempoolTransactionRow]) \
        -> None:
    db.cache.r.flushall()
    func = partial(db.bulk_load_mempool_tx_rows, mempool_tx_rows)
    time_interval = benchmark(func)
    print(f"Bulk load redis mempool set: {time_interval} seconds")
    print(f"Bulk load redis mempool set: {N/time_interval} rows/second")
    assert db.cache.r.scard(b"mempool") == N


# Bulk load redis mempool set: 822769.0822885996 rows/second
def test_bulk_load_temp_mined_tx_hashes(db: DBInterface, mined_tx_hashes: list[MinedTxHashes]) \
        -> None:
    db.cache.r.flushall()
    func = partial(db.load_temp_mined_tx_hashes, mined_tx_hashes)
    time_interval = benchmark(func)
    print(f"Bulk load temp mined tx hashes: {time_interval} seconds")
    print(f"Bulk load temp mined tx hashes: {N/time_interval} rows/second")
    assert db.cache.r.scard("temp_mined_tx_hashes") == N
