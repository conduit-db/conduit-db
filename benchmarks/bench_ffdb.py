# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.

import logging
import os
import shutil
from pathlib import Path
from typing import Iterator

import pytest
from _pytest.fixtures import FixtureRequest

from conduit_lib.database.ffdb.flat_file_db import FlatFileDb
from conduit_lib.utils import remove_readonly, Timer

MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
logger = logging.getLogger("test-flat-file-db")
TEST_DATADIR = str(MODULE_DIR / "test_path")
FFDB_LOCKFILE = os.environ["FFDB_LOCKFILE"] = "ffdb.lock"

os.makedirs(TEST_DATADIR, exist_ok=True)


class TestFlatFileDb:

    @pytest.fixture(scope="class", params=[False, True])
    def use_compression(self, request: FixtureRequest) -> Iterator[bool]:
        yield request.param
        if os.path.exists(TEST_DATADIR):
            shutil.rmtree(TEST_DATADIR, onerror=remove_readonly)

        if os.path.exists(FFDB_LOCKFILE):
            os.remove(FFDB_LOCKFILE)

    def test_small_data_puts_and_gets(self, use_compression: bool) -> None:
        ffdb = FlatFileDb(
            datadir=Path(TEST_DATADIR),
            mutable_file_lock_path=Path(os.environ["FFDB_LOCKFILE"]),
            fsync=True,
            use_compression=use_compression
        )
        logging.basicConfig(level=logging.DEBUG)
        # NOTE The use case of a chain indexer does not require fsync because we can always
        # re-sync from the node if we crash...
        COUNT = 1000
        with ffdb:
            entries: list[tuple[DataLocation, bytes]] = []
            with Timer(count=COUNT, name='bench-ffdb-put', units=" puts"):
                uncompressed_start_offset = 0
                for i in range(COUNT):
                    data = os.urandom(1000)
                    data_location = ffdb.put(data)
                    entries.append((data_location, data))
                    uncompressed_start_offset += len(data)

            with Timer(count=COUNT, name='bench-ffdb-get', units=" gets"):
                for data_location, data_expected in entries:
                    data = ffdb.get(data_location, lock_free_access=True)
                    assert data_expected == data

    def test_small_data_puts_and_gets_batched(self, use_compression: bool) -> None:
        ffdb = FlatFileDb(
            datadir=Path(TEST_DATADIR),
            mutable_file_lock_path=Path(os.environ["FFDB_LOCKFILE"]),
            fsync=True,
            use_compression=use_compression
        )
        logging.basicConfig(level=logging.DEBUG)
        # NOTE The use case of a chain indexer does not require fsync because we can always
        # re-sync from the node if we crash...
        COUNT = 100
        with ffdb:
            entries: list[tuple[DataLocation, bytes]] = []
            with Timer(count=COUNT, name='bench-ffdb-put-many', units=" puts"):
                batch = []
                for i in range(COUNT):
                    # data = os.urandom(64*1024)
                    # data = bytes.fromhex("aabbccddee")*2*1024*1024

                    # half-random, half-compressible
                    data = bytes(bytearray.fromhex("aabbccddee") + bytearray(os.urandom(5))*1000)
                    batch.append(data)
                data_locations = ffdb.put_many(batch)

                for data_location, data in zip(data_locations, batch):
                    entries.append((data_location, data))

            with Timer(count=COUNT, name='bench-ffdb-get', units=" gets"):
                for data_location, data_expected in entries:
                    data = ffdb.get(data_location, lock_free_access=True)
                    assert data_expected == data
