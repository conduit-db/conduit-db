import functools
import logging
import multiprocessing
import os
import shutil
from concurrent.futures.thread import ThreadPoolExecutor
from pathlib import Path
import time

import pytest

from conduit_lib.database.ffdb.flat_file_db import FlatFileDb, MAX_DAT_FILE_SIZE
from conduit_lib.types import Slice
from conduit_lib.utils import remove_readonly


MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
logger = logging.getLogger("test-flat-file-db")
TEST_DATADIR = str(MODULE_DIR / "test_path")
FFDB_LOCKFILE = os.environ['FFDB_LOCKFILE'] = "ffdb.lock"

os.makedirs(TEST_DATADIR, exist_ok=True)


def _do_general_read_and_write_ops(ffdb: FlatFileDb):
    logging.basicConfig(level=logging.DEBUG)
    # NOTE The use case of a chain indexer does not require fsync because we can always
    # re-sync from the node if we crash...
    with ffdb:
        data_location_aa = ffdb.put(b"a" * (MAX_DAT_FILE_SIZE // 16))
        # logger.debug(data_location_aa)
        data_location_bb = ffdb.put(b"b" * (MAX_DAT_FILE_SIZE // 16))
        # logger.debug(data_location_bb)

        # Read
        data_aa = ffdb.get(data_location_aa)
        assert data_aa == b"a" * (MAX_DAT_FILE_SIZE // 16), data_aa
        # logger.debug(data_aa)

        data_bb = ffdb.get(data_location_bb)
        assert data_bb == b"b" * (MAX_DAT_FILE_SIZE // 16), data_bb
        # logger.debug(data_bb)

        with ffdb.mutable_file_rwlock.write_lock():
            ffdb._maybe_get_new_mutable_file()

        return ffdb


def test_general_read_and_write_db():
    ffdb = FlatFileDb(
        datadir=Path(TEST_DATADIR),
        mutable_file_lock_path=Path(os.environ['FFDB_LOCKFILE']),
        fsync=True
    )
    _do_general_read_and_write_ops(ffdb)


def test_delete():
    with FlatFileDb(
            datadir=Path(TEST_DATADIR),
            mutable_file_lock_path=Path(os.environ['FFDB_LOCKFILE']),
            fsync=True) as ffdb:
        data_location_aa = ffdb.put(b"a" * (MAX_DAT_FILE_SIZE // 16))
        # logger.debug(f"Put to {data_location_aa.file_path}")
        ffdb.delete_file(Path(data_location_aa.file_path))
        # logger.debug(f"Deleted from {data_location_aa.file_path}")
        with pytest.raises(FileNotFoundError):
            ffdb.get(data_location_aa)


def test_slicing():
    with FlatFileDb(
            datadir=Path(TEST_DATADIR),
            mutable_file_lock_path=Path(os.environ['FFDB_LOCKFILE']),
            fsync=True) as ffdb:
        data_location_mixed = ffdb.put(b"aaaaabbbbbccccc")
        # print(data_location_mixed)

        slice = Slice(start_offset=5, end_offset=10)
        data_bb_slice = ffdb.get(data_location_mixed, slice)
        assert data_bb_slice == b"bbbbb"
        # print(data_bb_slice)


def _task_multithreaded(ffdb: FlatFileDb, x: int) -> None:
    """Need to instantiate and pass into threads"""
    _do_general_read_and_write_ops(ffdb)


def _task(x):
    """Create each FlatFileDb instance anew within each process"""
    ffdb = FlatFileDb(
        datadir=Path(TEST_DATADIR),
        mutable_file_lock_path=Path(os.environ['FFDB_LOCKFILE']),
        fsync=True
    )
    _do_general_read_and_write_ops(ffdb)


def test_multithreading_access():
    starttime = time.time()
    ffdb = FlatFileDb(
        datadir=Path(TEST_DATADIR),
        mutable_file_lock_path=Path(os.environ['FFDB_LOCKFILE']),
        fsync=True
    )
    func = functools.partial(_task_multithreaded, ffdb)
    with ThreadPoolExecutor(4) as pool:
        for result in pool.map(func, range(0, 32)):
            if result:
                logger.debug(result)
    endtime = time.time()
    logger.debug(f"Time taken {endtime - starttime} seconds")


def test_multiprocessing_access():
    starttime = time.time()
    with multiprocessing.Pool(4) as pool:
        pool.map(_task, range(0, 32))

    endtime = time.time()
    logger.debug(f"Time taken {endtime - starttime} seconds")


def test_cleanup():
    if os.path.exists(TEST_DATADIR):
        shutil.rmtree(TEST_DATADIR, onerror=remove_readonly)

    if os.path.exists(FFDB_LOCKFILE):
        os.remove(FFDB_LOCKFILE)
