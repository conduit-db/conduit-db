import io
import os
import shutil
from pathlib import Path

from conduit_lib.database.ffdb.flat_file_db import FlatFileDb
from conduit_lib.types import Slice
from tests.conftest import remove_readonly


def test_general_read_and_write_db():
    rand_dir = os.urandom(4).hex()
    try:
        ffdb = FlatFileDb(Path(rand_dir))
        assert ffdb.file_is_full() is False
        assert ffdb.file_size == 0
        assert ffdb.get_file_size() == 0
        write_path, start, end = ffdb.move_to_next_non_full_write_path()
        assert str(write_path.parts[-2]) == rand_dir
        assert str(write_path.parts[-1]) == 'data_00000000.dat'
        data_location_aa = ffdb.write_from_bytes(b"aa" * 10)
        print(data_location_aa)
        data_location_bb = ffdb.write_from_bytes(b"bb" * 10)
        print(data_location_bb)
        stream = io.BytesIO(b"cc" * 10)
        data_location_cc = ffdb.write_from_stream(stream.read)
        print(data_location_cc)

        # Read
        data_aa = ffdb.read_from_db(data_location_aa)
        assert data_aa == b"aa" * 10
        print(data_aa)

        data_bb = ffdb.read_from_db(data_location_bb)
        assert data_bb == b"bb" * 10
        print(data_bb)

        data_cc = ffdb.read_from_db(data_location_cc)
        assert data_cc == b"cc" * 10
        print(data_cc)
    finally:
        if os.path.exists(rand_dir):
            shutil.rmtree(rand_dir, onerror=remove_readonly)


def test_slicing():
    rand_dir = os.urandom(4).hex()
    try:
        ffdb = FlatFileDb(Path(rand_dir))
        data_location_mixed = ffdb.write_from_bytes(b"aaaaabbbbbccccc")
        print(data_location_mixed)

        slice = Slice(start_offset=5, end_offset=10)
        data_bb_slice = ffdb.read_from_db(data_location_mixed, slice)
        assert data_bb_slice == b"bbbbb"
        print(data_bb_slice)
    finally:
        if os.path.exists(rand_dir):
            shutil.rmtree(rand_dir, onerror=remove_readonly)
