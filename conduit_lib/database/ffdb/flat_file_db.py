import io
import logging
import os
from pathlib import Path
from typing import Callable, NamedTuple, Optional

import filelock
from filelock import FileLock, BaseFileLock

from conduit_lib.constants import MAX_DAT_FILE_SIZE
from conduit_lib.types import Slice

logger = logging.getLogger("lmdb-utils")


class DataLocation(NamedTuple):
    """This metadata must be persisted elsewhere.
    For example, a key-value store such as LMDB"""
    file_path: str
    start_offset: int
    end_offset: int


class FlatFileDb:
    """This is a thread and process-safe abstraction optimizing for large sequential
    writes and reads to leverage more cost-efficient spinning disc storage.

    To achieve thread and process safety, it uses file locking."""

    DEFAULT_MAX_STREAM_READ_SIZE = 128 * 1024 ** 2

    def __init__(self, datadir: Path, max_stream_size: Optional[int]=None) -> None:
        self.datadir = datadir
        if not self.datadir.exists():
            os.makedirs(self.datadir, exist_ok=True)

        self.last_data_file_num: int = 0  # will scan until it finds a non-full filepath
        if len(os.listdir(self.datadir)) == 0:
            open(self.file_num_to_write_path(self.last_data_file_num), 'wb').close()

        self.max_stream_size = max_stream_size
        self.file_size: Optional[int] = None

        if not max_stream_size:
            self.max_stream_size = self.DEFAULT_MAX_STREAM_READ_SIZE

        self.write_path, self.flock, self.last_data_file_num = \
            self.move_to_next_non_full_write_path()

    def file_num_to_write_path(self, file_num: int) -> Path:
        padded_str_num = str(file_num).zfill(8)
        filename = f"data_{padded_str_num}.dat"
        return self.datadir / filename

    def get_file_size(self) -> int:
        if self.file_size:
            return self.file_size

        # This should only ever run once when FlatFileDb is initialized
        # in order to cache the value for the first non-full file
        with open(self.write_path, 'rb') as f:
            f.seek(0, io.SEEK_END)
            self.file_size = f.tell()
            return self.file_size

    def file_is_full(self) -> bool:
        return self.get_file_size() > MAX_DAT_FILE_SIZE

    def move_to_next_non_full_write_path(self) -> tuple[Path, BaseFileLock, int]:
        """This function is idempotent"""
        self.write_path = self.file_num_to_write_path(self.last_data_file_num)
        if self.file_is_full():
            while os.path.exists(self.write_path):
                self.last_data_file_num += 1
                self.write_path = self.file_num_to_write_path(self.last_data_file_num)
                self.file_size = 0

        flock_path = str(self.write_path) + ".lock"
        self.flock = FileLock(flock_path, timeout=10)  # pylint: disable=E0110
        self.write_path = self.write_path
        return self.write_path, self.flock, self.last_data_file_num

    def write_from_bytes(self, data: bytes) -> DataLocation:
        """Returns the new file size after appending"""
        try:
            self.move_to_next_non_full_write_path()
            with self.flock:
                with open(self.write_path, 'ab') as file:
                    start_offset = file.tell()
                    file.write(data)
                    end_offset = file.tell()
                return DataLocation(str(self.write_path), start_offset, end_offset)
        except filelock.Timeout:
            logger.exception(f"Filelock timed out for write path: {self.write_path}")
            raise

    def write_from_stream(self, reader: Callable[[int], bytes]) -> DataLocation:
        """Returns the new file size after appending"""
        try:
            assert self.max_stream_size is not None
            self.move_to_next_non_full_write_path()
            with self.flock:
                with open(self.write_path, 'ab') as file:
                    start_offset = file.tell()
                    while True:
                        data = reader(self.max_stream_size)
                        if data:
                            file.write(data)
                        else:
                            end_offset = file.tell()
                            break

                return DataLocation(str(self.write_path), start_offset, end_offset)
        except filelock.Timeout:
            logger.exception(f"Filelock timed out for write path: {self.write_path}")
            raise

    def read_from_db(self, data_location: DataLocation,
            slice: Optional[Slice]=None) -> bytes:
        """If the end offset of the Slice is zero, it reads to the end of the
        data location"""
        try:
            read_path, start_offset, end_offset = data_location
            flock_path = str(read_path) + ".lock"
            flock = FileLock(flock_path, timeout=10)  # pylint: disable=E0110
            with flock:
                with open(read_path, 'rb') as f:
                    f.seek(start_offset)
                    full_data_length = end_offset - start_offset
                    if not slice:
                        return f.read(full_data_length)

                    start_offset_within_entry = start_offset + slice.start_offset
                    if slice.end_offset == 0:
                        end_offset_within_entry = end_offset
                    else:
                        end_offset_within_entry = start_offset + slice.end_offset
                    f.seek(start_offset_within_entry)
                    len_data = end_offset_within_entry - start_offset_within_entry
                    return f.read(len_data)
        except filelock.Timeout:
            logger.exception(f"Filelock timed out for read path: {data_location.file_path}")
            raise

