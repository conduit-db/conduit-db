"""
NOTE: FlatFileDb should remain decoupled from LMDB. Updates will not be allowed.
 Deletion of whole files will be the only way to remove old records (to create a rolling window
 storage) which is a good fit for the blockchain timestamp / time-series model.
"""

import logging
import os
import threading
from pathlib import Path
from types import TracebackType
from typing import NamedTuple, Optional, Set, Type, List
from fasteners import InterProcessReaderWriterLock

from conduit_lib.types import Slice

logger = logging.getLogger("lmdb-utils")


class FlatFileDbUnsafeAccessError(Exception):
    pass


class FlatFileDbWriteFailedError(Exception):
    pass


MAX_DAT_FILE_SIZE = 128 * (1024 ** 2)  # 128MB


class DataLocation(NamedTuple):
    """This metadata must be persisted elsewhere.
    For example, a key-value store such as LMDB"""
    file_path: str
    start_offset: int
    end_offset: int


class FlatFileDb:
    """This is optimized for use as a write once, read many BLOB storage abstraction which writes
    directly to flat files. Writes can only occur to a single designated "mutable_file" in
    append only mode. Synchronization uses a single file lock for controlling access to this file.

    All other data files of size >= MAX_DAT_FILE_SIZE, are immutable and concurrent readers do not
    require any file locking or synchronization. The only time readers require synchronization is
    when they are reading an entry from the "mutable_file".

    The API allows for reading arbitrarily small slices into each BLOB of data without thrashing
    memory.

    There are many BLOB values per file to avoid writing many thousands of small 1MB files and
    thereby minimising IOPS on spinning HDDs.

    The "mutable_file" will be written to until its size is >= MAX_DAT_FILE_SIZE.
    It will then open a new "mutable_file". The old mutable file can now be
    considered to be immutable.

    Deleting entries can only occur by deleting an entire file. There is no update functionality.

    InterProcessReaderWriterLock can do around 10000 lock/unlock cycles per second on an
    NVMe SSD drive. This far exceeds the maximum IOPS of a spinning HDD which is on the order of
    200/second. For this reason, it is a good idea to place the file lock on an SSD as well as
    the key value store for recording the DataLocation of each BLOB.

    NOTE: InterProcessReaderWriterLock is not re-entrant.
    NOTE: FlatFileDB is thread-safe but __init__ method is not. Must instantiate in parent thread
    """

    def __init__(self, datadir: Path, mutable_file_lock_path: Path, fsync: bool = False) -> None:
        self.threading_lock = threading.RLock()
        self.fsync: bool = fsync
        self.datadir = datadir
        self.mutable_file_lock_path = mutable_file_lock_path
        assert str(self.mutable_file_lock_path).endswith(
            ".lock"), "mutable_file_lock_path must end with '.lock'"

        if not self.datadir.exists():
            os.makedirs(self.datadir, exist_ok=True)

        # Inter-process synchronization of access to the mutable file
        # WARNING mutable_file_lock_path needs to exactly match across threads and processes
        self.mutable_file_rwlock = InterProcessReaderWriterLock(
            str(self.mutable_file_lock_path))

        # Initialization
        with self.mutable_file_rwlock.write_lock():
            # TODO if earlier files are pruned there needs to be a metadata file to record it
            # Create file data_00000000.dat if it's the first time opening the datadir
            self.mutable_file_path = self._file_num_to_mutable_file_path(file_num=0)
            if not self.mutable_file_path.exists():
                logger.debug(f"Initializing datadir {self.datadir} by creating"
                             f" {self.mutable_file_path}")
                with open(self.mutable_file_path, 'ab') as f:
                    f.flush()
                    os.fsync(f.fileno())

        with self.mutable_file_rwlock.read_lock():
            # Scans datadir to get the correct mutable_file_file cached properties
            _immutable_files: List[str] = os.listdir(self.datadir)
            _immutable_files.sort()
            # Pop the single mutable file (which always has the highest number)
            for file in _immutable_files:
                if file.endswith(".lock"):
                    _immutable_files.remove(file)
            mutable_filename = _immutable_files.pop()
            self.mutable_file_num = self._mutable_filename_to_num(mutable_filename)
            self.mutable_file_path = self._file_num_to_mutable_file_path(self.mutable_file_num)
            self.immutable_files: Set[str] = set(_immutable_files)

    def __enter__(self) -> 'FlatFileDb':
        self.threading_lock.acquire()
        return self

    def __exit__(self, exc_type: Optional[Type[BaseException]],
            exc_val: Optional[BaseException], exc_tb: Optional[TracebackType]) -> None:
        self.threading_lock.release()

    def _file_num_to_mutable_file_path(self, file_num: int) -> Path:
        padded_str_num = str(file_num).zfill(8)
        filename = f"data_{padded_str_num}.dat"
        assert self.datadir is not None
        return self.datadir / filename

    def _mutable_filename_to_num(self, filename: str) -> int:
        filename = filename.removeprefix("data_")
        filename = filename.removesuffix(".dat")
        return int(filename)

    def _maybe_get_new_mutable_file(self) -> tuple[Path, int]:
        """This function is idempotent. Caller must use a Write lock"""
        assert self.mutable_file_path is not None
        def _mutable_file_is_full() -> bool:
            assert self.mutable_file_path is not None
            file_size = os.path.getsize(self.mutable_file_path)
            is_full = file_size >= MAX_DAT_FILE_SIZE
            return is_full

        if _mutable_file_is_full():
            # If deletes and updates are allowed this needs to be more sophisticated
            # To ensure that the mutable file always has the highest number (no reuse of
            # lower mutable_file_num even if they get deleted)
            while True:
                logger.debug(f"Scanning forward... self.mutable_file_num={self.mutable_file_num}")
                self.mutable_file_num += 1
                self.mutable_file_path = self._file_num_to_mutable_file_path(
                    self.mutable_file_num)
                self.immutable_files.add(str(self.mutable_file_path))

                if os.path.exists(self.mutable_file_path):
                    if _mutable_file_is_full():
                        continue
                    else:
                        self.mutable_file_path = self.mutable_file_path
                        break
                else:
                    logger.debug(f"Creating a new mutable file at: {self.mutable_file_path}")
                    with open(self.mutable_file_path, 'ab') as f:
                        f.flush()
                        os.fsync(f.fileno())
                    self.mutable_file_path = self.mutable_file_path
                    break

        return self.mutable_file_path, self.mutable_file_num

    def put(self, data: bytes) -> DataLocation:
        assert self.mutable_file_path is not None
        with self.mutable_file_rwlock.write_lock():
            self._maybe_get_new_mutable_file()
            with open(self.mutable_file_path, 'ab') as file:
                start_offset = file.tell()
                file.write(data)
                end_offset = file.tell()
                if self.fsync:
                    file.flush()
                    os.fsync(file.fileno())

            return DataLocation(str(self.mutable_file_path), start_offset, end_offset)

    def get(self, data_location: DataLocation, slice: Optional[Slice] = None,
            lock_free_access: bool=False) -> bytes:
        """If the end offset of the Slice is zero, it reads to the end of the
        data location

        lock_free_access=True is only safe if there are no concurrent deletes.
        If this is used as a write once, read many database, this assumption holds and the
        advantage is unfettered lock-free read access to all immutable files. A lock must ALWAYS
        be acquired for reading the mutable file - this can never be disabled.
        """
        is_accessing_mutable_file = data_location.file_path not in self.immutable_files

        if is_accessing_mutable_file or not lock_free_access:
            self.mutable_file_rwlock.acquire_read_lock()

        try:
            read_path, start_offset, end_offset = data_location
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
        except OSError:
            logger.error(f"Error reading from file_path: {data_location.file_path}")
            raise FileNotFoundError(f"Error reading from file_path: {data_location.file_path}")
        finally:
            if is_accessing_mutable_file or not lock_free_access:
                self.mutable_file_rwlock.release_read_lock()

    def delete_file(self, file_path: Path) -> None:
        """It is critically important that no reads are being attempted when deleting a file.
        It is not safe.

        NOTE This does not raise in case of OSError and so there's a theoretical risk of
        leaking disc space if this goes unnoticed. But I'd rather leak some disc space and log the
        error to allow the server to remain operational.
        """
        with self.mutable_file_rwlock.write_lock():
            try:
                os.remove(file_path)
            except FileNotFoundError:
                logger.error(f"File not found at path: {file_path}. Was it already deleted?")
            except OSError:
                logger.exception(f"Exception while attempting to delete file: {file_path}")
