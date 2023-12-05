import logging
import os
import typing
from io import BytesIO
from typing import BinaryIO

from zstandard import (
    ZstdCompressor,
    ZstdDecompressor,
    ZstdDecompressionReader,
)

if typing.TYPE_CHECKING:
    from zstandard import ZstdCompressionChunker


logger = logging.getLogger("zstd-compression")


def zstd_create_reader(read_handle: BinaryIO, read_size: int = 1024 * 4) -> ZstdDecompressionReader:
    assert read_handle.mode == 'rb'
    # This seek before creating the reader has overhead but it's necessary if called after
    # `zstd_get_uncompressed_eof_offset` which seeks to the end of the file.
    # I still achieve 8.5K random reads per second on NVMe which is plenty fast enough
    read_handle.seek(0)
    dctx = ZstdDecompressor()
    return dctx.stream_reader(read_handle, read_size=read_size, read_across_frames=True, closefd=False)


def zstd_create_chunker(chunk_size: int = 32768) -> "ZstdCompressionChunker":
    cctx = ZstdCompressor(threads=-1)
    return cctx.chunker(chunk_size=chunk_size)


def zstd_append_chunks(chunker: "ZstdCompressionChunker", append_handle: BinaryIO, data: bytes) -> None:
    """It's best to keep files open as long as practically possible because on magnetic drives,
    closing and re-opening files adds costly IOPS."""
    in_stream = BytesIO(data)
    assert append_handle.mode == 'ab'
    while True:
        in_chunk = in_stream.read(32768)
        if not in_chunk:
            break

        for out_chunk in chunker.compress(in_chunk):
            append_handle.write(out_chunk)


def zstd_finish_appending_chunks(chunker: "ZstdCompressionChunker", append_handle: BinaryIO) -> None:
    """It's best to keep files open as long as practically possible because on magnetic drives,
    closing and re-opening files adds costly IOPS."""
    for out_chunk in chunker.finish():  # type: ignore[no-untyped-call]
        append_handle.write(out_chunk)


def zstd_close_reader(reader: ZstdDecompressionReader) -> None:
    reader.close()  # type: ignore[no-untyped-call]


def zstd_read_fd(reader: ZstdDecompressionReader, offset: int, size: int) -> bytes:
    """It's best to keep files open as long as practically possible because on magnetic drives,
    closing and re-opening files adds costly IOPS."""
    reader.seek(offset, os.SEEK_SET)
    return reader.read(size)


def zstd_get_uncompressed_eof_offset(file: BinaryIO) -> int:
    # WARNING: This doesn't seem to work when the zstd writer has not first closed the file
    # so this should only be done on a file that was freshly opened. So why not just
    # open the file here to enforce this behaviour? Because the caller will be caching
    # read-mode file handles ready for when the worker processes request slices of the raw blocks

    # This is a bit of a hack using a file size that exceeds anything we'd ever expect.
    # This is much preferred than needing to persist the size of the uncompressed file somewhere.
    assert file.mode == 'rb'
    excessively_large_file_size = 64 * 2**32  # 64GB
    dctx = ZstdDecompressor()
    reader: ZstdDecompressionReader
    with dctx.stream_reader(file, read_across_frames=True, closefd=False) as reader:
        eof_uncompressed_offset = reader.seek(excessively_large_file_size, os.SEEK_SET)
    return eof_uncompressed_offset
