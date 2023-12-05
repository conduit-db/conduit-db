import os
from pathlib import Path

from conduit_lib.compression import (
    zstd_read_fd,
    zstd_create_chunker,
    zstd_append_chunks,
    zstd_finish_appending_chunks,
    zstd_create_reader,
    zstd_get_uncompressed_eof_offset,
)


def test_compression() -> None:
    filepath = Path("test.zst")
    filepath.unlink(missing_ok=True)
    append_handle = open(filepath, 'ab')
    chunker = zstd_create_chunker()
    try:
        initial_compressible_bytes = bytes.fromhex("ff") * 256
        zstd_append_chunks(chunker, append_handle, initial_compressible_bytes)

        data = bytearray(b"aabbccddee") * ((1000**2) * 500 // 10)  # highly compressible data
        len_data = len(data)
        print(f"Size of input data: {len(data)} bytes")
        expected_data_a = data[0:100]  # first 100 bytes
        # 100 bytes at the 100MB mark + 1 extra byte
        expected_data_b = data[(1000**2) * 100 + 1 : (1000**2) * 100 + 101]
        # 100 bytes at the 100MB mark + 3 extra bytes
        expected_data_c = data[(1000**2) * 499 + 3 : (1000**2) * 499 + 103]  # 100 bytes at the 499MB mark
        expected_data_d = data[len(data) - 1 :]  # the last byte

        zstd_append_chunks(chunker, append_handle, data)
        zstd_finish_appending_chunks(chunker, append_handle)

        start_offset = len(initial_compressible_bytes)
        end_offset = start_offset + len(data)

        print(f"Size of compressed file: {os.stat(filepath).st_size} bytes")
        with open(filepath, 'rb') as read_handle:
            try:
                eof_offset = zstd_get_uncompressed_eof_offset(read_handle)
                assert eof_offset == end_offset
                reader = zstd_create_reader(read_handle)
                assert zstd_read_fd(reader, 0 + start_offset, 100) == expected_data_a
                assert zstd_read_fd(reader, (1000**2) * 100 + 1 + start_offset, 100) == expected_data_b
                assert zstd_read_fd(reader, (1000**2) * 499 + 3 + start_offset, 100) == expected_data_c
                assert zstd_read_fd(reader, (len_data + start_offset - 1), 1) == expected_data_d
            finally:
                reader.close()
                read_handle.close()
    finally:
        if append_handle:
            append_handle.close()
        filepath.unlink(missing_ok=True)
