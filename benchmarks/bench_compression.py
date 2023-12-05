# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.

import os
from pathlib import Path

from conduit_lib.compression import (
    zstd_create_chunker,
    zstd_append_chunks,
    zstd_finish_appending_chunks,
    zstd_read_fd,
    zstd_close_reader,
    zstd_create_reader,
    zstd_get_uncompressed_file_size,
)
from conduit_lib.utils import Timer


def test_compression_sequential() -> None:
    filepath = Path("test.zst")
    filepath.unlink(missing_ok=True)
    append_handle = open(filepath, 'ab')
    chunker = zstd_create_chunker(chunk_size=32768)
    try:
        initial_compressible_bytes = bytearray.fromhex("ff") * 256
        zstd_append_chunks(chunker, append_handle, initial_compressible_bytes)

        # highly compressible data -> 0.0091482% of original
        # data = bytearray(b"aabbccddee") * ((1000**2) * 500 // 10)

        # 500 repeats of 1MB uncompressible data -> 9.49% of original size
        data = bytearray(os.urandom((1000**2))) * 500

        # uncompressible data -> 100% of original size
        # data = bytearray(os.urandom((1000**2) * 500))
        len_data = len(data)
        print(f"Size of input data: {len(data)} bytes")

        with Timer(count=len_data // 1024**2, name='bench-zstd-write', units=" MB"):
            zstd_append_chunks(chunker, append_handle, data)
            zstd_finish_appending_chunks(chunker, append_handle)
            uncompressed_bytes_written = len_data
        assert uncompressed_bytes_written == len_data
        start_offset = len(initial_compressible_bytes)
        end_offset = start_offset + uncompressed_bytes_written

        compressed_size = os.stat(filepath).st_size
        print(f"Size of compressed file: {compressed_size} bytes")
        print(f"Compression Ratio: {compressed_size/uncompressed_bytes_written:0.4f}")

        full_data_in_file = initial_compressible_bytes + data
        expected_100_byte_chunks = []
        for i in range(400):
            from_offset = i * 400
            to_offset = i * 400 + 100
            expected_100_byte_chunks.append(full_data_in_file[from_offset:to_offset])

        read_handle = open(filepath, 'rb')
        read_handle.seek(0)
        eof_offset = zstd_get_uncompressed_file_size(read_handle)
        read_handle.seek(0)
        with Timer(count=400, name='bench-zstd-random-reads', units=" random 100 byte reads"):
            try:
                print(f"EOF offset: {eof_offset}")
                for i in range(400):
                    reader = zstd_create_reader(read_handle, read_size=1024 * 4)
                    assert expected_100_byte_chunks[i] == zstd_read_fd(reader, i * 400, 100)
                    zstd_close_reader(reader)
            finally:
                read_handle.close()
    finally:
        append_handle.close()
        filepath.unlink(missing_ok=True)


# 2023-12-05 13:33:49 DEBUG Timer[bench-zstd-write]: interval of 0.2666 seconds
# 2023-12-05 13:33:49 DEBUG Timer[bench-zstd-write]: throughput rate: 1785.4 MB per second
# 2023-12-05 13:33:49 DEBUG Timer[bench-zstd-random-reads]: interval of 0.0174 seconds
# 2023-12-05 13:33:49 DEBUG Timer[bench-zstd-random-reads]: throughput rate: 23053.3 random 100 byte reads per second
# Size of input data: 500000000 bytes
# Size of compressed file: 46793 bytes
# Compression Ratio: 0.0001
