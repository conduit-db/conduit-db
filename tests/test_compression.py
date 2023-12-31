import json
import os
from pathlib import Path

from conduit_lib.database.ffdb.compression import write_to_file_zstd, \
    CompressionStats, open_seekable_reader_zstd, uncompressed_file_size_zstd, CompressionBlockInfo, \
    check_and_recover_zstd_file


MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))


def test_compression() -> None:
    filepath = Path("test.zst")
    filepath.unlink(missing_ok=True)
    try:
        initial_compressible_bytes = bytes.fromhex("ff") * 256
        compression_stats = CompressionStats()
        data_locations = write_to_file_zstd(filepath, [initial_compressible_bytes], fsync=False,
            compression_stats=compression_stats, mode='wb')
        assert data_locations[0].file_path == str(filepath)
        assert data_locations[0].start_offset == 0
        assert data_locations[0].end_offset == len(initial_compressible_bytes)

        assert compression_stats.filename == str(filepath.name)
        assert compression_stats.uncompressed_size == len(initial_compressible_bytes)
        assert compression_stats.compressed_size > 0
        assert compression_stats.compressed_size < len(initial_compressible_bytes)
        assert compression_stats.fraction_of_compressed_size == compression_stats.compressed_size / compression_stats.uncompressed_size
        assert compression_stats.block_metadata == []
        assert compression_stats.max_window_size is not None
        assert compression_stats.zstandard_level is not None

        data = bytearray(b"aabbccddee") * ((1000 ** 2) * 500 // 10)  # highly compressible data
        print(f"Size of input data: {len(data)} bytes")
        expected_data_a = data[0:100]  # first 100 bytes
        # 100 bytes at the 100MB mark + 1 extra byte
        expected_data_b = data[(1000 ** 2) * 100 + 1: (1000 ** 2) * 100 + 101]
        # 100 bytes at the 499MB mark + 3 extra bytes
        expected_data_c = data[(1000 ** 2) * 499 + 3: (1000 ** 2) * 499 + 103]
        # the last byte
        expected_data_d = data[len(data) - 1:]

        compression_stats = CompressionStats()
        data_locations = write_to_file_zstd(filepath, [data], fsync=False,
            compression_stats=compression_stats)

        start_offset = len(initial_compressible_bytes)
        end_offset = len(initial_compressible_bytes) + len(data)
        assert data_locations[0].file_path == str(filepath)
        assert data_locations[0].start_offset == start_offset
        assert data_locations[0].end_offset == end_offset

        print(f"Size of compressed file: {os.stat(filepath).st_size} bytes")
        with open_seekable_reader_zstd(filepath) as reader:
            eof_offset = uncompressed_file_size_zstd(str(filepath))
            assert eof_offset == end_offset

            reader.seek(start_offset)
            assert reader.read(100) == expected_data_a

            reader.seek((1000 ** 2) * 100 + 1 + start_offset)
            assert reader.read(100) == expected_data_b

            reader.seek((1000 ** 2) * 499 + 3 + start_offset)
            assert reader.read(100) == expected_data_c

            reader.seek((len(data) + start_offset - 1))
            assert reader.read(1) == expected_data_d
    finally:
        filepath.unlink(missing_ok=True)


def test_compression_stats_json_serialization():
    block_metadata = CompressionBlockInfo(
        block_id="aa" * 32,
        tx_count=10000,
        size_mb=1000
    )
    compression_stats = CompressionStats(
        filename='myfile',
        block_metadata=[block_metadata],
        uncompressed_size=10000,
        compressed_size=100,
        fraction_of_compressed_size=100 / 10000,
    )
    assert json.loads(compression_stats.to_json()) == \
           json.loads(('{'
                       '    "filename": "myfile",'
                       '    "block_metadata": ['
                       '        {'
                       '            "block_id": '
                       '"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
                       '            "tx_count": 10000,'
                       '            "size_mb": 1000'
                       '        }'
                       '    ],'
                       '    "uncompressed_size": 10000,'
                       '    "compressed_size": 100,'
                       '    "fraction_of_compressed_size": 0.01,'
                       '    "max_window_size": 10485760,'
                       '    "zstandard_level": 3'
                       '}'))


def test_check_and_recover_zstd_file():
    # Uncorrupted file
    filepath = MODULE_DIR / "data" / "data_00000000.dat.zst"
    check_and_recover_zstd_file(str(filepath))


# def test_check_and_recover_zstd_file2():
#     # Uncorrupted file
#     filepath = MODULE_DIR / "data" / "3e2b03dd409ee5bdbb547c6509c71d75"
#     check_and_recover_zstd_file(str(filepath))
#     size = uncompressed_file_size_zstd(str(filepath))
#     DEFAULT_LARGE_MESSAGE_LIMIT = 32 * 1024 * 1024  # 32MB
#     assert size > DEFAULT_LARGE_MESSAGE_LIMIT
#
#     compressed_size = os.path.getsize(str(filepath))
#     print(compressed_size)

