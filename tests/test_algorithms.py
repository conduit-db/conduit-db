import gc
import io
import os
import time
from pathlib import Path

import bitcoinx
from bitcoinx import hex_str_to_hash, hash_to_hex_str

from .conftest import TEST_RAW_BLOCK_413567
from .data.block413567_offsets import TX_OFFSETS

from conduit_lib.algorithms import calc_depth, get_mtree_node_counts_per_level, \
    build_mtree_from_base, parse_txs, get_pk_and_pkh_from_script, unpack_varint, preprocessor
from conduit_lib.constants import HashXLength
from conduit_lib.types import PushdataMatchFlags


MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
os.environ['GENESIS_ACTIVATION_HEIGHT'] = "0"


def print_results(count_txs, t1, block_view):
    rate = count_txs / t1
    av_tx_size = round(len(block_view) / count_txs)
    bytes_per_sec = rate * av_tx_size
    MB_per_sec = round(bytes_per_sec / (1024 * 1024))

    print(
        f"block parsing took {round(t1, 5)} seconds for {count_txs} txs and"
        f" {len(block_view)} "
        f"bytes - therefore {round(rate)} txs per second for an average tx size "
        f"of {av_tx_size} bytes - therefore {MB_per_sec} MB/sec"
    )


def test_preprocessor_whole_block_as_a_single_chunk():
    next_chunk = bytearray(TEST_RAW_BLOCK_413567)
    tx_count, offset = unpack_varint(next_chunk[80:89], 0)
    assert tx_count == 1557
    block_hash_hex = hash_to_hex_str(bitcoinx.double_sha256(next_chunk[0:80]))
    assert block_hash_hex == "0000000000000000025aff8be8a55df8f89c77296db6198f272d6577325d4069"

    # Whole block as a single chunk
    REPEAT_N_TIMES = 10
    t0 = time.perf_counter()
    for i in range(REPEAT_N_TIMES):
        tx_offsets, last_tx_offset_in_chunk = preprocessor(next_chunk,
                adjustment=0, first_chunk=True, last_chunk=True)
    t1 = time.perf_counter() - t0

    print_results(len(tx_offsets), t1/REPEAT_N_TIMES, next_chunk)

    assert tx_offsets.tolist() == TX_OFFSETS
    assert last_tx_offset_in_chunk == len(next_chunk)


def test_preprocessor_with_block_divided_into_four_chunks():
    full_block = bytearray(TEST_RAW_BLOCK_413567)
    tx_count, offset = unpack_varint(full_block[80:89], 0)
    assert tx_count == 1557
    # Same block processed in 4 chunks
    chunks = [
        full_block[0:250_000],
        full_block[250_000:500_000],
        full_block[500_000:750_000],
        full_block[750_000:]
    ]
    tx_offsets_all = []
    remainder = b""
    adjustment = 0
    t0 = time.perf_counter()
    last_chunk = False
    for idx, chunk in enumerate(chunks):
        if idx == 0:
            first_chunk = True
        else:
            first_chunk = False
            if tx_offsets_all:
                adjustment = tx_offsets_all[-1]

        if idx == (len(chunks) - 1):
            last_chunk = True

        modified_chunk = remainder + chunk
        tx_offsets, last_tx_offset_in_chunk = preprocessor(modified_chunk,
                adjustment=adjustment, first_chunk=first_chunk, last_chunk=last_chunk)
        tx_offsets_all.extend(tx_offsets)
        remainder = modified_chunk[last_tx_offset_in_chunk:]

        if idx == 0:
            assert tx_offsets[0] == 83
            assert tx_offsets[-1] == 249138
        if idx == 1:
            assert tx_offsets[0] == 257471
            assert tx_offsets[-1] == 482309
        if idx == 2:
            assert tx_offsets[0] == 541517
            assert tx_offsets[-1] == 749909
        if idx == 3:
            assert tx_offsets[0] == 750134
            assert tx_offsets[-1] == 999367

    t1 = time.perf_counter() - t0
    print_results(len(tx_offsets_all), t1, full_block)
    assert len(tx_offsets_all) == 1557
    assert tx_offsets_all == TX_OFFSETS


def test_parse_txs():
    """This is performance critical so needs to maintain acceptable throughput rates.

    Check validity against bitcoinx.Tx.read - which is well battle tested code
    """
    tx_rows, in_rows, out_rows, pd_rows = [], [], [], []

    # Check unpack_varint
    for i in range(1, 1_000):
        varint_buf = bitcoinx.pack_varint(i)
        parsed_varint = unpack_varint(varint_buf, 0)[0]
        assert parsed_varint == i, f"parsed_varint: {parsed_varint} != {i}"
    print("Check for varint parsing (pure python): PASSED")

    raw_block = bytearray(TEST_RAW_BLOCK_413567)
    tx_offsets, offset_before_raise = preprocessor(raw_block, adjustment=0, first_chunk=True,
        last_chunk=True)

    t0 = time.perf_counter()
    REPEAT_N_TIMES = 10
    for i in range(REPEAT_N_TIMES):
        tx_rows, tx_rows_mempool, in_rows, out_rows, pd_rows = parse_txs(raw_block, tx_offsets,
            413567, True, 0)
    t1 = time.perf_counter() - t0
    print_results(len(tx_rows), t1/REPEAT_N_TIMES, raw_block)

    # -----------------------------  check validity ------------------------------------- #
    stream = io.BytesIO(raw_block)
    stream.read(80)
    tx_count = bitcoinx.read_varint(stream.read)
    assert tx_count == 1557

    txs = []
    inputs = []
    pushdata_hashes_per_script = []
    output_total_count = 0
    for i in range(1557):
        tx = bitcoinx.Tx.read(stream.read)

        # Check tx_hash matches
        tx_hash, tx_height, tx_position = tx_rows[i]
        assert tx.hash()[0:HashXLength] == bytes.fromhex(tx_hash)[0:HashXLength]

        for idx, input in enumerate(tx.inputs):
            pd_hashes = get_pk_and_pkh_from_script(input.script_sig.to_bytes(),
                tx_hash, idx, PushdataMatchFlags.INPUT, tx_pos=i, genesis_height=0)
            if len(pd_hashes) != 0:
                pushdata_hashes_per_script.extend(pd_hashes)

        inputs.extend((tx.inputs))

        output_total_count += len(tx.outputs)
        txs.append(tx)

    with open(MODULE_DIR / 'data' / 'block413567_pushdata_hashes', 'r') as file:
        correct_pushdata_hashes = file.readlines()
        expected = [line.strip() for line in correct_pushdata_hashes]
        actual = [hash_to_hex_str(pushdata_match.pushdata_hash) for pushdata_match in pushdata_hashes_per_script]
        assert expected == actual

    print(f"Check for all tx hashes matching bitcoinx parsing: PASSED")
    assert len(txs) == len(tx_rows)

    # This is slow but I don't care!
    def scan_inputs_for_hash_and_idx_match(prev_out_hash, prev_idx, in_rows):
        """Must reverse the hex rows endianness to match bitcoinx"""
        for row in in_rows:
            out_tx_hash, out_idx, in_tx_hash, in_idx = row
            if prev_out_hash[0:HashXLength] == hex_str_to_hash(out_tx_hash)[::-1] and prev_idx == out_idx:
                # print(f"Match found for {hash_to_hex_str(prev_out_hash)} == {out_tx_hash}")
                return True
            else:
                # print(f"{hash_to_hex_str(prev_out_hash)} != {hash_to_hex_str(hex_str_to_hash(out_tx_hash)[::-1])}")
                pass
        return False

    def scan_outputs_for_hash_and_idx_match(tx_hash, idx, value, out_rows):
        """Must reverse the hex rows endianness to match bitcoinx"""
        for row in out_rows:
            out_tx_hash, out_idx, out_value = row
            if tx_hash[0:HashXLength] == hex_str_to_hash(out_tx_hash)[::-1] and idx == out_idx and value == out_value:
                # print(f"Match found for {hash_to_hex_str(prev_out_hash)} == {out_tx_hash}")
                return True
            else:
                # print(f"{hash_to_hex_str(prev_out_hash)} != {hash_to_hex_str(hex_str_to_hash(out_tx_hash)[::-1])}")
                pass
        return False

    match_found = None
    assert len(in_rows) == len(inputs), f"{len(in_rows)} != {len(inputs)}"
    print(f"Check for total inputs count: PASSED ({len(in_rows)} inputs matches expected of: {len(inputs)})")
    for i in range(len(inputs)):
        bitcoinx_trusted_input = inputs[i]
        match_found = scan_inputs_for_hash_and_idx_match(
            bitcoinx_trusted_input.prev_hash[0:HashXLength],
            bitcoinx_trusted_input.prev_idx,
            in_rows)
        assert match_found
    print(f"Check for all inputs matching bitcoinx parsing: PASSED")

    assert output_total_count == len(out_rows), f"{output_total_count} != {len(out_rows)}"
    print(f"Check for total output count: PASSED ({output_total_count} outputs matches expected of: {len(out_rows)})")
    for tx in txs:
        trusted_bitcoinx_outputs = tx.outputs
        for idx, output in enumerate(trusted_bitcoinx_outputs):
            match_found = scan_outputs_for_hash_and_idx_match(
                tx.hash()[0:HashXLength], idx, output.value, out_rows)
        assert match_found
    print(f"Check for all outputs matching bitcoinx parsing: PASSED")



def test_calc_depth() -> None:
    tx_count = 1
    assert calc_depth(tx_count) == 1
    tx_count = 2
    assert calc_depth(tx_count) == 2
    tx_count = 3
    assert calc_depth(tx_count) == 3
    tx_count = 4
    assert calc_depth(tx_count) == 3
    tx_count = 5
    assert calc_depth(tx_count) == 4
    tx_count = 13
    assert calc_depth(tx_count) == 5


def test_get_mtree_node_counts_per_level() -> None:
    base_node_count = 4
    assert get_mtree_node_counts_per_level(base_node_count) == [1, 2, 4]
    base_node_count = 5
    assert get_mtree_node_counts_per_level(base_node_count) == [1, 2, 3, 5]
    base_node_count = 7
    assert get_mtree_node_counts_per_level(base_node_count) == [1, 2, 4, 7]
    base_node_count = 13
    assert get_mtree_node_counts_per_level(base_node_count) == [1, 2, 4, 7, 13]


def test_calc_mtree_base_level():
    tx_hashes = [b"aa"*32, b"bb"*32, b"cc"*32, b"dd"*32, b"ee"*32]
    tx_count = len(tx_hashes)

    base_level_index = calc_depth(leaves_count=tx_count) - 1
    mtree = {base_level_index: tx_hashes}
    mtree = build_mtree_from_base(base_level_index, mtree)
    print(mtree)
    assert len(mtree.keys()) == 4
    assert mtree[3] == tx_hashes
    assert mtree[2] == [
        b"\x05\xc7\x8a-06rc>'\xd9\x01V\xd88\xeb$\xc8\xd1\xa7t}k\xc4\n\xc7\xebK\xf0\xb5\xfc!",
        b'n\xe5\xe6v\xc8\xbc\x96\xf7Z^\xfe\xaa$vJ\xbcZ\xdcE\x18\xa6\xd2;mh\xe7\x8e\xefX\x89S&',
        b'\xa8\x01\xc5\x942B\x94F\xbf\xa7\x92\xfaC\xc3t\x0b\xb7\\9\x95\x908\xe2\x06\xd7nD8\x8cF\xe7)'
    ]
    assert mtree[1] == [
        b'(r O\xf8U$\xbd\x1e\xc7\xea4\x8fE\xaf\xb5\xe1Mc\xbf\xddA\x15WX\x97\xa8\xb8\xf6\x8b\xfcR',
        b'fN\xc0\xbf\x8d\x82\xf5\r\xf3"I\xc3\xd2s0k\xf9"\xa0\x83\xd0\x0cN\xc1\xf7bO\x121\x03de'
    ]
    assert mtree[0] == [
        b'a\xf5\x15[\x11\xa4\xef`\xcc\xe8\x11t@r\x0e\xc9\x82\x12u\xbaO\x9d\xd2\xb7\x83\x8e\x1e\xb4\xa1\x0e\xfau'
    ]
