# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.

import io
import json
import os
import time
from pathlib import Path

import bitcoinx
import pytest
from bitcoinx import hash_to_hex_str

from conduit_lib.database.db_interface.types import ConfirmedTransactionRow, InputRowParsed
from .conftest import TEST_RAW_BLOCK_413567
from .data.block413567_offsets import TX_OFFSETS

from conduit_lib.algorithms import (
    calc_depth,
    get_mtree_node_counts_per_level,
    build_mtree_from_base,
    get_pk_and_pkh_from_script,
    unpack_varint,
    preprocessor,
    PushdataMatch,
    preprocessor, parse_txs_to_list,
)
from conduit_lib.constants import HashXLength
from conduit_lib.types import PushdataMatchFlags

MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
os.environ["GENESIS_ACTIVATION_HEIGHT"] = "0"


def print_results(count_txs: int, t1: float, block_view: bytes) -> None:
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


def test_preprocessor_whole_block_as_a_single_chunk() -> None:
    full_block = bytearray(TEST_RAW_BLOCK_413567)
    tx_count, offset = unpack_varint(full_block[80:89], 0)
    assert tx_count == 1557
    block_hash_hex = hash_to_hex_str(bitcoinx.double_sha256(full_block[0:80]))
    assert block_hash_hex == "0000000000000000025aff8be8a55df8f89c77296db6198f272d6577325d4069"

    # Whole block as a single chunk
    tx_offsets_all: list[int] = []
    remainder = b""
    last_tx_offset_in_chunk = 0
    for idx, chunk in enumerate([full_block]):
        if idx == 0:
            tx_count, var_int_size = unpack_varint(full_block[80:89], 0)
            adjustment = 0
            offset = 80 + var_int_size
        else:
            adjustment = last_tx_offset_in_chunk
            offset = 0
        modified_chunk = remainder + chunk
        tx_offsets_for_chunk, last_tx_offset_in_chunk = preprocessor(modified_chunk, offset, adjustment)
        tx_offsets_all.extend(tx_offsets_for_chunk)
        remainder = modified_chunk[last_tx_offset_in_chunk - adjustment :]

    assert tx_offsets_all == TX_OFFSETS
    assert last_tx_offset_in_chunk == len(full_block)


def test_preprocessor_with_block_divided_into_four_chunks() -> None:
    full_block = bytearray(TEST_RAW_BLOCK_413567)
    tx_count, offset = unpack_varint(full_block[80:89], 0)
    assert tx_count == 1557
    # Same block processed in 4 chunks
    chunks = [
        full_block[0:250_000],
        full_block[250_000:500_000],
        full_block[500_000:750_000],
        full_block[750_000:],
    ]

    t0 = time.perf_counter()

    tx_offsets_all: list[int] = []
    remainder = b""
    last_tx_offset_in_chunk = 0
    for idx, chunk in enumerate(chunks):
        if idx == 0:
            tx_count, var_int_size = unpack_varint(full_block[80:89], 0)
            adjustment = 0
            offset = 80 + var_int_size
        else:
            offset = 0
            adjustment = last_tx_offset_in_chunk
        modified_chunk = remainder + chunk
        tx_offsets_for_chunk, last_tx_offset_in_chunk = preprocessor(modified_chunk, offset, adjustment)
        tx_offsets_all.extend(tx_offsets_for_chunk)
        remainder = modified_chunk[last_tx_offset_in_chunk - adjustment :]

        if idx == 0:
            assert tx_offsets_for_chunk[0] == 83
            assert tx_offsets_for_chunk[-1] == 248948
        if idx == 1:
            assert tx_offsets_for_chunk[0] == 249138
            assert tx_offsets_for_chunk[-1] == 423160
        if idx == 2:
            assert tx_offsets_for_chunk[0] == 482309
            assert tx_offsets_for_chunk[-1] == 749719
        if idx == 3:
            assert tx_offsets_for_chunk[0] == 749909
            assert tx_offsets_for_chunk[-1] == 999367
            assert last_tx_offset_in_chunk == 999887
            assert last_tx_offset_in_chunk == len(TEST_RAW_BLOCK_413567)

    t1 = time.perf_counter() - t0
    print_results(len(tx_offsets_all), t1, full_block)
    assert len(tx_offsets_all) == 1557
    assert tx_offsets_all == TX_OFFSETS


# This will only work if you have downloaded the >250MB block at height 593161 and downloaded
# it to data/block593161.hex. It's too large to include in CI/CD but I have included it here
# because it uncovered a bug in the preprocessor when I struck it on mainnet.
def test_preprocessor_on_256mb_block_593161() -> None:
    block593161_filepath = MODULE_DIR / "data" / "block593161.hex"
    if not block593161_filepath.exists():
        pytest.skip(
            "This test requires that you download the 256MB block at height 593161 " "to data/block593161.hex"
        )

    with open(block593161_filepath, "r") as file:
        rpc_result_json = file.read()

    rpc_result = json.loads(rpc_result_json)
    big_block = bytes.fromhex(rpc_result["result"])
    assert len(big_block) == 255996865
    raw_header = big_block[0:80]
    tx_count, offset = unpack_varint(big_block[80:89], 0)

    # BITCOINX
    stream = io.BytesIO(big_block[80 + offset :])
    adjustment = 80 + offset
    bitcoinx_tx_offsets = [adjustment]
    offset = adjustment
    tx_hashes_set = set()
    for i in range(tx_count):
        tx = bitcoinx.Tx.read(stream.read)
        offset += len(tx.to_bytes())
        bitcoinx_tx_offsets.append(offset)
        tx_hashes_set.add(tx.hex_hash())

    assert len(bitcoinx_tx_offsets) == tx_count + 1
    assert len(tx_hashes_set) == tx_count

    # CONDUIT PREPROCESSOR
    BUFFER_SIZE = 250_000_000  # block size is 255996865 so expect two chunks
    chunks = [
        big_block[0:BUFFER_SIZE],
        big_block[BUFFER_SIZE:],
    ]

    t0 = time.perf_counter()

    tx_offsets_all: list[int] = []
    remainder = b""
    last_tx_offset_in_chunk = 0
    for idx, chunk in enumerate(chunks):
        if idx == 0:
            tx_count, var_int_size = unpack_varint(big_block[80:89], 0)
            adjustment = 0
            offset = 80 + var_int_size
        else:
            adjustment = last_tx_offset_in_chunk
            offset = 0
        modified_chunk = remainder + chunk
        tx_offsets_for_chunk, last_tx_offset_in_chunk = preprocessor(modified_chunk, offset, adjustment)
        tx_offsets_all.extend(tx_offsets_for_chunk)
        # adjustment here is effectively acting as the prev_last_tx_offset_in_chunk
        remainder = modified_chunk[last_tx_offset_in_chunk - adjustment :]

    t1 = time.perf_counter() - t0
    print()
    print_results(len(tx_offsets_all), t1, big_block)
    print()
    assert tx_offsets_all == bitcoinx_tx_offsets[:-1]


def test_parse_txs() -> None:
    """This is performance critical so needs to maintain acceptable throughput rates.

    Check validity against bitcoinx.Tx.read - which is well battle tested code
    """
    tx_rows: list[ConfirmedTransactionRow] = []
    in_rows: list[InputRowParsed] = []

    # Check unpack_varint
    for i in range(1, 1_000):
        varint_buf = bitcoinx.pack_varint(i)
        parsed_varint = unpack_varint(varint_buf, 0)[0]
        assert parsed_varint == i, f"parsed_varint: {parsed_varint} != {i}"
    print("Check for varint parsing (pure python): PASSED")

    raw_block = bytearray(TEST_RAW_BLOCK_413567)
    tx_count, var_int_size = unpack_varint(TEST_RAW_BLOCK_413567[80:89], 0)
    offset = 80 + var_int_size
    tx_offsets, offset_before_raise = preprocessor(raw_block, offset=offset, adjustment=0)

    t0 = time.perf_counter()
    REPEAT_N_TIMES = 1
    for i in range(REPEAT_N_TIMES):
        (
            tx_rows,
            tx_rows_mempool,
            in_rows,
            pd_rows,
            utxo_spends,
            pushdata_matches_tip_filter,
        ) = parse_txs_to_list(raw_block, tx_offsets, 413567, True, 0)
    t1 = time.perf_counter() - t0
    print_results(len(tx_rows), t1 / REPEAT_N_TIMES, raw_block)

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
        tx_hash: str
        tx_height: int
        tx_position: int
        tx_hash, tx_height, tx_position = tx_rows[i]
        assert tx.hash()[0:HashXLength] == bytes.fromhex(tx_hash)[0:HashXLength]

        for idx, input in enumerate(tx.inputs):
            pd_hashes = get_pk_and_pkh_from_script(
                input.script_sig.to_bytes(),
                bytes.fromhex(tx_hash),
                idx,
                PushdataMatchFlags.INPUT,
                tx_pos=i,
                genesis_height=0,
            )
            if len(pd_hashes) != 0:
                pushdata_hashes_per_script.extend(pd_hashes)

        inputs.extend((tx.inputs))

        output_total_count += len(tx.outputs)
        txs.append(tx)

    with open(MODULE_DIR / "data" / "block413567_pushdata_hashes", "r") as file:
        correct_pushdata_hashes = file.readlines()
        expected = [line.strip() for line in correct_pushdata_hashes]
        actual = [
            hash_to_hex_str(pushdata_match.pushdata_hash) for pushdata_match in pushdata_hashes_per_script
        ]
        assert expected == actual

    print(f"Check for all tx hashes matching bitcoinx parsing: PASSED")
    assert len(txs) == len(tx_rows)

    # This is slow but I don't care!
    def scan_inputs_for_hash_and_idx_match(
        prev_out_hash: bytes, prev_idx: int, in_rows: list[InputRowParsed]
    ) -> bool:
        """Must reverse the hex rows endianness to match bitcoinx"""
        for row in in_rows:
            out_tx_hash, out_idx, in_tx_hash, in_idx = row
            if prev_out_hash == out_tx_hash and prev_idx == out_idx:
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
            bitcoinx_trusted_input.prev_hash,
            bitcoinx_trusted_input.prev_idx,
            in_rows,
        )
        assert match_found
    print(f"Check for all inputs matching bitcoinx parsing: PASSED")


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


def test_calc_mtree_base_level() -> None:
    tx_hashes = [b"aa" * 32, b"bb" * 32, b"cc" * 32, b"dd" * 32, b"ee" * 32]
    tx_count = len(tx_hashes)

    base_level_index = calc_depth(leaves_count=tx_count) - 1
    mtree = {base_level_index: tx_hashes}
    mtree = build_mtree_from_base(base_level_index, mtree)
    print(mtree)
    assert len(mtree.keys()) == 4
    assert mtree[3] == tx_hashes
    assert mtree[2] == [
        b"\x05\xc7\x8a-06rc>'\xd9\x01V\xd88\xeb$\xc8\xd1\xa7t}k\xc4\n\xc7\xebK\xf0\xb5\xfc!",
        b"n\xe5\xe6v\xc8\xbc\x96\xf7Z^\xfe\xaa$vJ\xbcZ\xdcE\x18\xa6\xd2;mh\xe7\x8e\xefX\x89S&",
        b"\xa8\x01\xc5\x942B\x94F\xbf\xa7\x92\xfaC\xc3t\x0b\xb7\\9\x95\x908\xe2\x06\xd7nD8\x8cF\xe7)",
    ]
    assert mtree[1] == [
        b"(r O\xf8U$\xbd\x1e\xc7\xea4\x8fE\xaf\xb5\xe1Mc\xbf\xddA\x15WX\x97\xa8\xb8\xf6\x8b\xfcR",
        b'fN\xc0\xbf\x8d\x82\xf5\r\xf3"I\xc3\xd2s0k\xf9"\xa0\x83\xd0\x0cN\xc1\xf7bO\x121\x03de',
    ]
    assert mtree[0] == [
        b"a\xf5\x15[\x11\xa4\xef`\xcc\xe8\x11t@r\x0e\xc9\x82\x12u\xbaO\x9d\xd2\xb7\x83\x8e\x1e\xb4\xa1\x0e\xfau"
    ]


def test_get_pk_and_pkh_from_script_1():
    """
    This rawtx is from mainnet and has:
     - a hodlocker time locking script at output index 0
        - i.e. it expects an unlocking script with format of: <signature> <pubkey> <preimage>
        and the pubkeyhash is buried after the OP_PUSH_TX validation logic (this particular
        implementation of OP_PUSH_TX is targeted at proving that the nlocktime is beyond a certain block height).
     - a 1SatOrdinals output at index 1
     - a standard p2pkh output at index 2
    """
    with open(
        MODULE_DIR / "data" / "bf88f55da5e4d000a08056d68ab6945077db2c5224512d088fbea57046de9e07.hex"
    ) as file:
        GENESIS_ACTIVATION_HEIGHT = 620_538
        tx = bitcoinx.Tx.from_bytes(bytes.fromhex(file.read()))
        assert tx.hex_hash() == "bf88f55da5e4d000a08056d68ab6945077db2c5224512d088fbea57046de9e07"

        # 3 public keys and 1 pubkey hash
        output0 = tx.outputs[0].script_pubkey
        pushdata_matches = get_pk_and_pkh_from_script(
            script=bytes(output0),
            tx_hash=tx.hash(),
            idx=0,
            flags=PushdataMatchFlags.OUTPUT,
            tx_pos=1,
            genesis_height=GENESIS_ACTIVATION_HEIGHT,
        )
        assert set(pushdata_matches) == {
            PushdataMatch(
                pushdata_hash=bytes.fromhex(
                    "56e426fb6ea4e8a81f51a9d0f3d83e869ac7215057e6aea2bc764d7a841b26bf"
                ),
                flags=PushdataMatchFlags.OUTPUT,
            ),
            PushdataMatch(
                pushdata_hash=bytes.fromhex(
                    "9eeb4618a48fac6e26544b46d811da8196f5cf277915ffdfbb922bda7bb407d6"
                ),
                flags=PushdataMatchFlags.OUTPUT,
            ),
            PushdataMatch(
                pushdata_hash=bytes.fromhex(
                    "c909eaa43779ae95cd36897f2ca0c5f55f695784ec856595b6bd4b443cbda740"
                ),
                flags=PushdataMatchFlags.OUTPUT,
            ),
            PushdataMatch(
                pushdata_hash=bytes.fromhex(
                    "5c1d6a6c1148fa41be81675e38f85744a1ac37517c17c6e4db6ce882fbe3b5cc"
                ),
                flags=PushdataMatchFlags.OUTPUT,
            ),
        }
        output1 = tx.outputs[1].script_pubkey
        pushdata_matches = get_pk_and_pkh_from_script(
            script=bytes(output1),
            tx_hash=tx.hash(),
            idx=1,
            flags=PushdataMatchFlags.OUTPUT,
            tx_pos=1,
            genesis_height=GENESIS_ACTIVATION_HEIGHT,
        )
        assert set(pushdata_matches) == {
            PushdataMatch(
                pushdata_hash=bytes.fromhex(
                    "57d59224ec387d77509f0416d778f76ee0145cc11a3f9bf12ef86564aab26861"
                ),
                flags=PushdataMatchFlags.OUTPUT,
            )
        }
        output2 = tx.outputs[2].script_pubkey
        pushdata_matches = get_pk_and_pkh_from_script(
            script=bytes(output2),
            tx_hash=tx.hash(),
            idx=2,
            flags=PushdataMatchFlags.OUTPUT,
            tx_pos=1,
            genesis_height=GENESIS_ACTIVATION_HEIGHT,
        )
        assert set(pushdata_matches) == {
            PushdataMatch(
                pushdata_hash=bytes.fromhex(
                    "9eeb4618a48fac6e26544b46d811da8196f5cf277915ffdfbb922bda7bb407d6"
                ),
                flags=PushdataMatchFlags.OUTPUT,
            )
        }

        assert bitcoinx.sha256(bytes.fromhex("9a81c9af89491144479b08030de42b0f7ca1dfa1")) == bytes.fromhex(
            "9eeb4618a48fac6e26544b46d811da8196f5cf277915ffdfbb922bda7bb407d6"
        )


def test_get_pk_and_pkh_from_script_2():
    """
    This is the last transaction in a 3GB block at height: 804157
    It contains 2,377,632 transactions and seems to be entirely of this type
    (Rekord IoT OP_FALSE OP_RETURN output scripts of zero spendable value)

    These contain 32 byte hashes which should not be detected. Only 20, 33 and 65 byte pushdatas.
    """
    tx_pos = 2377631
    with open(
        MODULE_DIR / "data" / "6370060cd55b046a87a72a4c07bffa1da52b3d12c227e795850b7d0ac68c203b.hex"
    ) as file:
        GENESIS_ACTIVATION_HEIGHT = 620_538
        tx = bitcoinx.Tx.from_bytes(bytes.fromhex(file.read()))
    assert tx.hex_hash() == "6370060cd55b046a87a72a4c07bffa1da52b3d12c227e795850b7d0ac68c203b"
    output0 = tx.outputs[0].script_pubkey
    pushdata_matches = get_pk_and_pkh_from_script(
        script=bytes(output0),
        tx_hash=tx.hash(),
        idx=0,
        flags=PushdataMatchFlags.OUTPUT,
        tx_pos=tx_pos,
        genesis_height=GENESIS_ACTIVATION_HEIGHT,
    )
    assert len(pushdata_matches) == 0


def test_get_pk_and_pkh_from_script_3():
    """This asserts that I am not indexing pushdata hashes after an OP_FALSE OP_RETURN"""
    # OP_PUSH_20 aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
    # OP_FALSE OP_RETURN OP_PUSH_20 bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
    output_script_hex = (
        "14aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa006a14bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    )
    GENESIS_ACTIVATION_HEIGHT = 620_538
    pushdata_matches = get_pk_and_pkh_from_script(
        script=bytes.fromhex(output_script_hex),
        tx_hash=os.urandom(32),
        idx=0,
        flags=PushdataMatchFlags.OUTPUT,
        tx_pos=0,
        genesis_height=GENESIS_ACTIVATION_HEIGHT,
    )
    assert len(pushdata_matches) == 1
    assert pushdata_matches[0].pushdata_hash == bitcoinx.sha256(
        bytes.fromhex("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
    )
