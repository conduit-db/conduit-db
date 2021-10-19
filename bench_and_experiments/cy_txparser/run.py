import io
import os
import time
import array
from pathlib import Path

import bitcoinx
from bitcoinx import hex_str_to_hash, hash_to_hex_str

from conduit_lib.constants import HashXLength

is_cython = False
try:
    from conduit_lib._algorithms import parse_txs, unpack_varint_cy, get_pk_and_pkh_from_script  # cython
    is_cython = True
except ModuleNotFoundError:
    from conduit_lib.algorithms import parse_txs, unpack_varint, get_pk_and_pkh_from_script  # pure python
from bench_and_experiments.cy_txparser.offsets import TX_OFFSETS
from bench_and_experiments.utils import print_results

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))

if __name__ == "__main__":

    # Check unpack_varint vs unpack_varint_cy if is_cython = True:
    if is_cython:
        for i in range(1, 1_000_000):
            stream = io.BytesIO()
            varint_buf = array.array('B', bitcoinx.pack_varint(i))
            parsed_varint = unpack_varint_cy(varint_buf, 0)[0]
            assert parsed_varint == i, f"parsed_varint: {parsed_varint} != {i}"
        print("Check for varint parsing (cython): PASSED")
    else:
        for i in range(1, 1_000_000):
            stream = io.BytesIO()
            varint_buf = array.array('B', bitcoinx.pack_varint(i))
            parsed_varint = unpack_varint(varint_buf, 0)[0]
            assert parsed_varint == i, f"parsed_varint: {parsed_varint} != {i}"
        print("Check for varint parsing (pure python): PASSED")


    # path_to_test_blockchain = Path(MODULE_DIR).parent.parent \
    #     / 'contrib' / 'blockchains' / 'blockchain_115_3677f4'

    with open("../data/block413567.raw", "rb") as f:
        raw_block = array.array('B', f.read())

    TX_OFFSETS = array.array('Q', TX_OFFSETS)

    t0 = time.perf_counter()
    for i in range(10):
        tx_rows, in_rows, out_rows, pd_rows = parse_txs(raw_block, TX_OFFSETS, 413567, True, 0)
    t1 = time.perf_counter() - t0
    print_results(len(tx_rows), t1/10, raw_block)

    # -----------------------------  check validity ------------------------------------- #
    stream = io.BytesIO(raw_block.tobytes())

    stream.read(80)
    tx_count = bitcoinx.read_varint(stream.read)
    assert tx_count == 1557

    if is_cython:
        tx_count_cy, _offset = unpack_varint_cy(raw_block[80:83], 0)
        assert tx_count == tx_count_cy

        num1, _offset = unpack_varint_cy(array.array('B', [253, 1, 0]), 0)
        assert num1 == 1

        num2, _offset = unpack_varint_cy(array.array('B', [254, 1, 0, 0, 0]), 0)
        assert num2 == 1

    txs = []
    inputs = []
    output_total_count = 0
    for i in range(1557):
        tx = bitcoinx.Tx.read(stream.read)
        tx_hash, tx_height, tx_position = tx_rows[i]
        assert tx.hash()[0:HashXLength] == bytes.fromhex(tx_hash)[0:HashXLength]
        inputs.extend(tx.inputs)
        output_total_count += len(tx.outputs)
        txs.append(tx)


    pushdata_hashes_per_script = []
    for input in inputs:
        pd_hashes = get_pk_and_pkh_from_script(array.array('B', input.script_sig.to_bytes()))
        pushdata_hashes_per_script.append(pd_hashes)
        # print(len(pd_hashes))


    with open('pushdata_hashes_cython', 'w') as f:
        for pd_hashes in pushdata_hashes_per_script:
            if pd_hashes:
                f.write(",".join([hash_to_hex_str(x) for x in pd_hashes]) + "\n")


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
