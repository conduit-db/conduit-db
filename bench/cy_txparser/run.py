import io
import time
import array

import bitcoinx

is_cython = False
try:
    from conduit_lib._algorithms import parse_txs, unpack_varint_cy  # cython
    is_cython = True
except ModuleNotFoundError:
    from conduit_lib.algorithms import parse_txs  # pure python
from bench.cy_txparser.offsets import TX_OFFSETS
from bench.utils import print_results


if __name__ == "__main__":
    with open("../data/block413567.raw", "rb") as f:
        raw_block = array.array('B', f.read())

    TX_OFFSETS = array.array('Q', TX_OFFSETS)

    t0 = time.perf_counter()
    for i in range(10):
        tx_rows, in_rows, out_rows, pd_rows = parse_txs(raw_block, TX_OFFSETS, 413567, True, 0)
    t1 = time.perf_counter() - t0
    print_results(len(tx_rows), t1/10, raw_block)

    # check validity
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
    t0 = time.perf_counter()
    for i in range(1557):
        tx = bitcoinx.Tx.read(stream.read)
        assert tx.hash() == bytes.fromhex(tx_rows[i][0])
        txs.append(tx)

    t1 = time.perf_counter() - t0
    # print_results(len(tx_rows), t1, raw_block)
    assert len(txs) == len(tx_rows)
