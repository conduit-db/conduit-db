import time

try:
    from conduit._algorithms import parse_block  # cython
except ModuleNotFoundError:
    from conduit.algorithms import parse_block  # pure python
from bench.cy_txparser.offsets import TX_OFFSETS
from bench.utils import print_results


if __name__ == "__main__":
    with open("../data/block413567.raw", "rb") as f:
        raw_block = bytearray(f.read())

    t0 = time.time()
    for i in range(10):
        tx_rows, in_rows, out_rows = parse_block(raw_block, TX_OFFSETS, 413567)
    t1 = time.time() - t0
    print_results(len(tx_rows), t1/10, raw_block)
