"""To compile the Cython extension read the top-level setup.py for instructions"""

import array
import io
import time
import bitcoinx
try:
    from conduit_lib._algorithms import preprocessor  # cython
except ModuleNotFoundError:
    from conduit_lib.algorithms import preprocessor  # pure python
from bench.utils import print_results

if __name__ == "__main__":

    with open("../data/block413567.raw", "rb") as f:
        raw_block = array.array('B', f.read())

    tx_offsets_array = array.array("Q", [i for i in range(1_000_000)])

    t0 = time.perf_counter()
    count = 0
    for i in range(100):
        count, tx_offsets_array = preprocessor(raw_block, tx_offsets_array)
    t1 = time.perf_counter() - t0
    print_results(count, t1/100, raw_block)

    # check validity
    t0 = time.time()
    stream = io.BytesIO(raw_block)
    txs = []
    for i in range(count):
        stream.seek(tx_offsets_array[i])
        txs.append(bitcoinx.Tx.read(stream.read))

    t1 = time.time() - t0
    assert len(txs) == count
    # print_results(count, t1, raw_block)
