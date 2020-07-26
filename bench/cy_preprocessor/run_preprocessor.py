import io
import time
import bitcoinx
try:
    from workers._algorithms import preprocessor  # cython
except ModuleNotFoundError:
    from workers.algorithms import preprocessor  # pure python
from bench.utils import print_results

if __name__ == "__main__":

    with open("../data/block413567.raw", "rb") as f:
        raw_block = bytearray(f.read())

    t0 = time.time()
    for i in range(100):
        tx_positions = preprocessor(raw_block)
        # tx_positions = preprocessor(raw_block)
    t1 = time.time() - t0
    print_results(len(tx_positions), t1/100, raw_block)

    # check validity
    t0 = time.time()
    stream = io.BytesIO(raw_block)
    txs = []
    for pos in tx_positions:
        stream.seek(pos)
        txs.append(bitcoinx.Tx.read(stream.read))

    t1 = time.time() - t0
    print(t1)
    assert len(txs) == len(tx_positions)
