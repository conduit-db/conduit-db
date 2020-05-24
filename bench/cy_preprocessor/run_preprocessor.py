import io
import time
import bitcoinx

from cy_preprocessor import print_results, cy_preprocessor

if __name__ == "__main__":
    with open("../data/block413567.raw", "rb") as f:
        raw_block = f.read()

    t0 = time.time()
    for i in range(100):
        tx_positions = cy_preprocessor(raw_block)
    t1 = time.time() - t0
    print_results(tx_positions, t1/100, raw_block)

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
