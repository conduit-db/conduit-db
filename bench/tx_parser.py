import io
import time
import bitcoinx

from bench.utils import print_results

"""This involves conversion to python objects and reading unnecessary bytes (such 
as the 'version' field so is quite suboptimal. Left here as a baseline throughput 
rate."""

def parse_block(block_view):
    # raw_header = block_view[0:80]
    block_subview = block_view[80:]
    stream: io.BytesIO = io.BytesIO(block_subview)

    txs = []
    count = bitcoinx.read_varint(stream.read)
    for i in range(count):
        tx = bitcoinx.Tx.read(stream.read)
        txs.append(tx)
    return count

if __name__ == "__main__":
    with open("data/block413567.raw", "rb") as f:
        raw_block = f.read()
        block_view = memoryview(raw_block)

    t0 = time.time()
    count = parse_block(block_view)
    t1 = time.time() - t0

    print_results(count, t1, block_view)

"""
block parsing took 0.02493 seconds for 1557 txs and 999887 bytes - 
therefore 62446 txs per second for an average tx size of 642 bytes - 
therefore 38 MB/sec
"""
