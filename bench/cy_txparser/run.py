import time

import bitcoinx
from bench.cy_txparser.txparser import cy_parse_block
from bench.cy_txparser.offsets import TX_OFFSETS
from bench.cy_txparser.py_txparse import parse_block
from bench.utils import print_results
import array
from hashlib import sha256

# def get_tx_hashes(raw_block, tx_offsets):
#     tx_hashes = []
#     count_txs = len(tx_offsets)
#     for index in range(count_txs):
#         offset = tx_offsets[index]
#         if index < count_txs - 1:
#             print(index, count_txs)
#             next_tx_offset = tx_offsets[index + 1]
#             tx_hash = sha256(sha256(raw_block[offset: next_tx_offset]).digest()).digest()
#             tx_hashes.append(tx_hash)
#     return tx_hashes


if __name__ == "__main__":
    with open("../data/block413567.raw", "rb") as f:
        raw_block = bytearray(f.read())

    t0 = time.time()
    for i in range(10):
        tx_rows = cy_parse_block(raw_block, TX_OFFSETS, 413567)
        # tx_rows = parse_block(raw_block, TX_OFFSETS, 413567)
    t1 = time.time() - t0
    print(len(tx_rows))
    print_results(len(tx_rows), t1/10, raw_block)
