import bitcoinx
import io
import time
from struct import Struct
from typing import Tuple

from bench.utils import print_results

HEADER_OFFSET = 80

def unpack_varint(buf: memoryview, offset: int) -> Tuple[int, int]:
    n, = Struct('B').unpack_from(buf, offset)  # 1 byte
    if n < 253:
        return n, offset + 1
    if n == 253:
        return Struct('<H').unpack_from(buf, offset + 1)[0], offset + 3
    if n == 254:
        return Struct('<I').unpack_from(buf, offset + 1)[0], offset + 5
    return Struct('<Q').unpack_from(buf, offset + 1)[0], offset + 9


def pre_processor(block_view, offset=0):
    offset += HEADER_OFFSET
    count, offset = unpack_varint(block_view, offset)
    tx_positions = [offset]  # start byte pos of each tx in the block
    for i in range(count - 1):
        # version
        offset += 4

        # tx_in block
        count_tx_in, offset = unpack_varint(block_view, offset)
        for i in range(count_tx_in):
            offset += 36  # prev_hash + prev_idx
            script_sig_len, offset = unpack_varint(block_view, offset)
            offset += script_sig_len
            offset += 4 # sequence

        # tx_out block
        count_tx_out, offset = unpack_varint(block_view, offset)
        for i in range(count_tx_out):
            offset += 8  # value
            script_pubkey_len, offset = unpack_varint(block_view, offset)  # script_pubkey
            offset += script_pubkey_len  # script_sig

        # lock_time
        offset += 4
        tx_positions.append(offset)
    return tx_positions

if __name__ == "__main__":
    with open("data/block413567.raw", "rb") as f:
        raw_block = f.read()
        block_view = memoryview(raw_block)

    t0 = time.time()
    tx_positions = pre_processor(block_view)
    t1 = time.time() - t0

    print_results(tx_positions, t1, block_view)

    # check validity
    t0 = time.time()
    stream = io.BytesIO(block_view)
    txs = []
    for pos in tx_positions:
        stream.seek(pos)
        txs.append(bitcoinx.Tx.read(stream.read))

    t1 = time.time() - t0
    print(t1)
    assert len(txs) == len(tx_positions)
