import io
import time
from struct import Struct
from typing import Tuple

import bitcoinx
from bitcoinx import read_varint, read_le_uint32, read_le_uint64

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

def print_results(tx_positions, t1, block_view):
    print(tx_positions)
    count = len(tx_positions)
    rate = round(count/t1)
    av_tx_size = round(len(block_view) / count)
    bytes_per_sec = rate * av_tx_size
    MB_per_sec = round(bytes_per_sec/(1024*1024))

    print(
        f"block parsing took {round(t1, 5)} seconds for {count} txs and"
        f" {len(block_view)} "
        f"bytes - therefore {rate} txs per second for an average tx size "
        f"of {av_tx_size} bytes - therefore {MB_per_sec} MB/sec"
    )


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
