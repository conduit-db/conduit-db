import io
import time

import bitcoinx
from bitcoinx import read_varint


def seek_to_next_tx(stream):
    # version
    stream.seek(4, io.SEEK_CUR)

    # tx_in block
    count_tx_in = read_varint(stream.read)
    for i in range(count_tx_in):
        stream.seek(36, io.SEEK_CUR)  # prev_hash + prev_idx
        script_sig_len = read_varint(stream.read)
        stream.seek(script_sig_len, io.SEEK_CUR)  # script_sig
        stream.seek(4, io.SEEK_CUR)  # sequence

    # tx_out block
    count_tx_out = read_varint(stream.read)
    for i in range(count_tx_out):
        stream.seek(8, io.SEEK_CUR)  # value
        script_pubkey_len = read_varint(stream.read)  # script_pubkey
        stream.seek(script_pubkey_len, io.SEEK_CUR)  # script_sig

    # lock_time
    stream.seek(4, io.SEEK_CUR)
    return stream.tell()


def pre_processor(block_view):
    stream: io.BytesIO = io.BytesIO(block_view)
    stream.seek(80)  # header
    count = bitcoinx.read_varint(stream.read)
    tx_positions = [stream.tell()]  # start byte pos of each tx in the block
    for i in range(count - 1):
        tx_positions.append(seek_to_next_tx(stream))
    return tx_positions

def print_results(tx_positions, t1, block_view):
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
