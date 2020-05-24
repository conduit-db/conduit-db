# distutils: language = c++
# cython: language_level=3

from libcpp.vector cimport vector
from struct import Struct

cdef unsigned int HEADER_OFFSET = 80

cdef (unsigned long long, unsigned long long) unpack_varint(bytes buf, int offset) \
        except *:
    cdef int n
    n = buf[offset]
    if n < 253:
        return n, offset + 1
    if n == 253:
        return Struct('<H').unpack_from(buf, offset + 1)[0], offset + 3
    if n == 254:
        return Struct('<I').unpack_from(buf, offset + 1)[0], offset + 5
    return Struct('<Q').unpack_from(buf, offset + 1)[0], offset + 9


cpdef cy_preprocessor(bytes block_view, unsigned long long offset=0):
    cdef unsigned long long count, i, script_sig_len, script_pubkey_len

    offset += HEADER_OFFSET
    count, offset = unpack_varint(block_view, offset)

    cdef vector[unsigned long long] tx_positions  # start byte pos of each tx in the block
    tx_positions.push_back(offset)
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
        tx_positions.push_back(offset)
    return tx_positions

def print_results(tx_positions, t1, block_view):
    count = len(tx_positions)
    rate = (count/t1)
    av_tx_size = round(len(block_view) / count)
    bytes_per_sec = rate * av_tx_size
    MB_per_sec = round(bytes_per_sec/(1024*1024))

    print(
        f"block parsing took {round(t1, 5)} seconds for {count} txs and"
        f" {len(block_view)} "
        f"bytes - therefore {rate} txs per second for an average tx size "
        f"of {av_tx_size} bytes - therefore {MB_per_sec} MB/sec"
    )
