# distutils: language = c++
# cython: language_level=3

from struct import Struct
from hashlib import sha256

cpdef unpack_varint(buf, unsigned long long offset):
    cdef int n
    cdef unsigned char byte
    byte = buf[offset]
    n = byte
    if n < 253:
        return n, offset + 1
    if n == 253:
        return Struct('<H').unpack_from(buf, offset + 1)[0], offset + 3
    if n == 254:
        return Struct('<I').unpack_from(buf, offset + 1)[0], offset + 5
    return Struct('<Q').unpack_from(buf, offset + 1)[0], offset + 9


cpdef list cy_parse_block(raw_block, tx_offsets, int height):
    cdef int index, i
    cdef int offset, count_tx_out, count_tx_in
    cdef list tx_rows
    tx_rows = []

    count_txs = len(tx_offsets)
    for index in range(count_txs):
        # tx_hash
        offset = tx_offsets[index]
        if index < count_txs - 1:
            next_tx_offset = tx_offsets[index + 1]
        else:
            next_tx_offset = len(raw_block)
        tx_hash = sha256(sha256(raw_block[offset: next_tx_offset]).digest()).digest()

        # version
        offset += 4

        # inputs
        count_tx_in, offset = unpack_varint(raw_block, offset)
        for i in range(count_tx_in):
            offset += 36  # skip (prev_out, idx)
            script_sig_len, offset = unpack_varint(raw_block, offset)
            script_sig = raw_block[offset: offset + script_sig_len]
            offset += script_sig_len
            offset += 4  # skip sequence

        # outputs
        count_tx_out, offset = unpack_varint(raw_block, offset)
        for i in range(count_tx_out):
            offset += 8  # skip value
            scriptpubkey_len, offset = unpack_varint(raw_block, offset)
            scriptpubkey = raw_block[offset: offset + scriptpubkey_len]

        # nlocktime
        offset += 4

        tx_row = (tx_hash, script_sig, scriptpubkey, height, offset)
        tx_rows.append(tx_row)
    assert len(tx_rows) == count_txs
    return tx_rows
