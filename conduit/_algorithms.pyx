# distutils: language = c++
# cython: language_level=3

from libcpp.vector cimport vector
from struct import Struct
from hashlib import sha256
from libcpp.set cimport set

cdef unsigned int HEADER_OFFSET = 80
cdef unsigned char OP_PUSH_20 = 20
cdef unsigned char OP_PUSH_33 = 33
cdef set[int] SET_OTHER_PUSH_OPS
cdef unsigned char i
for i in range(1,75):
    SET_OTHER_PUSH_OPS.insert(i)

struct_le_H = Struct('<H')
struct_le_I = Struct('<I')
struct_le_Q = Struct('<Q')
struct_OP_20 = Struct('<20s')
struct_OP_33 = Struct('<33s')


cpdef unpack_varint(buf, unsigned long long offset):
    # less heavily cythonized version
    cdef int n
    cdef unsigned char byte
    byte = buf[offset]
    n = byte
    if n < 253:
        return n, offset + 1
    if n == 253:
        return struct_le_H.unpack_from(buf, offset + 1)[0], offset + 3
    if n == 254:
        return struct_le_I.unpack_from(buf, offset + 1)[0], offset + 5
    return struct_le_Q.unpack_from(buf, offset + 1)[0], offset + 9


cdef (unsigned long long, unsigned long long) unpack_varint_preprocessor(bytes buf, int offset) \
        except *:
    # more heavily cythonized version
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
    count, offset = unpack_varint_preprocessor(block_view, offset)

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


cpdef get_pk_and_pkh_from_script(script: bytearray):
    cdef int i, len_script
    cdef list pks = []
    cdef list pkhs = []
    i = 0
    len_script = len(script)
    while i < len_script:
        if script[i] == 20:
            pkhs.append(struct_OP_20.unpack_from(script, i))
            i += 20 + 1
        elif script[i] == 33:
            pks.append(struct_OP_33.unpack_from(script, i))
            i += 33 + 1
        elif SET_OTHER_PUSH_OPS.find(script[i]) != SET_OTHER_PUSH_OPS.end():  # signature -> skip
            i += script[i] + 1
        # elif ... OP_PUSHDATA1,2,4... # Todo - this is broken otherwise...
        else:  # slow search byte by byte...
            i += 1
    return pks, pkhs


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
            pks, pkhs = get_pk_and_pkh_from_script(script_sig)
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

        tx_row = (tx_hash, pks, pkhs, scriptpubkey, height, offset)
        tx_rows.append(tx_row)
    assert len(tx_rows) == count_txs
    return tx_rows
