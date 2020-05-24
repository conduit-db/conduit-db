import struct
from struct import Struct
from hashlib import sha256

struct_le_H = Struct('<H')
struct_le_I = Struct('<I')
struct_le_Q = Struct('<Q')
struct_OP_20 = Struct('<20s')
struct_OP_33 = Struct('<33s')
OP_PUSH_20 = struct.pack('B', 20)
OP_PUSH_33 = struct.pack('B', 33)
SET_OTHER_PUSH_OPS = set(range(1, 75))

def unpack_varint(buf, offset):
    n = buf[offset]
    if n < 253:
        return n, offset + 1
    if n == 253:
        return struct_le_H.unpack_from(buf, offset + 1)[0], offset + 3
    if n == 254:
        return struct_le_I.unpack_from(buf, offset + 1)[0], offset + 5
    return struct_le_Q.unpack_from(buf, offset + 1)[0], offset + 9


def get_pk_and_pkh_from_script(script: bytearray):
    i = 0
    pks = []
    pkhs = []
    len_script = len(script)
    while i < len_script:
        if script[i] == 20:
            pkhs.append(struct_OP_20.unpack_from(script, i))
            i += 20 + 1
        elif script[i] == 33:
            pks.append(struct_OP_33.unpack_from(script, i))
            i += 33 + 1
        elif script[i] in SET_OTHER_PUSH_OPS:  # signature -> skip
            i += script[i] + 1
        # elif ... OP_PUSHDATA1,2,4... # Todo - this is broken otherwise...
        else:  # slow search byte by byte...
            i += 1
    return pks, pkhs


def parse_block(raw_block, tx_offsets, height):
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
