from struct import Struct


def unpack_varint(buf, offset):
    n = buf[offset]
    if n < 253:
        return n, offset + 1
    if n == 253:
        return Struct('<H').unpack_from(buf, offset + 1)[0], offset + 3
    if n == 254:
        return Struct('<I').unpack_from(buf, offset + 1)[0], offset + 5
    return Struct('<Q').unpack_from(buf, offset + 1)[0], offset + 9


def parse_block(raw_block, tx_offsets, height):
    tx_rows = []
    tx_hash = b'x'*32
    for index in range(len(tx_offsets)):
        offset = tx_offsets[index]
        offset += 4  # skip version
        count_tx_in, offset = unpack_varint(raw_block, offset)
        for i in range(count_tx_in):
            offset += 36  # skip (prev_out, idx)
            script_sig_len, offset = unpack_varint(raw_block, offset)
            script_sig = raw_block[offset: offset + script_sig_len]
            offset += script_sig_len
            offset += 4  # skip sequence

        count_tx_out, offset = unpack_varint(raw_block, offset)
        for i in range(count_tx_out):
            offset += 8  # skip value
            scriptpubkey_len, offset = unpack_varint(raw_block, offset)
            scriptpubkey = raw_block[offset: offset + scriptpubkey_len]
        offset += 4  # nlocktime
        tx_row = (tx_hash, script_sig, scriptpubkey, height, offset)
        tx_rows.append(tx_row)
    # print(tx_rows)
    return tx_rows
