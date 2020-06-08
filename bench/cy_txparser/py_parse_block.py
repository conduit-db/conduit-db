import logging
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
SET_OTHER_PUSH_OPS = set(range(1, 76))

OP_PUSHDATA1 = 0x4c
OP_PUSHDATA2 = 0x4d
OP_PUSHDATA4 = 0x4e

logger = logging.getLogger("py_parse_block")


def unpack_varint(buf, offset):
    n = buf[offset]
    if n < 253:
        return n, offset + 1
    if n == 253:
        return struct_le_H.unpack_from(buf, offset + 1)[0], offset + 3
    if n == 254:
        return struct_le_I.unpack_from(buf, offset + 1)[0], offset + 5
    return struct_le_Q.unpack_from(buf, offset + 1)[0], offset + 9


def get_pk_and_pkh_from_script(script: bytearray, pks, pkhs):
    i = 0
    len_script = len(script)
    try:
        while i < len_script:
            if script[i] == 20:
                i += 1
                pkhs.add(struct_OP_20.unpack_from(script, i)[0])
                i += 20
            elif script[i] == 33:
                i += 1
                pks.add(struct_OP_33.unpack_from(script, i)[0])
                i += 33
            elif script[i] in SET_OTHER_PUSH_OPS:  # signature -> skip
                i += script[i] + 1
            elif script[i] == 0x4C:
                i += 1
                length = script[i]
                i += 1 + length
            elif script[i] == OP_PUSHDATA2:
                i += 1
                length = int.from_bytes(script[i:i+2], byteorder='little', signed=False)
                i += 2 + length
            elif script[i] == OP_PUSHDATA4:
                i += 1
                length = int.from_bytes(script[i:i+4], byteorder='little', signed=False)
                i += 4 + length
            else:  # slow search byte by byte...
                i += 1
    except Exception as e:
        logger.debug(f"script={script}, len(script)={len(script)}, i={i}")
        logger.exception(e)
        raise
    return pks, pkhs


def py_parse_block(raw_block, tx_offsets, height):
    tx_rows = []

    count_txs = len(tx_offsets)
    try:
        for index in range(count_txs):
            pks = set()
            pkhs = set()

            # tx_hash
            offset = tx_offsets[index]
            if index < count_txs - 1:
                next_tx_offset = tx_offsets[index + 1]
            else:
                next_tx_offset = len(raw_block)
            tx_hash = sha256(sha256(raw_block[offset:next_tx_offset]).digest()).digest()

            # version
            offset += 4

            # inputs
            count_tx_in, offset = unpack_varint(raw_block, offset)
            for i in range(count_tx_in):

                offset += 36  # skip (prev_out, idx)
                script_sig_len, offset = unpack_varint(raw_block, offset)
                script_sig = raw_block[offset : offset + script_sig_len]
                if not index == 0:  # some coinbase tx scriptsigs don't obey any rules.
                    pubkeys, pubkey_hashes = get_pk_and_pkh_from_script(
                        script_sig, pks, pkhs
                    )
                    if len(pubkeys):
                        pks.update(pubkeys)
                    if len(pubkey_hashes):
                        pkhs.update(pubkey_hashes)
                offset += script_sig_len
                offset += 4  # skip sequence

            # outputs
            count_tx_out, offset = unpack_varint(raw_block, offset)
            for i in range(count_tx_out):
                offset += 8  # skip value
                scriptpubkey_len, offset = unpack_varint(raw_block, offset)
                scriptpubkey = raw_block[offset : offset + scriptpubkey_len]
                pubkeys, pubkey_hashes = get_pk_and_pkh_from_script(
                    scriptpubkey, pks, pkhs
                )
                if len(pubkeys):
                    pks.update(pubkeys)
                if len(pubkey_hashes):
                    pkhs.update(pubkey_hashes)
                offset += scriptpubkey_len

            # nlocktime
            offset += 4

            tx_row = (tx_hash, pks, pkhs, height, offset)
            tx_rows.append(tx_row)
        assert len(tx_rows) == count_txs
        return tx_rows
    except Exception as e:
        logger.debug(f"count_txs={count_txs}")
        logger.exception(e)
        raise
