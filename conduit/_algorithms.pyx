# distutils: language = c++
# cython: language_level=3
"""cythonized version of algorithms.py"""
import bitcoinx

from libcpp.vector cimport vector
from struct import Struct
from hashlib import sha256
from libcpp.set cimport set as cppset

from .logs import logs

cdef unsigned int HEADER_OFFSET = 80
cdef unsigned char OP_PUSH_20 = 20
cdef unsigned char OP_PUSH_33 = 33
cdef unsigned char OP_PUSH_65 = 65
cdef cppset[int] SET_OTHER_PUSH_OPS
cdef unsigned char i
for i in range(1,76):
    SET_OTHER_PUSH_OPS.insert(i)

struct_le_H = Struct('<H')
struct_le_I = Struct('<I')
struct_le_Q = Struct('<Q')
struct_OP_20 = Struct('<20s')
struct_OP_33 = Struct('<33s')
struct_OP_65 = Struct("<65s")


OP_PUSHDATA1 = 0x4c
OP_PUSHDATA2 = 0x4d
OP_PUSHDATA4 = 0x4e

logger = logs.get_logger("_algorithms")


def unpack_varint(buf, offset):
    n = buf[offset]
    if n < 253:
        return n, offset + 1
    if n == 253:
        return struct_le_H.unpack_from(buf, offset + 1)[0], offset + 3
    if n == 254:
        return struct_le_I.unpack_from(buf, offset + 1)[0], offset + 5
    return struct_le_Q.unpack_from(buf, offset + 1)[0], offset + 9


cdef (unsigned long long, unsigned long long) unpack_varint_preprocessor(bytearray buf,
        int offset) \
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



cpdef preprocessor(bytearray block_view, unsigned long long offset=0):
    cdef unsigned long long count, i, script_sig_len, script_pubkey_len

    offset += HEADER_OFFSET
    count, offset = unpack_varint_preprocessor(block_view, offset)

    cdef vector[unsigned long long] tx_positions  # start byte pos of each tx in the block
    tx_positions.push_back(offset)
    for i in range(count - 1):
        # version
        offset += 4

        # tx_in block
        count_tx_in, offset = unpack_varint_preprocessor(block_view, offset)
        for i in range(count_tx_in):
            offset += 36  # prev_hash + prev_idx
            script_sig_len, offset = unpack_varint_preprocessor(block_view, offset)
            offset += script_sig_len
            offset += 4 # sequence

        # tx_out block
        count_tx_out, offset = unpack_varint_preprocessor(block_view, offset)
        for i in range(count_tx_out):
            offset += 8  # value
            script_pubkey_len, offset = unpack_varint_preprocessor(block_view, offset)  # script_pubkey
            offset += script_pubkey_len  # script_sig

        # lock_time
        offset += 4
        tx_positions.push_back(offset)
    return tx_positions


cpdef get_pk_and_pkh_from_script(bytearray script, set pks, set pkhs):
    cdef unsigned long long i, len_script
    i = 0
    pd_hashes = []
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
            elif script[i] == 65:
                i += 1
                pks.add(struct_OP_65.unpack_from(script, i)[0])
                i += 65
            elif SET_OTHER_PUSH_OPS.find(script[i]) != SET_OTHER_PUSH_OPS.end():  # signature -> skip
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
        # hash pushdata
        if len(pks) == 1:
            pd_hashes.append(sha256(pks.pop()).digest()[0:20])  # skip for loop if possible
        else:
            for pk in pks:
                pd_hashes.append(sha256(pk).digest()[0:20])

        if len(pkhs) == 1:
            pd_hashes.append(sha256(pkhs.pop()).digest()[0:20])  # skip for loop if possible
        else:
            for pkh in pkhs:
                pd_hashes.append(sha256(pkh).digest()[0:20])
        return pd_hashes
    except Exception as e:
        logger.debug(f"script={script}, len(script)={len(script)}, i={i}")
        logger.exception(e)
        raise


cpdef parse_block(bytearray raw_block, list tx_offsets, unsigned int height):
    """
    returned rows:
    - tx_rows:      [("tx_hash", "height", "tx_position", "tx_offset")]
    - in_rows:      [("in_prevout_hash", "in_prevout_idx", "in_pushdata_hash", "in_idx", "in_tx_hash")...]
    - out_rows:     [("out_tx_hash", "out_idx", "out_pushdata_hash", "out_value")...]
    """
    cdef unsigned int position
    cdef unsigned long long offset, next_tx_offset, count_txs, count_tx_in, input, output, \
        count_tx_out, script_sig_len, scriptpubkey_len
    cdef set pks, pkhs
    cdef list tx_rows, in_rows, out_rows
    cdef tuple tx_row

    tx_rows = []
    in_rows = []
    out_rows = []
    count_txs = len(tx_offsets)
    try:
        for position in range(count_txs):
            pks = set()
            pkhs = set()

            # tx_hash
            offset = tx_offsets[position]
            if position < count_txs - 1:
                next_tx_offset = tx_offsets[position + 1]
            else:
                next_tx_offset = len(raw_block)
            tx_hash = sha256(sha256(raw_block[offset:next_tx_offset]).digest()).digest()[0:20]

            # version
            offset += 4

            # inputs
            count_tx_in, offset = unpack_varint(raw_block, offset)
            for in_idx in range(count_tx_in):
                in_prevout_hash = raw_block[offset : offset + 20]
                offset += 32
                in_prevout_idx = struct_le_I.unpack_from(
                    raw_block[offset : offset + 4]
                )[0]
                offset += 4
                script_sig_len, offset = unpack_varint(raw_block, offset)
                script_sig = raw_block[offset : offset + script_sig_len]
                # some coinbase tx scriptsigs don't obey any rules.
                if not position == 0:
                    pushdata_hashes = get_pk_and_pkh_from_script(script_sig, pks, pkhs)
                    if len(pushdata_hashes):
                        for in_pushdata_hash in pushdata_hashes:
                            in_rows.append(
                                (
                                    in_prevout_hash,
                                    in_prevout_idx,
                                    in_pushdata_hash,
                                    in_idx,
                                    tx_hash,
                                )
                            )
                offset += script_sig_len
                offset += 4  # skip sequence

            # outputs
            count_tx_out, offset = unpack_varint(raw_block, offset)
            for out_idx in range(count_tx_out):
                out_value = struct_le_Q.unpack_from(raw_block[offset : offset + 8])[0]
                offset += 8  # skip value
                scriptpubkey_len, offset = unpack_varint(raw_block, offset)
                scriptpubkey = raw_block[offset : offset + scriptpubkey_len]
                pushdata_hashes = get_pk_and_pkh_from_script(scriptpubkey, pks, pkhs)
                if len(pushdata_hashes):
                    for out_pushdata_hash in pushdata_hashes:
                        out_rows.append((tx_hash, out_idx, out_pushdata_hash, out_value))
                offset += scriptpubkey_len

            # nlocktime
            offset += 4

            tx_row = (tx_hash, height, position, offset)
            tx_rows.append(tx_row)
        assert len(tx_rows) == count_txs
        return tx_rows, in_rows, out_rows
    except Exception as e:
        logger.debug(f"count_txs={count_txs}, position={position}, input={input}, output={output}, "
                     f"txid={bitcoinx.hash_to_hex_str(tx_hash)}")
        logger.exception(e)
        raise
