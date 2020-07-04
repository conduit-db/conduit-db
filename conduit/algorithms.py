"""slower pure python alternative"""

import bitcoinx
from struct import Struct
from hashlib import sha256

from bitcoinx import double_sha256

from .logs import logs

HEADER_OFFSET = 80
OP_PUSH_20 = 20
OP_PUSH_33 = 33
OP_PUSH_65 = 65
SET_OTHER_PUSH_OPS = set(range(1, 76))

struct_le_H = Struct("<H")
struct_le_I = Struct("<I")
struct_le_Q = Struct("<Q")
struct_le_q = Struct("<q")  # for short_hashes
struct_OP_20 = Struct("<20s")
struct_OP_33 = Struct("<33s")
struct_OP_65 = Struct("<65s")


OP_PUSHDATA1 = 0x4C
OP_PUSHDATA2 = 0x4D
OP_PUSHDATA4 = 0x4E

logger = logs.get_logger("algorithms")


def unpack_varint(buf, offset):
    n = buf[offset]
    if n < 253:
        return n, offset + 1
    if n == 253:
        return struct_le_H.unpack_from(buf, offset + 1)[0], offset + 3
    if n == 254:
        return struct_le_I.unpack_from(buf, offset + 1)[0], offset + 5
    return struct_le_Q.unpack_from(buf, offset + 1)[0], offset + 9


def preprocessor(block_view, offset=0):
    offset += HEADER_OFFSET
    count, offset = unpack_varint(block_view, offset)

    tx_positions = []  # start byte pos of each tx in the block
    tx_positions.append(offset)
    for i in range(count - 1):
        # version
        offset += 4

        # tx_in block
        count_tx_in, offset = unpack_varint(block_view, offset)
        for i in range(count_tx_in):
            offset += 36  # prev_hash + prev_idx
            script_sig_len, offset = unpack_varint(block_view, offset)
            offset += script_sig_len
            offset += 4  # sequence

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


def get_pk_and_pkh_from_script(script: bytearray, pks, pkhs):
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
            elif script[i] in SET_OTHER_PUSH_OPS:  # signature -> skip
                i += script[i] + 1
            elif script[i] == 0x4C:
                try:
                    i += 1
                    length = script[i]
                    i += 1 + length
                except IndexError as e:
                    # This can legitimately happen (bad output scripts...) e.g. see:
                    # ebc9fa1196a59e192352d76c0f6e73167046b9d37b8302b6bb6968dfd279b767
                    logger.error(f"script={script}, len(script)={len(script)}, i={i}")
                    logger.exception(e)
            elif script[i] == OP_PUSHDATA2:
                i += 1
                length = int.from_bytes(script[i : i + 2], byteorder="little", signed=False)
                i += 2 + length
            elif script[i] == OP_PUSHDATA4:
                i += 1
                length = int.from_bytes(script[i : i + 4], byteorder="little", signed=False)
                i += 4 + length
            else:  # slow search byte by byte...
                i += 1
        # hash pushdata
        if len(pks) == 1:
            pd_hashes.append(sha256(pks.pop()).digest()[0:32])  # skip for loop if possible
        else:
            for pk in pks:
                pd_hashes.append(sha256(pk).digest()[0:32])

        if len(pkhs) == 1:
            pd_hashes.append(sha256(pkhs.pop()).digest()[0:32])  # skip for loop if possible
        else:
            for pkh in pkhs:
                pd_hashes.append(sha256(pkh).digest()[0:32])
        return pd_hashes
    except Exception as e:
        logger.debug(f"script={script}, len(script)={len(script)}, i={i}")
        logger.exception(e)
        raise


def parse_block(raw_block, tx_offsets, height):
    """
    returns
        tx_rows =       [(tx_shash, tx_hash, height, position, offset_start, offset_end, tx_has_collided)...]
        in_rows =       [(prevout_shash, out_idx, tx_shash, in_idx, out_has_collided)...)...]
        out_rows =      [(out_tx_shash, idx, value, in_has_collided)...)]
        pd_rows =       [(pushdata_hash, pushdata_shash, tx_shash, idx, ref_type=0 or 1,
            pd_has_collided)...]

        NOTE: full pushdata_hash is not committed to the database but is temporarily included in
        these rows in case of a collision -> committed to collision table.
    """
    tx_rows = []
    # sets rule out possibility of duplicate pushdata in same input / output
    in_rows = set()
    out_rows = set()
    set_pd_rows = set()
    count_txs = len(tx_offsets)

    tx_has_collided = False
    out_has_collided = False
    in_has_collided = False
    pd_has_collided = False
    try:
        for position in range(count_txs):
            pks = set()
            pkhs = set()

            # tx_hash
            offset = tx_offsets[position]
            tx_offset_start = offset
            if position < count_txs - 1:
                next_tx_offset = tx_offsets[position + 1]
            else:
                next_tx_offset = len(raw_block)
            tx_hash = double_sha256(raw_block[tx_offset_start:next_tx_offset])
            tx_shash = struct_le_q.unpack(tx_hash[0:8])[0]

            # version
            offset += 4

            # inputs
            count_tx_in, offset = unpack_varint(raw_block, offset)
            ref_type = 1
            for in_idx in range(count_tx_in):
                in_prevout_hash = raw_block[offset : offset + 32]
                in_prevout_shash = struct_le_q.unpack(in_prevout_hash[0:8])[0]
                offset += 32
                in_prevout_idx = struct_le_I.unpack_from(raw_block[offset : offset + 4])[0]
                offset += 4
                script_sig_len, offset = unpack_varint(raw_block, offset)
                script_sig = raw_block[offset : offset + script_sig_len]
                # some coinbase tx scriptsigs don't obey any rules.
                if not position == 0:

                    in_rows.add(
                        (in_prevout_shash, in_prevout_idx, tx_shash, in_idx, in_has_collided,),
                    )

                    pushdata_hashes = get_pk_and_pkh_from_script(script_sig, pks, pkhs)
                    if len(pushdata_hashes):
                        for in_pushdata_hash in pushdata_hashes:
                            # Todo - in_pushdata_hash needs to be kept in memory for collisions
                            in_pushdata_shash = struct_le_q.unpack(in_pushdata_hash[0:8])[0]
                            set_pd_rows.add(
                                (
                                    in_pushdata_shash,
                                    in_pushdata_hash,
                                    tx_shash,
                                    in_idx,
                                    ref_type,
                                    pd_has_collided,
                                )
                            )
                offset += script_sig_len
                offset += 4  # skip sequence

            # outputs
            count_tx_out, offset = unpack_varint(raw_block, offset)
            ref_type = 0
            for out_idx in range(count_tx_out):
                out_value = struct_le_Q.unpack_from(raw_block[offset : offset + 8])[0]
                offset += 8  # skip value
                scriptpubkey_len, offset = unpack_varint(raw_block, offset)
                scriptpubkey = raw_block[offset : offset + scriptpubkey_len]

                out_rows.add((tx_shash, out_idx, out_value, out_has_collided, None, None, None,))

                pushdata_hashes = get_pk_and_pkh_from_script(scriptpubkey, pks, pkhs)
                if len(pushdata_hashes):
                    for out_pushdata_hash in pushdata_hashes:
                        # Todo - out_pushdata_hash needs to be kept in memory for collisions...
                        out_pushdata_shash = struct_le_q.unpack(out_pushdata_hash[0:8])[0]
                        set_pd_rows.add(
                            (out_pushdata_shash, out_pushdata_hash, tx_shash, out_idx, ref_type,
                            pd_has_collided,)
                        )
                offset += scriptpubkey_len

            # nlocktime
            offset += 4

            # NOTE: when partitioning blocks ensure position is correct!
            tx_rows.append(
                (
                    tx_shash,
                    tx_hash,
                    height,
                    position,
                    tx_offset_start,
                    next_tx_offset,
                    tx_has_collided,
                )
            )
        assert len(tx_rows) == count_txs
        return tx_rows, in_rows, out_rows, set_pd_rows
    except Exception as e:
        logger.debug(
            f"count_txs={count_txs}, position={position}, in_idx={in_idx}, out_idx={out_idx}, "
            f"txid={bitcoinx.hash_to_hex_str(tx_hash)}"
        )
        logger.exception(e)
        raise
