"""slower pure python alternative"""
import logging
import struct
from datetime import datetime
from typing import Dict, Union, List

import bitcoinx
from bitcoinx import double_sha256, hash_to_hex_str
from struct import Struct
from hashlib import sha256

from conduit.logging_client import setup_tcp_logging

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

logger = logging.getLogger("algorithms")


def unpack_varint(buf, offset):
    n = buf[offset]
    if n < 253:
        return n, offset + 1
    if n == 253:
        return struct_le_H.unpack_from(buf, offset + 1)[0], offset + 3
    if n == 254:
        return struct_le_I.unpack_from(buf, offset + 1)[0], offset + 5
    return struct_le_Q.unpack_from(buf, offset + 1)[0], offset + 9


# -------------------- PREPROCESSOR -------------------- #


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


# -------------------- PARSE BLOCK TXS -------------------- #


def get_pk_and_pkh_from_script(script: bytearray, pks, pkhs):
    i = 0
    pd_hashes = []
    len_script = len(script)
    try:
        while i < len_script:
            try:
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
                    i += 1
                    length = script[i]
                    i += 1 + length
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
            except (IndexError, struct.error) as e:
                # This can legitimately happen (bad output scripts...) e.g. see:
                # ebc9fa1196a59e192352d76c0f6e73167046b9d37b8302b6bb6968dfd279b767
                # especially on testnet - lots of bad output scripts...
                logger.error(f"script={script}, len(script)={len(script)}, i={i}")
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


def parse_txs(
    buffer: bytes, tx_offsets: List[int], height_or_timestamp: Union[int, datetime], confirmed: bool
):
    """
    This function is dual-purpose - it can:
    1) ingest raw_blocks (buffer=raw_block) and the height_or_timestamp=height
    2) ingest mempool rawtxs (buffer=rawtx) and the height_or_timestamp=datetime.now()

    This obviates the need for duplicated code and makes it possible to do batch processing of
    mempool transactions at a later date (where buffer=contiguous array of rawtxs)

    returns
        tx_rows =       [(tx_shash, tx_hash, height, position, offset_start, offset_end, tx_has_collided)...]
        in_rows =       [(prevout_shash, out_idx, tx_shash, in_idx, out_has_collided)...)...]
        out_rows =      [(out_tx_shash, idx, value, in_has_collided)...)]
        pd_rows =       [(pushdata_shash, pushdata_hash, tx_shash, idx, ref_type=0 or 1,
            pd_tx_has_collided)...]

        NOTE: full pushdata_hash is not committed to the database but is temporarily included in
        these rows in case of a collision -> committed to collision table.
    """
    tx_rows = []
    tx_shashes = []

    # sets rule out possibility of duplicate pushdata in same input / output
    in_rows = set()
    out_rows = set()
    set_pd_rows = set()
    count_txs = len(tx_offsets)

    tx_has_collided = False
    out_has_collided = False
    in_has_collided = False
    pd_tx_has_collided = False
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
                next_tx_offset = len(buffer)

            rawtx = buffer[tx_offset_start:next_tx_offset]
            tx_hash = double_sha256(rawtx)
            tx_shash = struct_le_q.unpack(tx_hash[0:8])[0]

            # version
            offset += 4

            # inputs
            count_tx_in, offset = unpack_varint(buffer, offset)
            ref_type = 1
            for in_idx in range(count_tx_in):
                in_prevout_hash = buffer[offset : offset + 32]
                in_prevout_shash = struct_le_q.unpack(in_prevout_hash[0:8])[0]
                offset += 32
                in_prevout_idx = struct_le_I.unpack_from(buffer[offset : offset + 4])[0]
                offset += 4
                script_sig_len, offset = unpack_varint(buffer, offset)
                script_sig = buffer[offset : offset + script_sig_len]
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
                                    pd_tx_has_collided,
                                )
                            )
                offset += script_sig_len
                offset += 4  # skip sequence

            # outputs
            count_tx_out, offset = unpack_varint(buffer, offset)
            ref_type = 0
            for out_idx in range(count_tx_out):
                out_value = struct_le_Q.unpack_from(buffer[offset : offset + 8])[0]
                offset += 8  # skip value
                scriptpubkey_len, offset = unpack_varint(buffer, offset)
                scriptpubkey = buffer[offset : offset + scriptpubkey_len]

                out_rows.add((tx_shash, out_idx, out_value, out_has_collided, None, None, None,))

                pushdata_hashes = get_pk_and_pkh_from_script(scriptpubkey, pks, pkhs)
                if len(pushdata_hashes):
                    for out_pushdata_hash in pushdata_hashes:
                        # Todo - out_pushdata_hash needs to be kept in memory for collisions...
                        out_pushdata_shash = struct_le_q.unpack(out_pushdata_hash[0:8])[0]
                        set_pd_rows.add(
                            (
                                out_pushdata_shash,
                                out_pushdata_hash,
                                tx_shash,
                                out_idx,
                                ref_type,
                                pd_tx_has_collided,
                            )
                        )
                offset += scriptpubkey_len

            # nlocktime
            offset += 4

            # NOTE: when partitioning blocks ensure position is correct!
            if confirmed:
                tx_rows.append(
                    (
                        tx_shash,
                        tx_hash,
                        height_or_timestamp,
                        position,
                        tx_offset_start,
                        next_tx_offset,
                        tx_has_collided,
                    )
                )
                tx_shashes.append(tx_shash)
            else:
                tx_rows.append((tx_shash, tx_hash, height_or_timestamp, tx_has_collided, rawtx))
        assert len(tx_rows) == count_txs
        return tx_rows, in_rows, out_rows, set_pd_rows, tx_shashes
    except Exception as e:
        logger.debug(
            f"count_txs={count_txs}, position={position}, in_idx={in_idx}, out_idx={out_idx}, "
            f"txid={bitcoinx.hash_to_hex_str(tx_hash)}"
        )
        logger.exception(e)
        raise


# -------------------- MERKLE TREE -------------------- #


def calc_depth(leaves_count):
    result = leaves_count
    mtree_depth = 0
    while result >= 1:  # 1 represents merkle root
        result = result / 2
        mtree_depth += 1
    return mtree_depth


def calc_mtree_base_level(base_level, leaves_count, mtree, raw_block, tx_offsets):
    mtree[base_level] = []
    for i in range(leaves_count):
        if i < (leaves_count - 1):
            rawtx = raw_block[tx_offsets[i] : tx_offsets[i + 1]]
        else:
            rawtx = raw_block[tx_offsets[i] :]
        tx_hash = double_sha256(rawtx)
        mtree[base_level].append(tx_hash)
    return mtree


def build_mtree_from_base(base_level, mtree):
    """if there is an odd number of hashes at a given level -> raise IndexError
    then duplicate the last hash, concatenate and double_sha256 to continue."""

    for current_level in reversed(range(1, base_level + 1)):
        next_level_up = []
        hashes = mtree[current_level]
        for i in range(0, len(hashes), 2):
            try:
                _hash = double_sha256(hashes[i] + hashes[i + 1])
            except IndexError:
                _hash = double_sha256(hashes[i] + hashes[i])
            finally:
                next_level_up.append(_hash)
        hashes = next_level_up
        mtree[current_level - 1] = hashes


def calc_mtree(raw_block, tx_offsets) -> Dict:
    """base_level refers to the bottom/widest part of the mtree (merkle root is level=0)"""
    # Todo - optimizations:
    #  1) operate on raw binary instead of python lists which would remove the need to then
    #  convert all of the python lists to binary in LMDB
    #  2) only store to mid-level (would save around 40% time on insertion) and around 12% on total
    #  disc space of storing full blockchain.
    #  3) parallelise the calculation across more than 1 process

    mtree = {}
    leaves_count = len(tx_offsets)
    base_level = calc_depth(leaves_count)
    mtree = calc_mtree_base_level(base_level, leaves_count, mtree, raw_block, tx_offsets)
    build_mtree_from_base(base_level, mtree)
    # logger.debug(f"merkle_root={hash_to_hex_str(mtree[0][0])}")
    return mtree
