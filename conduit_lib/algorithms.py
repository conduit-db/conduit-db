"""slower pure python alternative"""
from __future__ import annotations

import array
import hashlib
import logging
import os
import struct
from math import ceil, log
from typing import cast, NamedTuple

from bitcoinx import hash_to_hex_str
from struct import Struct

from conduit_lib.constants import HashXLength
from conduit_lib.database.mysql.types import ConfirmedTransactionRow, InputRow, OutputRow, \
    PushdataRow, MempoolTransactionRow
from conduit_lib.types import PushdataMatchFlags

_sha256 = hashlib.sha256

MTreeLevel = int
MTreeNodeArray = list[bytes]
MTree = dict[MTreeLevel, MTreeNodeArray]


HEADER_OFFSET = 80
OP_PUSH_20 = 20
OP_PUSH_32 = 32
OP_PUSH_33 = 33
OP_PUSH_65 = 65
OP_RETURN = 0x6a
OP_DROP = 0x75
OP_ELSE = 0x67
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
logger.setLevel(logging.DEBUG)


def sha256(x: bytes) -> bytes:
    return _sha256(x).digest()


def double_sha256(x: bytes) -> bytes:
    return sha256(sha256(x))


def unpack_varint(buf: bytes | memoryview | array.ArrayType[int], offset: int) -> tuple[int, int]:
    n = buf[offset]
    if n < 253:
        return n, offset + 1
    if n == 253:
        return struct_le_H.unpack_from(buf, offset + 1)[0], offset + 3
    if n == 254:
        return struct_le_I.unpack_from(buf, offset + 1)[0], offset + 5
    return struct_le_Q.unpack_from(buf, offset + 1)[0], offset + 9

# -------------------- PREPROCESSOR -------------------- #


def preprocessor(next_chunk: bytes, adjustment: int, first_chunk: bool,
        last_chunk: bool) -> tuple[array.ArrayType[int], int]:
    """
    Call this function iteratively as more slices of the raw block become available from the
    p2p socket.

    Goes until it hits a struct.error or IndexError to indicate it needs the next chunk.
    """
    # skip header bytes + tx_count and set first tx offset
    tx_offsets: array.ArrayType[int] = array.array('Q')
    chunk_offset = 0
    if first_chunk:
        tx_count, chunk_offset = unpack_varint(next_chunk[80:89], chunk_offset)
        chunk_offset = 80 + chunk_offset
        tx_offsets.append(chunk_offset)

    last_tx_offset_in_chunk = chunk_offset
    try:
        while chunk_offset < len(next_chunk):
            last_tx_offset_in_chunk = chunk_offset

            # version
            chunk_offset += 4

            # tx_in block
            count_tx_in, chunk_offset = unpack_varint(next_chunk, chunk_offset)
            for i in range(count_tx_in):
                chunk_offset += 36  # prev_hash + prev_idx
                script_sig_len, chunk_offset = unpack_varint(next_chunk, chunk_offset)
                chunk_offset += script_sig_len
                chunk_offset += 4  # sequence

            # tx_out block
            count_tx_out, chunk_offset = unpack_varint(next_chunk, chunk_offset)
            for i in range(count_tx_out):
                chunk_offset += 8  # value
                script_pubkey_len, chunk_offset = unpack_varint(next_chunk, chunk_offset)  # script_pubkey
                chunk_offset += script_pubkey_len  # script_sig

            # lock_time
            chunk_offset += 4
            end_of_tx_offset = chunk_offset + adjustment
            last_tx_offset_in_chunk = end_of_tx_offset
            tx_offsets.append(end_of_tx_offset)

        if last_chunk:
            tx_offsets.pop(-1)  # the last offset represents the very end of the raw block
        return tx_offsets, last_tx_offset_in_chunk
    except (struct.error, IndexError):
        return tx_offsets, last_tx_offset_in_chunk


# -------------------- PARSE BLOCK TXS -------------------- #
class PushdataMatch(NamedTuple):
    pushdata_hash: bytes
    flags: PushdataMatchFlags


# Todo - unittest coverage
def get_pk_and_pkh_from_script(script: bytes, tx_hash: bytes, idx: int,
        flags: PushdataMatchFlags, tx_pos: int, genesis_height: int) -> list[PushdataMatch]:
    i = 0
    all_pushdata: set[tuple[bytes, PushdataMatchFlags]] = set()
    pd_matches: list[PushdataMatch] = []
    len_script = len(script)
    unreachable_code = False
    try:
        while i < len_script:
            try:
                # Todo - this should ideally check for OP_FALSE OP_RETURN post genesis activation
                if script[i] == OP_RETURN:
                    unreachable_code = True
                    i += 1
                # Todo - unittest; I have not given this enough scrutiny to have confidence in it
                elif script[i] == OP_ELSE:
                    unreachable_code = False
                    i += 1
                elif script[i] in {OP_PUSH_20, OP_PUSH_32, OP_PUSH_33, OP_PUSH_65}:
                    length = script[i]
                    i += 1
                    if unreachable_code:
                        flags |= PushdataMatchFlags.DATA
                    all_pushdata.add((bytes(script[i: i + length]), flags))
                    i += length
                elif script[i] in SET_OTHER_PUSH_OPS:
                    length = script[i]
                    i += 1
                    # ----- Uncomment to capture arbitrary length pushdata from outputs only --- #
                    # if length >= 20 and unreachable_code:
                    #     pd = script[i: i + length].tobytes()
                    #     flags |= PushdataMatchFlags.DATA
                    #     all_pushdata.add((pd, flags))
                    i += length
                elif script[i] == OP_PUSHDATA1:
                    i += 1
                    length = script[i]
                    i += 1
                    # # ----- Uncomment to capture arbitrary length pushdata from outputs only --- #
                    # if length > 20 and unreachable_code:
                    #     pd = script[i: i + length].tobytes()
                    #     all_pushdata.add((pd, flags | PushdataMatchFlags.DATA))
                    i += length
                elif script[i] == OP_PUSHDATA2:
                    i += 1
                    length = int.from_bytes(script[i : i + 2], byteorder="little", signed=False)
                    # # ----- Uncomment to capture arbitrary length pushdata from outputs only --- #
                    # if length > 20 and unreachable_code:
                    #     pd = script[2 + i: 2 + i + length].tobytes()
                    #     all_pushdata.add((pd, flags | PushdataMatchFlags.DATA))
                    i += 2 + length
                elif script[i] == OP_PUSHDATA4:
                    i += 1
                    length = int.from_bytes(script[i : i + 4], byteorder="little", signed=False)
                    # # ----- Uncomment to capture arbitrary length pushdata from outputs only --- #
                    # if length > 20 and unreachable_code:
                    #     pd = script[4 + i: 4 + i + length].tobytes()
                    #     all_pushdata.add((pd, flags | PushdataMatchFlags.DATA))
                    i += 4 + length
                else:  # slow search byte by byte...
                    i += 1
            except (IndexError, struct.error) as e:
                # This can legitimately happen (bad output scripts that have not packed the
                # pushdatas correctly) e.g. see:
                # ebc9fa1196a59e192352d76c0f6e73167046b9d37b8302b6bb6968dfd279b767
                # especially on testnet - lots of bad output scripts...
                logger.error(f"Ignored a bad script for tx_hash: %s, idx: %s, ref_type: %s, "
                    f"tx_pos: %s", hash_to_hex_str(tx_hash), idx, flags, tx_pos)

        for pushdata, flags in all_pushdata:
            pd_matches.append(PushdataMatch(sha256(pushdata)[0:HashXLength], flags))

        return pd_matches
    except Exception as e:
        logger.exception(f"Bad script for tx_hash: %s, idx: %s, ref_type: %s, tx_pos: %s",
            hash_to_hex_str(tx_hash), idx, flags, tx_pos)
        raise


def parse_txs(buffer: bytes, tx_offsets: list[int] | array.ArrayType[int],
        block_num_or_timestamp: int, confirmed: bool, first_tx_pos_batch: int=0,
        already_seen_offsets: set[int] | None=None) -> tuple[list[ConfirmedTransactionRow],
            list[MempoolTransactionRow], list[InputRow], list[OutputRow], list[PushdataRow]]:
    """
    This function is dual-purpose - it can:
    1) ingest raw_blocks (buffer=raw_block) and the blk_num_or_timestamp=height
    2) ingest mempool rawtxs (buffer=rawtx) and the blk_num_or_timestamp=datetime.now()

    This obviates the need for duplicated code and makes it possible to do batch processing of
    mempool transactions at a later date (where buffer=contiguous array of rawtxs)

    `already_seen_offsets` is to exclude inputs, outputs and pushdata rows from the end
    result because these have already been flushed to disc earlier - this could be from either
    a reorg or from mempool processing
    """
    genesis_height = int(os.environ['GENESIS_ACTIVATION_HEIGHT'])
    tx_rows_confirmed = []
    tx_rows_mempool = []
    in_rows = []
    out_rows = []
    set_pd_rows = []
    count_txs = len(tx_offsets)
    previously_processed = False
    if already_seen_offsets is None:
        already_seen_offsets = set()

    # Partitions other than the first, the tx offsets need to be adjusted (to start at zero)
    # to account for the preceding partitions that are excluded in this buffer.
    adjustment = 0
    if first_tx_pos_batch != 0:
        adjustment = tx_offsets[0]

    try:
        for i in range(count_txs):
            tx_pos = i + first_tx_pos_batch  # for multiprocessing need to track position in block

            # tx_hash
            offset = tx_offsets[i] - adjustment
            tx_offset_start = offset
            if i < count_txs - 1:
                next_tx_offset = tx_offsets[i + 1] - adjustment
            else:
                next_tx_offset = len(buffer)

            rawtx = buffer[tx_offset_start:next_tx_offset]
            tx_hash = double_sha256(rawtx)
            tx_hashX = tx_hash[0:HashXLength]

            if offset in already_seen_offsets:
                previously_processed = True

            # version
            offset += 4

            # inputs
            count_tx_in, offset = unpack_varint(buffer, offset)
            # in_offset_start = offset + adjustment
            for in_idx in range(count_tx_in):
                in_prevout_hashX = buffer[offset: offset + 32][0:HashXLength]
                offset += 32
                in_prevout_idx = struct_le_I.unpack_from(buffer[offset : offset + 4])[0]
                offset += 4
                script_sig_len, offset = unpack_varint(buffer, offset)
                # script_sig = buffer[offset : offset + script_sig_len]
                offset += script_sig_len
                offset += 4  # skip sequence

                if not previously_processed:
                    in_rows.append(
                        (in_prevout_hashX.hex(), in_prevout_idx, tx_hashX.hex(), in_idx))

                # some coinbase tx scriptsigs don't obey any rules so for now they are not
                # included in the pushdata table at all
                # if ((not tx_pos == 0 and confirmed) or not confirmed) \
                #         and not previously_processed:
                #
                #     pushdata_matches = get_pk_and_pkh_from_script(script_sig, tx_hash=tx_hash,
                #         idx=in_idx, flags=PushdataMatchFlags.INPUT, tx_pos=tx_pos,
                #         genesis_height=genesis_height)
                #     if len(pushdata_matches):
                #         for in_pushdata_hashX, flags in set(pushdata_matches):
                #             set_pd_rows.append((
                #                     in_pushdata_hashX.hex(),
                #                     tx_hashX.hex(),
                #                     in_idx,
                #                     int(flags),
                #                 )
                #             )

            # outputs
            count_tx_out, offset = unpack_varint(buffer, offset)
            for out_idx in range(count_tx_out):
                out_value = struct_le_Q.unpack_from(buffer[offset : offset + 8])[0]
                offset += 8  # skip value
                scriptpubkey_len, offset = unpack_varint(buffer, offset)
                scriptpubkey = buffer[offset : offset + scriptpubkey_len]  # keep as array.array

                if not previously_processed:
                    pushdata_matches = get_pk_and_pkh_from_script(scriptpubkey, tx_hash=tx_hash,
                        idx=out_idx, flags=PushdataMatchFlags.OUTPUT, tx_pos=tx_pos,
                        genesis_height=genesis_height)
                    if len(pushdata_matches):
                        for out_pushdata_hashX, flags in set(pushdata_matches):
                            set_pd_rows.append((
                                    out_pushdata_hashX.hex(),
                                    tx_hashX.hex(),
                                    out_idx,
                                    int(flags),
                                )
                            )
                offset += scriptpubkey_len
                # out_offset_end = offset + adjustment
                if not previously_processed:
                    out_rows.append((tx_hashX.hex(), out_idx, out_value))

            # nlocktime
            offset += 4

            # NOTE: when partitioning blocks ensure position is correct!
            if confirmed:
                tx_rows_confirmed.append((tx_hashX.hex(), block_num_or_timestamp, tx_pos))
            else:
                # Note mempool uses full length tx_hash
                tx_rows_mempool.append((tx_hash.hex(), block_num_or_timestamp))
        assert len(tx_rows_confirmed) + len(tx_rows_mempool) == count_txs
        return cast(list[ConfirmedTransactionRow], tx_rows_confirmed), \
            cast(list[MempoolTransactionRow], tx_rows_mempool), \
            cast(list[InputRow], in_rows), \
            cast(list[OutputRow], out_rows), \
            cast(list[PushdataRow], set_pd_rows)
    except Exception as e:
        logger.debug(
            f"count_txs={count_txs}, tx_pos={tx_pos}, in_idx={in_idx}, out_idx={out_idx}, "
            f"txid={hash_to_hex_str(tx_hash)}, height_or_timestamp={block_num_or_timestamp}"
        )
        raise


# -------------------- MERKLE TREE -------------------- #


def calc_depth(leaves_count: int) -> int:
    return ceil(log(leaves_count, 2)) + 1


def calc_mtree_base_level(base_level: int, leaves_count: int, mtree: MTree, raw_block: bytes,
        tx_offsets: array.ArrayType[int]) -> MTree:
    mtree[base_level] = []
    for i in range(leaves_count):
        if i < (leaves_count - 1):
            rawtx = raw_block[tx_offsets[i] : tx_offsets[i + 1]]
        else:
            rawtx = raw_block[tx_offsets[i] :]
        tx_hash = double_sha256(rawtx)
        mtree[base_level].append(tx_hash)
    return mtree


def build_mtree_from_base(base_level: MTreeLevel, mtree: MTree) -> MTree:
    """if there is an odd number of hashes at a given level -> raise IndexError
    then duplicate the last hash, concatenate and double_sha256 to continue."""

    for current_level in reversed(range(1, base_level + 1)):
        next_level_up = []
        hashes = mtree[current_level]
        for i in range(0, len(hashes), 2):
            _hash = bytes()
            try:
                _hash = double_sha256(hashes[i] + hashes[i + 1])
            except IndexError:
                _hash = double_sha256(hashes[i] + hashes[i])
            finally:
                next_level_up.append(_hash)
        hashes = next_level_up
        mtree[current_level - 1] = hashes
    return mtree


def calc_mtree(raw_block: bytes, tx_offsets: array.ArrayType[int]) -> MTree:
    """base_level refers to the bottom/widest part of the mtree (merkle root is level=0)"""
    # This is a naive, brute force implementation
    mtree: MTree = {}
    leaves_count = len(tx_offsets)
    base_level = calc_depth(leaves_count) - 1
    mtree = calc_mtree_base_level(base_level, leaves_count, mtree, raw_block, tx_offsets)
    build_mtree_from_base(base_level, mtree)
    # logger.debug(f"merkle_root={hash_to_hex_str(mtree[0][0])}")
    return mtree


def get_mtree_node_counts_per_level(base_node_count: int) -> list[int]:
    depth = calc_depth(base_node_count)
    counts = []

    node_count = base_node_count
    for level in reversed(range(depth)):
        counts.append(node_count)
        if node_count & 1 and level != 0:  # odd number
            node_count += 1
        node_count = node_count // 2
    return list(reversed(counts))
