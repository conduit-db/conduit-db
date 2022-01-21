"""slower pure python alternative"""
import array
import logging
import struct
from hashlib import sha256
from math import ceil, log
from typing import Dict, Union, Tuple, List, cast
from bitcoinx import double_sha256, hash_to_hex_str
from struct import Struct

from conduit_lib.constants import HashXLength
from conduit_lib.database.mysql.types import ConfirmedTransactionRow, InputRow, OutputRow, \
    PushdataRow, MempoolTransactionRow, MySQLFlushBatch

MTreeLevel = int
MTreeNodeArray = list[bytes]
MTree = Dict[MTreeLevel, MTreeNodeArray]


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
logger.setLevel(logging.DEBUG)


# typing(AustEcon) - array.ArrayType doesn't let me specify int or bytes
def unpack_varint(buf: Union[memoryview, array.ArrayType], offset: int) -> tuple[int, int]:  # type: ignore
    n = buf[offset]
    if n < 253:
        return n, offset + 1
    if n == 253:
        return struct_le_H.unpack_from(buf, offset + 1)[0], offset + 3
    if n == 254:
        return struct_le_I.unpack_from(buf, offset + 1)[0], offset + 5
    return struct_le_Q.unpack_from(buf[offset+1:offset+3].tobytes(), offset + 1)[0], offset + 9

# -------------------- PREPROCESSOR -------------------- #


# typing(AustEcon) - array.ArrayType doesn't let me specify int or bytes
def preprocessor(block_view: memoryview,
        tx_offsets_array: array.ArrayType, block_offset: int=0) -> Tuple[int, array.ArrayType]:  # type: ignore
    block_offset += HEADER_OFFSET
    count, block_offset = unpack_varint(block_view, block_offset)
    cur_idx = 0

    try:
        tx_offsets_array[cur_idx], cur_shm_idx = block_offset, cur_idx + 1
        for i in range(count - 1):
            # version
            block_offset += 4

            # tx_in block
            count_tx_in, block_offset = unpack_varint(block_view, block_offset)
            for i in range(count_tx_in):
                block_offset += 36  # prev_hash + prev_idx
                script_sig_len, block_offset = unpack_varint(block_view, block_offset)
                block_offset += script_sig_len
                block_offset += 4  # sequence

            # tx_out block
            count_tx_out, block_offset = unpack_varint(block_view, block_offset)
            for i in range(count_tx_out):
                block_offset += 8  # value
                script_pubkey_len, block_offset = unpack_varint(block_view, block_offset)  # script_pubkey
                block_offset += script_pubkey_len  # script_sig

            # lock_time
            block_offset += 4
            tx_offsets_array[cur_shm_idx], cur_shm_idx = block_offset, cur_shm_idx + 1
        return count, tx_offsets_array
    except IndexError:
        logger.error(f"likely overflowed size of tx_offsets_array; size={len(tx_offsets_array)}; "
                     f"count of txs in block={count}")
        logger.exception(f"cur_idx={cur_idx}; block_offset={block_offset}")
        raise


# -------------------- PARSE BLOCK TXS -------------------- #


# typing(AustEcon) - array.ArrayType doesn't let me specify int or bytes
def get_pk_and_pkh_from_script(script: 'array.ArrayType', tx_hash: bytes, idx: int,  # type: ignore
        ref_type: int) -> List[bytes]:
    i = 0
    pks, pkhs = set(), set()
    pd_hashXes: List[bytes] = []
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
                # This can legitimately happen (bad output scripts that have not packed the
                # pushdatas correctly) e.g. see:
                # ebc9fa1196a59e192352d76c0f6e73167046b9d37b8302b6bb6968dfd279b767
                # especially on testnet - lots of bad output scripts...
                logger.error(f"Ignored a bad script for tx_hash: %s, idx: %s, ref_type: %s",
                    hash_to_hex_str(tx_hash), idx, ref_type)
        # hash pushdata
        for pk in pks:
            pd_hashXes.append(sha256(pk).digest()[0:HashXLength])

        for pkh in pkhs:
            pd_hashXes.append(sha256(pkh).digest()[0:HashXLength])
        return pd_hashXes
    except Exception as e:
        logger.exception(f"Bad script for tx_hash: %s, idx: %s, ref_type: %s",
            hash_to_hex_str(tx_hash), idx, ref_type)
        raise


# typing(AustEcon) - array.ArrayType doesn't let me specify int or bytes
def parse_txs(buffer: array.ArrayType, tx_offsets: Union[List[int], array.ArrayType],  # type: ignore
        height_or_timestamp: Union[int, str], confirmed: bool, first_tx_pos_batch=0) \
            -> MySQLFlushBatch:
    """
    This function is dual-purpose - it can:
    1) ingest raw_blocks (buffer=raw_block) and the blk_num_or_timestamp=height
    2) ingest mempool rawtxs (buffer=rawtx) and the blk_num_or_timestamp=datetime.now()

    This obviates the need for duplicated code and makes it possible to do batch processing of
    mempool transactions at a later date (where buffer=contiguous array of rawtxs)

    returns
        tx_rows =       [(tx_hash, height, position, offset_start, offset_end)...]
        in_rows =       [(prevout_hash, out_idx, tx_hash, in_idx, in_offset_start, in_offset_end)...)...]
        out_rows =      [(out_tx_hash, idx, value, out_offset_start, out_offset_end)...)]
        pd_rows =       [(pushdata_hash, tx_hash, idx, ref_type=0 or 1)...]
    """
    tx_rows = []
    in_rows = set()
    out_rows = set()
    set_pd_rows = set()
    count_txs = len(tx_offsets)

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

            rawtx = buffer[tx_offset_start:next_tx_offset].tobytes()
            tx_hash = double_sha256(rawtx)
            tx_hashX = tx_hash[0:HashXLength]

            # version
            offset += 4

            # inputs
            count_tx_in, offset = unpack_varint(buffer, offset)
            ref_type = 1
            # in_offset_start = offset + adjustment
            for in_idx in range(count_tx_in):
                in_prevout_hashX = buffer[offset : offset + 32].tobytes()[0:HashXLength]
                offset += 32
                in_prevout_idx = struct_le_I.unpack_from(buffer[offset : offset + 4])[0]
                offset += 4
                script_sig_len, offset = unpack_varint(buffer, offset)
                script_sig = buffer[offset : offset + script_sig_len]  # keep as array.array
                offset += script_sig_len
                offset += 4  # skip sequence
                # in_offset_end = offset + adjustment

                in_rows.add(
                    InputRow(in_prevout_hashX.hex(), in_prevout_idx, tx_hashX.hex(), in_idx))

                # some coinbase tx scriptsigs don't obey any rules so for now they are not
                # included in the pushdata table at all
                # mempool txs will appear to have a tx_pos=0
                if (not tx_pos == 0 and confirmed) or not confirmed:

                    pushdata_hashXes = get_pk_and_pkh_from_script(script_sig, tx_hash=tx_hash,
                        idx=in_idx, ref_type=ref_type)
                    if len(pushdata_hashXes):
                        for in_pushdata_hashX in pushdata_hashXes:
                            set_pd_rows.add(
                                (
                                    in_pushdata_hashX.hex(),
                                    tx_hashX.hex(),
                                    in_idx,
                                    ref_type,
                                )
                            )

            # outputs
            count_tx_out, offset = unpack_varint(buffer, offset)
            ref_type = 0
            # out_offset_start = offset + adjustment
            for out_idx in range(count_tx_out):
                out_value = struct_le_Q.unpack_from(buffer[offset : offset + 8])[0]
                offset += 8  # skip value
                scriptpubkey_len, offset = unpack_varint(buffer, offset)
                scriptpubkey = buffer[offset : offset + scriptpubkey_len]  # keep as array.array

                pushdata_hashXes = get_pk_and_pkh_from_script(scriptpubkey, tx_hash=tx_hash,
                    idx=out_idx, ref_type=ref_type)
                if len(pushdata_hashXes):
                    for out_pushdata_hashX in pushdata_hashXes:
                        set_pd_rows.add(
                            PushdataRow(
                                out_pushdata_hashX.hex(),
                                tx_hashX.hex(),
                                out_idx,
                                ref_type,
                            )
                        )
                offset += scriptpubkey_len
                # out_offset_end = offset + adjustment
                out_rows.add(OutputRow(tx_hashX.hex(), out_idx, out_value))

            # nlocktime
            offset += 4

            # NOTE: when partitioning blocks ensure position is correct!
            if confirmed:
                height_or_timestamp = cast(int, height_or_timestamp)
                tx_rows.append(ConfirmedTransactionRow(tx_hashX.hex(), height_or_timestamp, tx_pos))
            else:
                # Note mempool uses full length tx_hash
                height_or_timestamp = cast(str, height_or_timestamp)
                tx_rows.append(MempoolTransactionRow(tx_hash.hex(), height_or_timestamp))  # type: ignore
        assert len(tx_rows) == count_txs
        return MySQLFlushBatch(
            cast(List[Union[ConfirmedTransactionRow, MempoolTransactionRow]], tx_rows),
            cast(List[InputRow], list(in_rows)),
            cast(List[OutputRow], list(out_rows)),
            cast(List[PushdataRow], list(set_pd_rows)))
    except Exception as e:
        logger.debug(
            f"count_txs={count_txs}, tx_pos={tx_pos}, in_idx={in_idx}, out_idx={out_idx}, "
            f"txid={hash_to_hex_str(tx_hashX)}"
        )
        logger.exception(e)
        raise


# -------------------- MERKLE TREE -------------------- #


def calc_depth(leaves_count: int) -> int:
    return ceil(log(leaves_count, 2)) + 1


def calc_mtree_base_level(base_level: int, leaves_count: int, mtree: MTree, raw_block: bytes,
        tx_offsets: 'array.ArrayType[int]') -> MTree:
    mtree[base_level] = []
    for i in range(leaves_count):
        if i < (leaves_count - 1):
            rawtx = raw_block[tx_offsets[i] : tx_offsets[i + 1]]
        else:
            rawtx = raw_block[tx_offsets[i] :]
        tx_hash = double_sha256(rawtx)
        mtree[base_level].append(tx_hash)
    return mtree


def build_mtree_from_base(base_level: MTreeLevel, mtree: MTree) -> None:
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


def calc_mtree(raw_block: Union[memoryview, bytes], tx_offsets: 'array.ArrayType[int]') -> MTree:
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
