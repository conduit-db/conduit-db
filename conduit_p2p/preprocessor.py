# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.
import array
import logging
import struct
from struct import Struct


HEADER_OFFSET = 80
struct_le_H = Struct("<H")
struct_le_I = Struct("<I")
struct_le_Q = Struct("<Q")

logger = logging.getLogger("conduit.p2p.algorithms")


def unpack_varint(buf: bytes | memoryview, offset: int) -> tuple[int, int]:
    n = buf[offset]
    if n < 253:
        return n, offset + 1
    if n == 253:
        return struct_le_H.unpack_from(buf, offset + 1)[0], offset + 3
    if n == 254:
        return struct_le_I.unpack_from(buf, offset + 1)[0], offset + 5
    return struct_le_Q.unpack_from(buf, offset + 1)[0], offset + 9


def skip_one_tx(buffer: bytes, offset: int) -> int:
    # version
    offset += 4

    # tx_in block
    count_tx_in, offset = unpack_varint(buffer, offset)
    for i in range(count_tx_in):
        offset += 36  # prev_hash + prev_idx
        script_sig_len, offset = unpack_varint(buffer, offset)
        offset += script_sig_len
        offset += 4  # sequence

    # tx_out block
    count_tx_out, offset = unpack_varint(buffer, offset)
    for i in range(count_tx_out):
        offset += 8  # value
        script_pubkey_len, offset = unpack_varint(buffer, offset)  # script_pubkey
        offset += script_pubkey_len  # script_sig

    # lock_time
    offset += 4
    return offset


def tx_preprocessor(buffer: bytes, offset: int, adjustment: int) -> tuple["array.ArrayType[int]", int]:
    """
    Call this function iteratively as more slices of the raw block become available from the
    p2p socket.

    Goes until either:
        a) it hits a struct.error or IndexError
        b) the offset exceeds the buffer length
    Either of these conditions indicate we need more data (in the next chunk) to complete the rest of
    the raw transaction.

    - `offset` is only relevant for the first chunk where it is equal to the 80 byte header and
    the number of bytes consumed by the varint for hte tx_count of the block. This places us a the beginning
    of the first transaction for the block. For all other chunks, offset will be zero.
    - `adjustment` shifts the final set of tx_offsets up by this amount. For the first chunk, it will be zero,
    for all subsequent chunks, it should be set to `last_tx_offset_in_chunk`. This is so that when all the
    tx_offsets from all the chunks are concatenated, it will give the correct tx_offset for each rawtx in
    the full block. To know the relative offsets for parallel processing of a chunk, you just subtract the
    first offset from all the others.
    """
    tx_offsets: "array.ArrayType[int]" = array.array("Q")
    tx_offsets.append(offset + adjustment)
    last_tx_offset_in_chunk = offset
    try:
        buffer_len = len(buffer)
        while True:
            offset = skip_one_tx(buffer, offset)
            if offset > buffer_len:
                break
            last_tx_offset_in_chunk = offset + adjustment
            tx_offsets.append(offset + adjustment)

        tx_offsets.pop(-1)  # the last offset represents the end of the last tx for the chunk
        return tx_offsets, last_tx_offset_in_chunk
    except (struct.error, IndexError):
        tx_offsets.pop(-1)  # the last offset represents the end of the last tx for the chunk
        return tx_offsets, last_tx_offset_in_chunk
