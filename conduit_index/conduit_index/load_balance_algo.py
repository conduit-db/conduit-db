import array
import math
from typing import NamedTuple

from conduit_lib.constants import SMALL_BLOCK_SIZE, LOAD_BALANCE_RAW_BLOCK_CHUNK_SIZE


class WorkPart(NamedTuple):
    size_of_part: int
    blk_hash: bytes
    blk_height: int
    first_tx_pos: int
    end_byte_offset: int
    tx_offsets: "array.ArrayType[int]"


def find_largest_tx_size(sorted_offsets: "array.ArrayType[int]") -> int:
    max_gap = 0
    for i in range(1, len(sorted_offsets)):
        gap = sorted_offsets[i] - sorted_offsets[i - 1]
        max_gap = max(gap, max_gap)
    return max_gap


def distribute_load(
        blk_hash: bytes,
        blk_height: int,
        count_added: int,
        block_size: int,
        tx_offsets_array: "array.ArrayType[int]"
) -> list[WorkPart]:
    """
    This function distributes the load of processing transactions across worker processes.

    There is a trade-off to be made. Dividing a block into chunks that are too small creates stutter on the
    magnetic drive of ConduitIndex from high frequency random read access. Too large and there can be "hotspots"
    in a raw block slice with very "dense" areas of transactions packed with 1000s of txos per transaction. These
    are much more resource intensive to process than data carrier transactions on a per MB basis. This can lead
    to all the other workers idling while one worker is choking on the hotspot.

    So a reasonable trade-off chunk size seems to be 32MB chunks. Therefore, a 4GB block, tightly packed with
    transactions with thousands of txos each would only allocate around 1 million txos or input rows for parsing
    per worker (at one time). This limits the peak memory consumption of each worker process to within acceptable
    limits too (because the whole byte array and associated caches need to hold all data for the chunk in memory
    at one time).
    """
    # Constants and environment variables
    largest_tx_size_in_block = find_largest_tx_size(tx_offsets_array)

    # Calculate the work item size and batch count
    # If the largest tx size is bigger than 32MB then the smallest chunk size needs to be large
    # enough to accommodate at least 1 full raw transaction.
    work_item_size = max(LOAD_BALANCE_RAW_BLOCK_CHUNK_SIZE, largest_tx_size_in_block)
    # First go with a simple approach. Divide the block evenly based on its size
    batch_count = math.ceil(block_size / work_item_size)
    first_tx_pos_batch = 0

    # Handle small blocks or blocks with fewer transactions than workers
    if count_added < SMALL_BLOCK_SIZE or batch_count == 1:
        size_of_part = block_size
        return [WorkPart(size_of_part, blk_hash, blk_height, first_tx_pos_batch, block_size, tx_offsets_array)]

    # Distribute larger blocks across workers
    divided_tx_positions = []
    last_batch_id = batch_count - 1
    first_tx_pos = 0
    for batch_id in range(batch_count):
        start_idx_batch = min(batch_id * (count_added // batch_count), count_added - 1)
        end_idx_batch = min((batch_id + 1) * (count_added // batch_count), count_added)
        if batch_id == last_batch_id:
            tx_offsets = tx_offsets_array[start_idx_batch:]
            end_byte_offset = block_size  # include remainder all the way to the end of the raw block
            size_of_part = end_byte_offset - tx_offsets[0]
        else:
            tx_offsets = tx_offsets_array[start_idx_batch:end_idx_batch]
            end_byte_offset = tx_offsets_array[end_idx_batch]
            size_of_part = end_byte_offset - tx_offsets[0]

        divided_tx_positions.append(
            WorkPart(
                    size_of_part,
                    blk_hash,
                    blk_height,
                    first_tx_pos,
                    end_byte_offset,
                    tx_offsets,
            )
        )
        first_tx_pos += len(tx_offsets)
    return divided_tx_positions
