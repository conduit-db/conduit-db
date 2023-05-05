import array
import logging
import math
import os
import time
from os import urandom

from conduit_lib.constants import CHIP_AWAY_BYTE_SIZE_LIMIT, SMALL_BLOCK_SIZE

from .types import WorkPart

logger = logging.getLogger("distribute_load")


def distribute_load(
    blk_hash: bytes,
    blk_height: int,
    count_added: int,
    block_size: int,
    tx_offsets_array: "array.ArrayType[int]",
) -> list[WorkPart]:
    """tx_offsets_array must be all the tx_offsets in a full raw block
    Todo - This very badly needs unittest coverage - and TDD for working around the risk of a
        freakishly large transaction in the batch.
    """
    # logger.debug(f"Length of tx_offsets_array={len(tx_offsets_array)}")

    # If MAX_WORK_ITEM_SIZE is very small (smaller than the average tx size then it will lead
    # to most of the txs 'piling up' in the last work item...
    # Also - if MAX_WORK_ITEM_SIZE is smaller than even a single tx then it would error!
    WORKER_COUNT_TX_PARSERS = int(os.getenv("WORKER_COUNT_TX_PARSERS", "4"))
    WORK_ITEM_SIZE = CHIP_AWAY_BYTE_SIZE_LIMIT / WORKER_COUNT_TX_PARSERS
    BATCH_COUNT = math.ceil(block_size / WORK_ITEM_SIZE)
    BATCH_SIZE = math.floor(count_added / BATCH_COUNT)

    first_tx_pos_batch = 0
    if BATCH_COUNT == 1 or count_added < SMALL_BLOCK_SIZE:
        start_idx_batch = 0

        # This might look unnecessary (why not just return tx_offsets?)
        # It's because this function is also used by the preprocessor which has a huge pre-allocated
        # array.array that it operates on so the position needs to be explicit
        end_idx_batch = start_idx_batch + count_added
        tx_offsets = tx_offsets_array[start_idx_batch:end_idx_batch]
        part_end_offset = block_size
        size_of_part = part_end_offset - tx_offsets[first_tx_pos_batch]
        return [
            (
                size_of_part,
                blk_hash,
                blk_height,
                first_tx_pos_batch,
                part_end_offset,
                tx_offsets,
            )
        ]

    divided_tx_positions = []
    for i in range(BATCH_COUNT):
        if i == BATCH_COUNT - 1:  # last batch - can be a bit bigger to include the remainder
            start_idx_batch = i * BATCH_SIZE
            end_idx_batch = count_added
            tx_offsets = tx_offsets_array[start_idx_batch:end_idx_batch]
            part_end_offset = block_size
            size_of_part = tx_offsets_array[end_idx_batch - 1] - tx_offsets_array[start_idx_batch]
            divided_tx_positions.extend(
                [
                    (
                        size_of_part,
                        blk_hash,
                        blk_height,
                        first_tx_pos_batch,
                        part_end_offset,
                        tx_offsets,
                    )
                ]
            )

        else:
            start_idx_batch = i * BATCH_SIZE
            end_idx_batch = (i + 1) * BATCH_SIZE
            num_txs = end_idx_batch - start_idx_batch
            tx_offsets = tx_offsets_array[start_idx_batch:end_idx_batch]
            size_of_part = tx_offsets_array[end_idx_batch] - tx_offsets_array[start_idx_batch]
            part_end_offset = tx_offsets_array[end_idx_batch]
            divided_tx_positions.extend(
                [
                    (
                        size_of_part,
                        blk_hash,
                        blk_height,
                        first_tx_pos_batch,
                        part_end_offset,
                        tx_offsets,
                    )
                ]
            )
            first_tx_pos_batch += num_txs

    # for index, batch in enumerate(divided_tx_positions):
    #     logger.debug(f"work part index={index} batched tx_offsets length={len(batch[5])};
    #     first_tx_pos_batch={batch[3]}; part_end_offset={batch[4]}")

    return divided_tx_positions


if __name__ == "__main__":
    t0 = time.perf_counter()
    blk_hash = urandom(32)
    count_added = 10001
    blk_height = 10
    blk_end_pos = 100000
    tx_offsets = array.array("Q", [i for i in range(20000)])
    result = distribute_load(blk_hash, blk_height, count_added, blk_end_pos, tx_offsets)
    t1 = time.perf_counter() - t0
    # print(len(result[0][4]))
    print(result)
    print(f"completed in {t1} seconds for a simulated block with {count_added} txs")
