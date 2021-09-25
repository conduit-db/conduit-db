import array
import logging
import math
import time
from os import urandom
from typing import List

from conduit_lib.constants import WORKER_COUNT_TX_PARSERS, SMALL_BLOCK_TX_COUNT

logger = logging.getLogger("distribute_load")


def distribute_load(blk_hash, blk_height, count_added, blk_end_pos, tx_offsets_array) -> \
        list[tuple[bytes, int, int, int, List]]:
    """
    returns item with:
        block_header.hash, block_header.height, first_tx_pos_batch, part_end_offset, tx_offsets
    """
    BATCH_COUNT = WORKER_COUNT_TX_PARSERS
    BATCH_SIZE = math.floor(count_added / BATCH_COUNT)

    first_tx_pos_batch = 0
    if BATCH_COUNT == 1 or count_added <= SMALL_BLOCK_TX_COUNT or count_added < BATCH_COUNT:
        start_idx_batch = 0

        # This might look unnecessary (why not just return tx_offsets?)
        # It's because this function is also used by the preprocessor which has a huge pre-allocated
        # array.array that it operates on so the position needs to be explicit
        end_idx_batch = start_idx_batch + count_added
        tx_offsets = tx_offsets_array[start_idx_batch:end_idx_batch]

        part_end_offset = blk_end_pos
        return [(blk_hash, blk_height, first_tx_pos_batch, part_end_offset, tx_offsets)]

    divided_tx_positions = []
    # if there is only 1 tx in the block the other batches are empty
    for i in range(BATCH_COUNT):

        if i == BATCH_COUNT - 1:  # last batch
            start_idx_batch = i * BATCH_SIZE
            end_idx_batch = count_added
            tx_offsets = tx_offsets_array[start_idx_batch:end_idx_batch]
            part_end_offset = blk_end_pos
            divided_tx_positions.extend([(blk_hash, blk_height, first_tx_pos_batch,
                part_end_offset, tx_offsets)])

        else:
            start_idx_batch = i * BATCH_SIZE
            end_idx_batch = (i + 1) * BATCH_SIZE
            num_txs = end_idx_batch - start_idx_batch
            tx_offsets = tx_offsets_array[start_idx_batch:end_idx_batch]
            part_end_offset = tx_offsets_array[end_idx_batch]
            divided_tx_positions.extend([(blk_hash, blk_height, first_tx_pos_batch,
                part_end_offset, tx_offsets)])
            first_tx_pos_batch += num_txs

    # for index, batch in enumerate(divided_tx_positions):
    #     logger.debug(f"worker={index+1} batch={batch}")

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

