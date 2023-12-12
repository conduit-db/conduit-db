import array
import os
from pathlib import Path

from conduit_index.conduit_index.load_balance_algo import distribute_load
from conduit_lib.constants import SMALL_BLOCK_SIZE


MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))


def test_distribute_load_very_few_txs():
    blk_hash = b"aa"*32
    blk_height = 1
    count_added = 1
    average_size_tx = 250 // count_added
    tx_offsets_array = array.array('Q', [i*average_size_tx + 83 for i in range(count_added)])
    block_size = 1_000_000

    work_items = distribute_load(blk_hash, blk_height, count_added, block_size, tx_offsets_array)
    print()
    print(len(work_items))
    assert len(work_items) == 1
    assert work_items[0].blk_hash == blk_hash
    assert work_items[0].blk_height == 1
    assert work_items[0].tx_offsets == tx_offsets_array
    assert len(work_items[0].tx_offsets) == count_added
    assert work_items[0].end_byte_offset == block_size
    assert work_items[0].first_tx_pos == 0


def test_distribute_load_few_txs():
    blk_hash = b"aa"*32
    blk_height = 1
    count_added = SMALL_BLOCK_SIZE - 1
    average_size_tx = 1_000_000 // count_added
    tx_offsets_array = array.array('Q', [i*average_size_tx + 83 for i in range(count_added)])
    block_size = 1_000_000

    work_items = distribute_load(blk_hash, blk_height, count_added, block_size, tx_offsets_array)
    print()
    print(len(work_items))
    assert len(work_items) == 1
    assert work_items[0].blk_hash == blk_hash
    assert work_items[0].blk_height == 1
    assert work_items[0].tx_offsets == tx_offsets_array
    assert len(work_items[0].tx_offsets) == count_added
    assert work_items[0].end_byte_offset == block_size
    assert work_items[0].first_tx_pos == 0


def test_distribute_load_many_txs():
    """Large blocks whether they are dense with many small txs or 'fluffy' with few large data carrier txs,
    should be divided evenly amongst all workers."""
    blk_hash = b"aa"*32
    blk_height = 1
    count_added = SMALL_BLOCK_SIZE + 1
    average_size_tx = 1_000_000 // count_added
    header_and_tx_count_bytes = 83
    tx_offsets_array = array.array('Q', [i*average_size_tx + header_and_tx_count_bytes for i in range(count_added)])
    block_size = 1_000_000

    os.environ['WORKER_COUNT_TX_PARSERS'] = '4'
    expected_worker_count = 4

    work_items = distribute_load(blk_hash, blk_height, count_added, block_size, tx_offsets_array)
    assert len(work_items) == expected_worker_count
    for work_item in work_items:
        assert work_item.blk_hash == blk_hash
        assert work_item.blk_height == 1

    assert work_items[0].tx_offsets[0] == 83
    assert work_items[0].tx_offsets[-1] == 247484
    assert work_items[1].tx_offsets[0] == 247583
    assert work_items[1].tx_offsets[-1] == 494984
    assert work_items[2].tx_offsets[0] == 495083
    assert work_items[2].tx_offsets[-1] == 742484
    assert work_items[3].tx_offsets[0] == 742583
    assert work_items[3].tx_offsets[-1] == 990083

    assert work_items[0].first_tx_pos == 0
    assert work_items[1].first_tx_pos == 2500
    assert work_items[2].first_tx_pos == 5000
    assert work_items[3].first_tx_pos == 7500

    assert work_items[0].end_byte_offset == 247583
    assert work_items[1].end_byte_offset == 495083
    assert work_items[2].end_byte_offset == 742583
    assert work_items[3].end_byte_offset == block_size

    assert len(work_items[0].tx_offsets) == 2500
    assert len(work_items[1].tx_offsets) == 2500
    assert len(work_items[2].tx_offsets) == 2500
    assert len(work_items[3].tx_offsets) == 2501
    assert sum([
        len(work_items[0].tx_offsets),
        len(work_items[1].tx_offsets),
        len(work_items[2].tx_offsets),
        len(work_items[3].tx_offsets)
    ]) == len(tx_offsets_array)

    assert work_items[0].size_of_part + \
           work_items[1].size_of_part + \
           work_items[2].size_of_part + \
           work_items[3].size_of_part == block_size - header_and_tx_count_bytes

