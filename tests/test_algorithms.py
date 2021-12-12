from bitcoinx import double_sha256

from conduit_lib.algorithms import calc_depth, get_mtree_node_counts_per_level


def test_calc_depth():
    tx_count = 1
    assert calc_depth(tx_count) == 1
    tx_count = 2
    assert calc_depth(tx_count) == 2
    tx_count = 3
    assert calc_depth(tx_count) == 3
    tx_count = 4
    assert calc_depth(tx_count) == 3
    tx_count = 5
    assert calc_depth(tx_count) == 4
    tx_count = 13
    assert calc_depth(tx_count) == 5


def test_get_mtree_node_counts_per_level():
    base_node_count = 4
    assert get_mtree_node_counts_per_level(base_node_count) == [1, 2, 4]
    base_node_count = 7
    assert get_mtree_node_counts_per_level(base_node_count) == [1, 2, 4, 7]
    base_node_count = 13
    assert get_mtree_node_counts_per_level(base_node_count) == [1, 2, 4, 7, 13]

# def test_calc_mtree_base_level():
#     pass
#
#
# def test_build_mtree_from_base():
#     pass
#
# def test_calc_mtree():
#     pass
