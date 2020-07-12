import time

from bitcoinx import double_sha256, hash_to_hex_str
from offsets import TX_OFFSETS


def calc_depth(leaves_count):
    result = leaves_count
    mtree_depth = 0
    while result >= 1:  # 1 represents merkle root
        result = result / 2
        mtree_depth += 1
    return mtree_depth


def calc_mtree_base_level(base_level, leaves_count, mtree):
    mtree[base_level] = []
    for i in range(leaves_count):
        if i < (leaves_count - 1):
            rawtx = raw_block[TX_OFFSETS[i]:TX_OFFSETS[i + 1]]
        else:
            rawtx = raw_block[TX_OFFSETS[i]:]
        tx_hash = double_sha256(rawtx)
        mtree[base_level].append(tx_hash)
    return mtree


def build_mtree_from_base(base_level, mtree):
    """if there is an odd number of hashes at a given level -> raise IndexError
    then duplicate the last hash, concatenate and double_sha256 to continue."""

    for current_level in reversed(range(1, base_level+1)):
        next_level_up = []
        hashes = mtree[current_level]
        for i in range(0, len(hashes), 2):
            try:
                _hash = double_sha256(hashes[i] + hashes[i+1])
            except IndexError:
                # print(f"index error at level={current_level}. i={i+1} index doesn't exist - there are {i+1} "
                #       f"hashes at this level")
                _hash = double_sha256(hashes[i] + hashes[i])
            finally:
                next_level_up.append(_hash)
        hashes = next_level_up
        mtree[current_level-1] = hashes


def calc_mtree(raw_block):
    """base_level refers to the bottom/widest part of the mtree (merkle root is level=0)"""
    mtree = {}
    leaves_count = len(TX_OFFSETS)
    base_level = calc_depth(leaves_count)
    mtree = calc_mtree_base_level(base_level, leaves_count, mtree)
    build_mtree_from_base(base_level, mtree)
    # for row in mtree.values():
    #     print(len(row))
    print(hash_to_hex_str(mtree[0][0]))


if __name__ == "__main__":
    with open("../data/block413567.raw", "rb") as f:
        raw_block = bytearray(f.read())
    t0 = time.time()
    calc_mtree(raw_block)
    t1 = time.time() - t0
    print(f"time taken={t1} seconds")
