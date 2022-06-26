from typing import NamedTuple

MerkleTree = dict[int, list[bytes]]


class MerkleTreeRow(NamedTuple):
    block_hash: bytes
    mtree: MerkleTree
    tx_count: int
