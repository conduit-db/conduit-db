from typing import Dict, NamedTuple

MerkleTree = Dict[int, list[bytes]]


class MerkleTreeRow(NamedTuple):
    block_hash: bytes
    mtree: MerkleTree
    tx_count: int
