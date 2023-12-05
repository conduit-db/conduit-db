# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.

from typing import NamedTuple

MerkleTree = dict[int, list[bytes]]


class MerkleTreeRow(NamedTuple):
    block_hash: bytes
    mtree: MerkleTree
    tx_count: int
