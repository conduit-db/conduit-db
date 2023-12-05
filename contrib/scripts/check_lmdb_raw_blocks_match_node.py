# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.

import os
from pathlib import Path

from bitcoinx import double_sha256
from electrumsv_node import electrumsv_node

from conduit_lib import LMDB_Database
from conduit_lib.startup_utils import load_dotenv

MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
dotenv_path = MODULE_DIR.parent.parent / ".env"
load_dotenv(dotenv_path)
lmdb_db = LMDB_Database(lock=True)

result = electrumsv_node.call_any("getinfo").json()["result"]
block_height = result["blocks"]

for height in range(1, block_height):
    hex_block = electrumsv_node.call_any("getblockbyheight", height, 0).json()["result"]
    bin_block = bytes.fromhex(hex_block)
    header_hash = double_sha256(bin_block[0:80])
    block_num = lmdb_db.blocks.get_block_num(header_hash)
    bin_block_lmdb = lmdb_db.blocks.get_block(block_num)
    assert bin_block == bin_block_lmdb
    print(f"Block {height} of size: {len(bin_block)} check with LMDB store passed")

print("done")
