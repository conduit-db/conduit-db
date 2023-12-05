# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.

import os
from pathlib import Path


MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))

os.environ["NETWORK"] = "regtest"
os.environ["DATADIR_HDD"] = "./datadir"
os.environ["DATADIR_SSD"] = "./datadir"
os.environ['SCYLLA_KEYSPACE'] = os.getenv('SCYLLA_KEYSPACE', 'condiutdbtest')

with open(MODULE_DIR / "data/block400000.raw", "rb") as f:
    TEST_RAW_BLOCK_400000 = f.read()

with open(MODULE_DIR / "data/block413567.raw", "rb") as f:
    TEST_RAW_BLOCK_413567 = f.read()

with open(MODULE_DIR / "data/block700007.raw", "rb") as f:
    TEST_RAW_BLOCK_700007 = f.read()
