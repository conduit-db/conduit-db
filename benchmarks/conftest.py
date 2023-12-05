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
