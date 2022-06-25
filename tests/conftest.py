import os
import stat
from pathlib import Path

from typing import Callable, Optional

os.environ['NETWORK'] = 'regtest'
os.environ['LMDB_DATABASE_DIR'] = './lmdb_test'
os.environ['RAW_BLOCKS_DIR'] = './raw_blocks'
os.environ['MERKLE_TREES_DIR'] = './merkle_trees'
os.environ['TX_OFFSETS_DIR'] = './tx_offsets'

def remove_readonly(func: Callable[[Path], None], path: Path,
        excinfo: Optional[BaseException]) -> None:
    os.chmod(path, stat.S_IWRITE)
    func(path)
