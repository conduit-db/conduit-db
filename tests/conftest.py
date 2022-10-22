import os
import stat
from pathlib import Path

from typing import Callable, Optional

MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))

os.environ['NETWORK'] = 'regtest'
os.environ['LMDB_DATABASE_DIR'] = './lmdb_test'
os.environ['RAW_BLOCKS_DIR'] = './raw_blocks'
os.environ['MERKLE_TREES_DIR'] = './merkle_trees'
os.environ['TX_OFFSETS_DIR'] = './tx_offsets'


def remove_readonly(func: Callable[[Path], None], path: Path,
        excinfo: Optional[BaseException]) -> None:
    os.chmod(path, stat.S_IWRITE)
    func(path)


with open(MODULE_DIR / "data/block400000.raw", "rb") as f:
    TEST_RAW_BLOCK_400000 = f.read()

with open(MODULE_DIR / "data/block413567.raw", "rb") as f:
    TEST_RAW_BLOCK_413567 = f.read()

with open(MODULE_DIR / "data/block700007.raw", "rb") as f:
    TEST_RAW_BLOCK_700007 = f.read()
