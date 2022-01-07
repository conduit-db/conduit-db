import os
import stat
from pathlib import Path

from typing import Callable, Optional

os.environ['NETWORK'] = 'regtest'


def remove_readonly(func: Callable[[Path], None], path: Path,
        excinfo: Optional[BaseException]) -> None:
    os.chmod(path, stat.S_IWRITE)
    func(path)
