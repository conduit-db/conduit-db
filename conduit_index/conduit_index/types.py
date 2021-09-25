import array
from typing import List, Tuple

import bitcoinx

WorkUnit = Tuple[int, bytes, int, int, int, array.array]
MainBatch = List[Tuple[int, array.array, bitcoinx.Header]]
