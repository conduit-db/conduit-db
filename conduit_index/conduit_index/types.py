import array
from typing import List, Tuple, Dict

import bitcoinx

WorkUnit = Tuple[int, int, bytes, int, int, int, array.ArrayType]
MainBatch = List[Tuple[int, array.ArrayType, bitcoinx.Header, int]]
WorkPart = Tuple[int, bytes, int, int, int, array.ArrayType]
BatchedRawBlockSlices = List[tuple[array.ArrayType, int, int, int, int]]
ProcessedBlockAcks = List[tuple[int, int, bytes, list[bytes]]]
TxHashRows = List[Tuple[bytes]]
TxHashes = List[bytes]
TxHashToWorkIdMap = Dict[bytes, int]
TxHashToOffsetMap = Dict[bytes, int]
BlockSliceOffsets = Tuple[int, int]  # i.e. start and end byte offset for the slice
