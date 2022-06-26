import array

import bitcoinx

# NOTE(typing) For some reason we can't embed `array.ArrayType[int]` here:
#   TypeError: 'type' object is not subscriptable
WorkUnit = tuple[bool, int, int, bytes, int, int, int, 'array.ArrayType[int]']
MainBatch = list[tuple[int, 'array.ArrayType[int]', bitcoinx.Header, int]]
WorkPart = tuple[int, bytes, int, int, int, 'array.ArrayType[int]']
BatchedRawBlockSlices = list[tuple['array.ArrayType[int]', int, int, int, int]]
ProcessedBlockAcks = list[tuple[int, int, bytes, list[bytes]]]
TxHashRows = list[tuple[str]]
TxHashes = list[bytes]
TxHashToWorkIdMap = dict[bytes, int]
TxHashToOffsetMap = dict[bytes, int]
BlockSliceOffsets = tuple[int, int]  # i.e. start and end byte offset for the slice
