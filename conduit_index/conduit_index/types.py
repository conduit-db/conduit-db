import typing

import bitcoinx

if typing.TYPE_CHECKING:
    import array


WorkUnit = tuple[bool, int, int, bytes, int, int, int, "array.ArrayType[int]"]
MainBatch = list[tuple[int, "array.ArrayType[int]", bitcoinx.Header, int]]
WorkPart = tuple[int, bytes, int, int, int, "array.ArrayType[int]"]
BatchedRawBlockSlices = list[tuple[bytes, int, int, int, bytes, int]]
TxHashRows = list[tuple[str]]
TxHashes = list[bytes]
TxHashToWorkIdMap = dict[bytes, int]
TxHashToOffsetMap = dict[bytes, int]
BlockSliceOffsets = tuple[int, int]  # i.e. start and end byte offset for the slice