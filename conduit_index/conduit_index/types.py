# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.
import typing

if typing.TYPE_CHECKING:
    import array

import bitcoinx


WorkUnit = tuple[bool, int, int, bytes, int, int, int, "array.ArrayType[int]"]
MainBatch = list[tuple[int, "array.ArrayType[int]", bitcoinx.Header, int]]
BatchedRawBlockSlices = list[tuple[bytes, int, int, int, bytes, int]]
TxHashRows = list[tuple[str]]
TxHashes = list[bytes]
TxHashToWorkIdMap = dict[bytes, int]
TxHashToOffsetMap = dict[bytes, int]
