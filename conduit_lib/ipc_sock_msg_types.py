"""This is kind of a replacement for protobufs where we can still get type safety but
without introducing heavy new technologies to the tech stack. We should constrain ourselves
to primitive types supported by cbor by default for simplicity sake (although I am sure we
could extend it if we wanted)."""
import json
from typing import Optional

import cbor2
from bitcoinx import hash_to_hex_str

from conduit_lib.types import BlockMetadata, BlockSliceRequestType, ChainHashes

try:
    from conduit_lib import ipc_sock_commands
except ImportError:
    import rs_server_commands


BlockHashes = list[bytes]
BlockHeaders = list[bytes]


class BaseMsg:

    def __init__(self, *args, **kwargs):
        pass

    def to_cbor(self) -> bytes:
        raise NotImplementedError

    def to_json(self) -> str:
        raise NotImplementedError

    def __repr__(self):
        return str(self.to_json())


class PingRequest(BaseMsg):
    command = ipc_sock_commands.PING

    def to_cbor(self) -> bytes:
        return cbor2.dumps({'command': self.command})

    def to_json(self) -> str:
        return json.dumps({'command': self.command})


class PingResponse(BaseMsg):
    command = ipc_sock_commands.PING

    def to_cbor(self) -> bytes:
        return cbor2.dumps({'command': self.command})

    def to_json(self) -> str:
        return json.dumps({'command': self.command})


class StopRequest(BaseMsg):
    command = ipc_sock_commands.STOP

    def to_cbor(self) -> bytes:
        return cbor2.dumps({'command': self.command})

    def to_json(self) -> str:
        return json.dumps({'command': self.command})


class StopResponse(BaseMsg):
    command = ipc_sock_commands.STOP

    def to_cbor(self) -> bytes:
        return cbor2.dumps({})

    def to_json(self) -> str:
        return json.dumps({'command': self.command})


class ChainTipRequest(BaseMsg):
    command = ipc_sock_commands.CHAIN_TIP

    def to_cbor(self) -> bytes:
        return cbor2.dumps({'command': self.command})

    def to_json(self) -> str:
        return json.dumps({'command': ipc_sock_commands.CHAIN_TIP})



class ChainTipResponse(BaseMsg):
    command = ipc_sock_commands.CHAIN_TIP

    def __init__(self, header: bytes, height: int, command: Optional[str]=None):
        super().__init__()
        self.header = header
        self.height = height

    def to_cbor(self) -> bytes:
        return cbor2.dumps({'command': self.command, 'header': self.header, 'height': self.height})

    def to_json(self) -> str:
        return json.dumps({
            'command': self.command,
            'header': self.header.hex(),
            'height': self.height
        })



class BlockNumberBatchedRequest(BaseMsg):
    command = ipc_sock_commands.BLOCK_NUMBER_BATCHED

    def __init__(self, block_hashes: BlockHashes, command: Optional[str]=None):
        super().__init__()
        self.block_hashes = block_hashes

    def to_cbor(self) -> bytes:
        return cbor2.dumps({'command': self.command, 'block_hashes': self.block_hashes})

    def to_json(self) -> str:
        return json.dumps({'command': self.command, 'block_hashes': [hash_to_hex_str(x) for x in self.block_hashes]})


class BlockNumberBatchedResponse(BaseMsg):
    command = ipc_sock_commands.BLOCK_NUMBER_BATCHED

    def __init__(self, block_numbers: list[int], command: Optional[str]=None):
        super().__init__()
        self.block_numbers = block_numbers

    def to_cbor(self) -> bytes:
        return cbor2.dumps({'command': self.command, 'block_numbers': self.block_numbers})

    def to_json(self) -> str:
        return json.dumps({'command': self.command, 'block_numbers': self.block_numbers})


class BlockBatchedRequest(BaseMsg):
    command = ipc_sock_commands.BLOCK_BATCHED

    def __init__(self, block_requests: list[BlockSliceRequestType], command: Optional[str]=None):
        super().__init__()
        self.block_requests = block_requests

    def to_cbor(self) -> bytes:
        return cbor2.dumps({
            'command': self.command,
            'block_requests': self.block_requests,
        })

    def to_json(self) -> str:
        return json.dumps({
            'command': self.command,
            'block_requests': self.block_requests,
        })

## Commented out because response for raw block slices is performance critical
## do not use cbor serialization - directly pack the bytearray for sending over the socket.
# class BlockBatchedResponse(BaseMsg):
#     command = rs_server_commands.BLOCK_BATCHED
#
#     def __init__(self, raw_block_slices_array: bytes, command: Optional[str]=None):
#         super().__init__()
#         self.raw_block_slices_array = raw_block_slices_array
#
#     def to_cbor(self) -> bytes:
#         return cbor2.dumps({'command': self.command, 'raw_block_slices_array': self.raw_block_slices_array})
#
#     def to_json(self) -> str:
#         return json.dumps({'command': self.command, 'raw_block_slices_array': self.raw_block_slices_array.hex()})


class MerkleTreeRowRequest(BaseMsg):
    command = ipc_sock_commands.MERKLE_TREE_ROW

    def __init__(self, block_hash: bytes, level: int, command: Optional[str]=None):
        super().__init__()
        self.block_hash = block_hash
        self.level = level

    def to_cbor(self) -> bytes:
        return cbor2.dumps({
            'command': self.command,
            'block_hash': self.block_hash,
            'level': self.level
        })

    def to_json(self) -> str:
        return json.dumps({
            'command': self.command,
            'block_hash': hash_to_hex_str(self.block_hash),
            'level': self.level
        })


class MerkleTreeRowResponse(BaseMsg):
    command = ipc_sock_commands.MERKLE_TREE_ROW

    def __init__(self, mtree_row: bytes, command: Optional[str]=None):
        super().__init__()
        self.mtree_row = mtree_row

    # Cbor serialization is not used for efficiency
    def to_cbor(self) -> bytes:
        return cbor2.dumps({'command': self.command, 'mtree_row': self.mtree_row})

    def to_json(self) -> str:
        return json.dumps({
            'command': self.command,
            'mtree_row': self.mtree_row.hex()
        })


class TransactionOffsetsBatchedRequest(BaseMsg):
    command = ipc_sock_commands.TRANSACTION_OFFSETS_BATCHED

    def __init__(self, block_hashes: BlockHashes, command: Optional[str]=None):
        super().__init__()
        self.block_hashes = block_hashes

    def to_cbor(self) -> bytes:
        return cbor2.dumps({'command': self.command, 'block_hashes': self.block_hashes})

    def to_json(self) -> str:
        return json.dumps({'command': self.command, 'block_hashes': [x.hex() for x in self.block_hashes]})


class TransactionOffsetsBatchedResponse(BaseMsg):
    command = ipc_sock_commands.TRANSACTION_OFFSETS_BATCHED

    def __init__(self, tx_offsets_batch: list[bytes], command: Optional[str]=None):
        super().__init__()
        self.tx_offsets_batch = tx_offsets_batch

    def to_cbor(self) -> bytes:
        return cbor2.dumps({'command': self.command, 'tx_offsets_batch': self.tx_offsets_batch})

    def to_json(self) -> str:
        return json.dumps({'command': self.command, 'tx_offsets_batch': [x.hex() for x in self.tx_offsets_batch]})


class BlockMetadataBatchedRequest(BaseMsg):
    command = ipc_sock_commands.BLOCK_METADATA_BATCHED

    def __init__(self, block_hashes: BlockHashes, command: Optional[str]=None):
        super().__init__()
        self.block_hashes = block_hashes

    def to_cbor(self) -> bytes:
        return cbor2.dumps({'command': self.command, 'block_hashes': self.block_hashes})

    def to_json(self) -> str:
        return json.dumps({'command': self.command, 'block_hashes': [hash_to_hex_str(x) for x in self.block_hashes]})


class BlockMetadataBatchedResponse(BaseMsg):
    command = ipc_sock_commands.BLOCK_METADATA_BATCHED

    def __init__(self, block_metadata_batch: list[BlockMetadata], command: Optional[str]=None):
        super().__init__()
        # Cast to BlockMetadata again because cbor converts tuples to lists
        self.block_metadata_batch = [BlockMetadata(block_size, tx_count) for block_size, tx_count in block_metadata_batch]


    def to_cbor(self) -> bytes:
        return cbor2.dumps({'command': self.command, 'block_metadata_batch': self.block_metadata_batch})

    def to_json(self) -> str:
        return json.dumps({'command': self.command, 'block_metadata_batch': self.block_metadata_batch})


class HeadersBatchedRequest(BaseMsg):
    command = ipc_sock_commands.HEADERS_BATCHED

    def __init__(self, start_height: int, batch_size: int, command: Optional[str]=None):
        super().__init__()
        self.start_height = start_height
        self.batch_size = batch_size

    def to_cbor(self) -> bytes:
        return cbor2.dumps({
            'command': self.command,
            'start_height': self.start_height,
            'batch_size': self.batch_size
        })

    def to_json(self) -> str:
        return json.dumps({
            'command': self.command,
            'start_height': self.start_height,
            'batch_size': self.batch_size
        })


class HeadersBatchedResponse(BaseMsg):
    command = ipc_sock_commands.HEADERS_BATCHED

    def __init__(self, headers_batch: BlockHeaders, command: Optional[str]=None):
        super().__init__()
        self.headers_batch = headers_batch

    def to_cbor(self) -> bytes:
        return cbor2.dumps({'command': self.command, 'headers_batch': self.headers_batch})

    def to_json(self) -> str:
        return json.dumps({'command': self.command, 'headers_batch': [x.hex() for x in self.headers_batch]})


class ReorgDifferentialRequest(BaseMsg):
    command = ipc_sock_commands.REORG_DIFFERENTIAL

    def __init__(self, old_hashes: ChainHashes, new_hashes: ChainHashes, command: Optional[str]=None):
        super().__init__()
        self.old_hashes = old_hashes
        self.new_hashes = new_hashes

    def to_cbor(self) -> bytes:
        return cbor2.dumps({
            'command': self.command,
            'old_hashes': self.old_hashes,
            'new_hashes': self.new_hashes,
        })

    def to_json(self) -> str:
        return json.dumps({
            'command': self.command,
            'old_hashes': [x.hex() for x in self.old_hashes],
            'new_hashes': [x.hex() for x in self.new_hashes]
        })


class ReorgDifferentialResponse(BaseMsg):
    command = ipc_sock_commands.REORG_DIFFERENTIAL

    def __init__(self, removals_from_mempool: set[bytes], additions_to_mempool: set[bytes], command: Optional[str]=None):
        super().__init__()
        self.removals_from_mempool = removals_from_mempool
        self.additions_to_mempool = additions_to_mempool

    def to_cbor(self) -> bytes:
        return cbor2.dumps({
            'command': self.command,
            'removals_from_mempool': self.removals_from_mempool,
            'additions_to_mempool': self.additions_to_mempool

        })

    def to_json(self) -> str:
        return json.dumps({
            'command': self.command,
            'removals_from_mempool': [x.hex() for x in self.removals_from_mempool],
            'additions_to_mempool': [x.hex() for x in self.additions_to_mempool]
        })
