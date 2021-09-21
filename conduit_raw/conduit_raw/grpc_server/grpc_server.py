"""The Python AsyncIO implementation of the GRPC helloworld.Greeter server."""

import logging
import asyncio
import os
import shutil
import threading
from pathlib import Path

import grpc

from conduit_lib.database.lmdb.lmdb_database import LMDB_Database

try:
    from .conduit_raw_pb2 import (PingRequest, PingResponse, BlockNumberRequest, BlockNumberResponse,
        BlockResponse, BlockRequest, MerkleTreeRowRequest, MerkleTreeRowResponse,
        TransactionOffsetsRequest, TransactionOffsetsResponse, BlockMetadataRequest,
        BlockMetadataResponse, StopRequest, StopResponse, TransactionOffsetsBatchedRequest,
        TransactionOffsetsBatchedResponse, BlockMetadataBatchedRequest,
        BlockMetadataBatchedResponse)

    from conduit_raw.conduit_raw.grpc_server import conduit_raw_pb2_grpc
except ImportError:
    from conduit_raw_pb2 import (PingRequest, PingResponse, BlockNumberRequest, BlockNumberResponse,
        BlockResponse, BlockRequest, MerkleTreeRowRequest, MerkleTreeRowResponse,
        TransactionOffsetsRequest, TransactionOffsetsResponse, BlockMetadataRequest,
        BlockMetadataResponse, StopRequest, StopResponse, TransactionOffsetsBatchedRequest,
        TransactionOffsetsBatchedResponse, BlockMetadataBatchedRequest,
        BlockMetadataBatchedResponse)

    import conduit_raw_pb2_grpc


# Coroutines to be invoked when the event loop is shutting down.
_cleanup_coroutines = []


class ConduitRaw(conduit_raw_pb2_grpc.ConduitRawServicer):

    def __init__(self, storage_path: Path):
        super().__init__()
        self.logger = logging.getLogger("conduit-raw-grpc")
        self.lmdb = LMDB_Database(storage_path=str(storage_path))

    async def Ping(self, request: PingRequest,
            context: grpc.aio.ServicerContext) -> PingResponse:
        return PingResponse(message=request.data)

    async def Stop(self, request: StopRequest, context: grpc.aio.ServicerContext):
        _task = asyncio.create_task(self.server_graceful_shutdown())
        return StopResponse(message='stopping')

    async def GetBlockNumber(self, request: BlockNumberRequest,
            context: grpc.aio.ServicerContext) -> BlockNumberResponse:
        # self.logger.debug(f"Got BlockNumberRequest.blockHash={request.blockHash}")
        block_number = self.lmdb.get_block_num(request.blockHash)
        return BlockNumberResponse(blockNumber=block_number)

    async def GetBlock(self, request: BlockRequest,
            context: grpc.aio.ServicerContext) -> BlockNumberResponse:
        # self.logger.debug(f"Got BlockRequest.blockHash={request.blockNumber}")
        raw_block = self.lmdb.get_block(request.blockNumber)
        return BlockResponse(rawBlock=raw_block)

    async def GetMerkleTreeRow(self, request: MerkleTreeRowRequest,
            context: grpc.aio.ServicerContext) -> MerkleTreeRowResponse:
        # self.logger.debug(f"Got MerkleTreeRowRequest.blockHash={request.blockHash}, "
        #       f"MerkleTreeRowRequest.level={request.level}")
        mtree_row = self.lmdb.get_mtree_row(request.blockHash, request.level)
        return MerkleTreeRowResponse(mtreeRow=mtree_row)

    async def GetTransactionOffsets(self, request: TransactionOffsetsRequest,
            context: grpc.aio.ServicerContext) -> TransactionOffsetsResponse:
        # self.logger.debug(f"Got TransactionOffsetsRequest.blockHash={request.blockHash}")
        tx_offsets = self.lmdb.get_tx_offsets(request.blockHash)
        return TransactionOffsetsResponse(txOffsetsArray=tx_offsets)

    async def GetTransactionOffsetsBatched(self, request: TransactionOffsetsBatchedRequest,
            context: grpc.aio.ServicerContext):
        # self.logger.debug(f"Got TransactionOffsetsBatchedRequest.blockHashes={request.blockHashes}")
        batch = []
        for block_hash in request.blockHashes:
            tx_offsets = self.lmdb.get_tx_offsets(block_hash)
            batch.append(TransactionOffsetsResponse(txOffsetsArray=tx_offsets))
        return TransactionOffsetsBatchedResponse(batch=batch)

    async def GetBlockMetadata(self, request: BlockMetadataRequest,
            context: grpc.aio.ServicerContext) -> BlockMetadataResponse:
        # self.logger.debug(f"Got BlockMetadataRequest.blockHash={request.blockHash}")
        block_size = self.lmdb.get_block_metadata(request.blockHash)
        return BlockMetadataResponse(blockSizeBytes=block_size)

    async def GetBlockMetadataBatched(self, request: BlockMetadataBatchedRequest,
            context: grpc.aio.ServicerContext) -> BlockMetadataBatchedResponse:
        # self.logger.debug(f"Got BlockMetadataRequest.blockHashes={request.blockHashes}")
        batch = []
        for block_hash in request.blockHashes:
            block_size = self.lmdb.get_block_metadata(block_hash)
            batch.append(block_size)
        return BlockMetadataBatchedResponse(batch=batch)

    async def server_graceful_shutdown(self):
        logging.info("Starting graceful shutdown...")
        # Shuts down the server with 0 seconds of grace period. During the
        # grace period, the server won't accept new connections and allow
        # existing RPCs to continue within the grace period.
        await self.server.stop(1)
        self.lmdb.close()

    async def serve(self) -> None:
        self.server = grpc.aio.server(maximum_concurrent_rpcs=10)
        conduit_raw_pb2_grpc.add_ConduitRawServicer_to_server(self, self.server)
        listen_addr = '[::]:50000'
        self.server.add_insecure_port(listen_addr)
        logging.info("Starting server on %s", listen_addr)
        await self.server.start()

        _cleanup_coroutines.append(self.server_graceful_shutdown())
        await self.server.wait_for_termination()


def run_server_thread(loop, storage_path):
    logging.basicConfig(level=logging.DEBUG)
    try:
        server = ConduitRaw(storage_path)
        loop.run_until_complete(server.serve())
    except KeyboardInterrupt:
        logging.debug("caught KeyboardInterrupt")
    finally:
        loop.run_until_complete(*_cleanup_coroutines)
        loop.close()
        if storage_path:
            shutil.rmtree(storage_path)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
    storage_path = MODULE_DIR / "test_lmdb"
    threading.Thread(target=run_server_thread, args=(loop,storage_path)).start()
