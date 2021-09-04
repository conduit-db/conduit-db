import array
import logging
from typing import Optional

import grpc

try:
    from . import conduit_raw_pb2
    from . import conduit_raw_pb2_grpc
    from .conduit_raw_pb2 import (BlockRequest, MerkleTreeRowRequest, MerkleTreeRowResponse,
    BlockResponse, TransactionOffsetsRequest, TransactionOffsetsResponse, BlockMetadataRequest, \
    BlockMetadataResponse, PingResponse)
except ImportError:
    import conduit_raw_pb2
    import conduit_raw_pb2_grpc
    from conduit_raw_pb2 import (
        BlockRequest, MerkleTreeRowRequest, BlockResponse,
        TransactionOffsetsRequest, TransactionOffsetsResponse, BlockMetadataRequest,
        BlockMetadataResponse
    )

from grpc._channel import _InactiveRpcError

from conduit_lib.conduit_raw_pb2 import BlockNumberRequest

channel = grpc.insecure_channel('localhost:5000')
stub = conduit_raw_pb2_grpc.ConduitRawStub(channel)


class ConduitRawAPIClient:

    def __init__(self, conduit_raw_api_url: str = 'localhost:5000'):
        self.logger = logging.getLogger("conduit-raw-api-client")
        self.conduit_raw_api_url = conduit_raw_api_url
        self.channel = grpc.insecure_channel(conduit_raw_api_url)
        self.stub = conduit_raw_pb2_grpc.ConduitRawStub(channel)

    def ping(self, count):
        try:
            response: PingResponse = stub.Ping(conduit_raw_pb2.PingRequest(data='ping'))
            return response.message
        except _InactiveRpcError as e:
            self.logger.error(f"The ConduitRaw gRPC service is unreachable on: "
                              f"{self.conduit_raw_api_url}")
        except Exception:
            self.logger.exception("unexpected exception")

    def get_block_num(self, block_hash: bytes) -> Optional[int]:
        try:
            response = stub.GetBlockNumber(BlockNumberRequest(blockHash=block_hash))
            return response.blockNumber
        except _InactiveRpcError as e:
            self.logger.exception("unexpected exception")

    def get_block(self, block_number: int) -> Optional[bytes]:
        try:
            response: BlockResponse = stub.GetBlock(BlockRequest(blockNumber=block_number))
            return response.rawBlock
        except _InactiveRpcError as e:
            self.logger.exception("unexpected exception")
            return

    def get_mtree_row(self, block_hash: bytes, level: int):
        try:
            response = stub.GetMerkleTreeRow(MerkleTreeRowRequest(blockHash=block_hash, level=level))
            return response.mtreeRow
        except _InactiveRpcError as e:
            self.logger.exception("unexpected exception")

    def get_tx_offsets(self, block_hash: bytes) -> array.array:
        try:
            response: TransactionOffsetsResponse = stub.GetTransactionOffsets(
                TransactionOffsetsRequest(blockHash=block_hash))
            return response.txOffsetsArray
        except _InactiveRpcError as e:
            self.logger.exception("unexpected exception")

    def get_block_metadata(self, block_hash: bytes) -> int:
        try:
            response: BlockMetadataResponse = stub.GetBlockMetadata(
                BlockMetadataRequest(blockHash=block_hash))
            return response.blockSizeBytes
        except _InactiveRpcError as e:
            self.logger.exception("unexpected exception")


if __name__ == '__main__':
    block_hash = bytes.fromhex("deadbeef")

    client = ConduitRawAPIClient()
    print(client.ping(0))
    print(client.get_block_num(block_hash))

    response = client.get_block(10)
    print(response)
    # print(f"len get_block response = {len(response)}")

    response = client.get_mtree_row(block_hash, level=0)
    print(response)
    # print(f"len get_mtree_row response = {len(response)}")

    print(client.get_tx_offsets(block_hash))
    print(client.get_block_metadata(block_hash))
