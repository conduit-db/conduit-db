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

channel = grpc.insecure_channel('127.0.0.1:5000', options=(('grpc.enable_http_proxy', 0),))
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
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                self.logger.error(f"Block num for block_hash: {block_hash.hex()} not found")
            else:
                self.logger.exception(e)
        except Exception as e:
            raise e

    def get_block(self, block_number: int) -> Optional[bytes]:
        try:
            response: BlockResponse = stub.GetBlock(BlockRequest(blockNumber=block_number))
            return response.rawBlock
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                self.logger.error(f"Block for block num: {block_number} not found")
            else:
                self.logger.exception(e)
        except Exception as e:
            raise e

    def get_mtree_row(self, block_hash: bytes, level: int):
        try:
            response = stub.GetMerkleTreeRow(MerkleTreeRowRequest(blockHash=block_hash, level=level))
            return response.mtreeRow
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                self.logger.error(f"Merkle tree row for block_hash: {block_hash.hex()} not found")
            else:
                self.logger.exception(e)
        except Exception as e:
            raise e

    def get_tx_offsets(self, block_hash: bytes) -> array.array:
        try:
            response: TransactionOffsetsResponse = stub.GetTransactionOffsets(
                TransactionOffsetsRequest(blockHash=block_hash))
            return response.txOffsetsArray
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                self.logger.error(f"Tx offsets for block_hash: {block_hash.hex()} not found")
            else:
                self.logger.exception(e)
        except Exception as e:
            raise e

    def get_block_metadata(self, block_hash: bytes) -> int:
        try:
            response: BlockMetadataResponse = stub.GetBlockMetadata(
                BlockMetadataRequest(blockHash=block_hash))
            return response.blockSizeBytes
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                self.logger.error(f"Block metadata for block_hash: {block_hash.hex()} not found")
            else:
                self.logger.exception(e)
        except Exception as e:
            raise e


if __name__ == '__main__':
    block_hash = bytes.fromhex("deadbeef")

    client = ConduitRawAPIClient()
    print(client.ping(0))
    response = client.get_block_num(block_hash)
    if response:
        print(response)

    response = client.get_block(10)
    if response:
        print(response)
    # print(f"len get_block response = {len(response)}")

    response = client.get_mtree_row(block_hash, level=0)
    if response:
        print(response)
    # print(f"len get_mtree_row response = {len(response)}")

    response = client.get_tx_offsets(block_hash)
    if response:
        print(response)

    response = client.get_block_metadata(block_hash)
    if response:
        print(response)
