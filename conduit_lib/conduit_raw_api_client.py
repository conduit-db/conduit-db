import array
import logging
import os
import sys
from typing import Optional

import grpc
from bitcoinx import hex_str_to_hash

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(MODULE_DIR)
try:
    from conduit_lib import conduit_raw_pb2
    from conduit_lib import conduit_raw_pb2_grpc
    from conduit_lib.conduit_raw_pb2 import (BlockRequest, MerkleTreeRowRequest, MerkleTreeRowResponse,
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


class ConduitRawAPIClient:

    def __init__(self, host: str = '127.0.0.1', port: int = 5000):
        self.logger = logging.getLogger("conduit-raw-api-client")
        self.host = host
        self.port = port
        self.channel = grpc.insecure_channel(f"{host}:{port}")
        self.stub = conduit_raw_pb2_grpc.ConduitRawStub(self.channel)

    def ping(self, count):
        try:
            response: PingResponse = self.stub.Ping(conduit_raw_pb2.PingRequest(data='ping'))
            return response.message
        except _InactiveRpcError as e:
            self.logger.error(f"The ConduitRaw gRPC service is unreachable on: "
                              f"{self.host}:{self.port}")
        except Exception:
            self.logger.exception("unexpected exception")

    def get_block_num(self, block_hash: bytes) -> Optional[int]:
        try:
            response = self.stub.GetBlockNumber(BlockNumberRequest(blockHash=block_hash))
            return response.message
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                self.logger.error(f"Block num for block_hash: {block_hash.hex()} not found")
            else:
                self.logger.exception(e)
        except Exception as e:
            raise e

    def get_block(self, block_number: int) -> Optional[bytes]:
        try:
            response: BlockResponse = self.stub.GetBlock(BlockRequest(blockNumber=block_number))
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
            response = self.stub.GetMerkleTreeRow(MerkleTreeRowRequest(blockHash=block_hash, level=level))
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
            response: TransactionOffsetsResponse = self.stub.GetTransactionOffsets(
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
            response: BlockMetadataResponse = self.stub.GetBlockMetadata(
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
    block_hash = hex_str_to_hash("3b98a9b60e872b7328566ac1ea26608fc617d8805aabfc03ff075a7885cbe000")

    client = ConduitRawAPIClient()
    print(client.ping(0))
    # response = client.get_block_num(block_hash)
    # if response:
    #     print(response)
    #
    # response = client.get_block(10)
    # if response:
    #     print(response)
    # # print(f"len get_block response = {len(response)}")
    #
    # response = client.get_mtree_row(block_hash, level=0)
    # if response:
    #     print(response)
    # # print(f"len get_mtree_row response = {len(response)}")
    #
    # response = client.get_tx_offsets(block_hash)
    # if response:
    #     print(response)
    #
    # response = client.get_block_metadata(block_hash)
    # if response:
    #     print(response)
