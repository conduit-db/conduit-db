import grpc
import array
import logging
import os
import sys
from typing import Optional, List, Tuple
from bitcoinx import hex_str_to_hash, hash_to_hex_str

from conduit_lib.utils import cast_to_valid_ipv4

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(MODULE_DIR)
try:
    from conduit_lib import conduit_raw_pb2
    from conduit_lib import conduit_raw_pb2_grpc
    from conduit_lib.conduit_raw_pb2 import (BlockRequest, MerkleTreeRowRequest, MerkleTreeRowResponse,
    BlockResponse, TransactionOffsetsRequest, TransactionOffsetsResponse, BlockMetadataRequest,
    BlockMetadataResponse, PingResponse, BlockNumberResponse, StopResponse,
    TransactionOffsetsBatchedResponse, TransactionOffsetsBatchedRequest,
    BlockMetadataBatchedResponse, BlockMetadataBatchedRequest, BlockHeadersBatchedRequest,
    BlockHeadersBatchedResponse, BlockBatchedRequest, BlockBatchedResponse,
    BlockNumberBatchedResponse, BlockNumberBatchedRequest, ChainTipRequest, ChainTipResponse)
except ImportError:
    import conduit_raw_pb2
    import conduit_raw_pb2_grpc
    from conduit_raw_pb2 import (BlockRequest, MerkleTreeRowRequest, MerkleTreeRowResponse,
    BlockResponse, TransactionOffsetsRequest, TransactionOffsetsResponse, BlockMetadataRequest,
    BlockMetadataResponse, PingResponse, BlockNumberResponse, StopResponse,
    TransactionOffsetsBatchedResponse, TransactionOffsetsBatchedRequest,
    BlockMetadataBatchedResponse, BlockMetadataBatchedRequest, BlockHeadersBatchedRequest,
    BlockHeadersBatchedResponse, BlockBatchedRequest, BlockBatchedResponse,
    BlockNumberBatchedResponse, BlockNumberBatchedRequest)

from grpc._channel import _InactiveRpcError

from conduit_lib.conduit_raw_pb2 import BlockNumberRequest


class ServiceUnavailableError(Exception):
    """Only raised by ping() method"""
    pass


class ConduitRawAPIClient:

    def __init__(self, host: str = '127.0.0.1', port: int = 50000):
        self.logger = logging.getLogger("conduit-raw-api-client")
        self.logger.setLevel(logging.DEBUG)
        # Todo - this is horrible, unsightly code - need to unify configuration better
        CONDUIT_RAW_API_HOST: str = os.environ.get('CONDUIT_RAW_API_HOST', '127.0.0.1:50000')
        self.host = cast_to_valid_ipv4(CONDUIT_RAW_API_HOST.split(":")[0])
        self.port = int(CONDUIT_RAW_API_HOST.split(":")[1])
        self.channel = grpc.insecure_channel(f"{self.host}:{self.port}", options=[
            ('grpc.max_send_message_length', 50 * 1024 * 1024),
            ('grpc.max_receive_message_length', 50 * 1024 * 1024)
        ])
        self.stub = conduit_raw_pb2_grpc.ConduitRawStub(self.channel)

    def close(self):
        self.channel.close()

    def ping(self, count, wait_for_ready=True):
        try:
            response: PingResponse = self.stub.Ping(conduit_raw_pb2.PingRequest(data='ping'),
                wait_for_ready=wait_for_ready)
            return response.message
        except _InactiveRpcError as e:
            raise ServiceUnavailableError()
        except Exception:
            self.logger.exception("unexpected exception")

    def stop(self):
        try:
            response: StopResponse = self.stub.Stop(conduit_raw_pb2.StopRequest(message='stop'),
                wait_for_ready=True)
            return response.message
        except _InactiveRpcError as e:
            self.logger.error(f"The ConduitRaw gRPC service is unreachable on: "
                              f"{self.host}:{self.port}")
        except Exception:
            self.logger.exception("unexpected exception")

    def get_chain_tip(self) -> Optional[Tuple[bytes, int]]:
        try:
            response: ChainTipResponse = self.stub.GetChainTip(
                ChainTipRequest(), wait_for_ready=True)
            return response.header, response.height
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                self.logger.error(f"Chain tip for block_hash: {hash_to_hex_str(block_hash)} "
                    f"not found")
            else:
                self.logger.exception(e)
        except Exception as e:
            raise e

    def get_block_num(self, block_hash: bytes) -> Optional[int]:
        try:
            response: BlockNumberResponse = self.stub.GetBlockNumber(
                BlockNumberRequest(blockHash=block_hash), wait_for_ready=True)
            return response.blockNumber
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                self.logger.error(f"Block num for block_hash: {hash_to_hex_str(block_hash)} "
                    f"not found")
            else:
                self.logger.exception(e)
        except Exception as e:
            raise e

    def get_block_num_batched(self, block_hashes: List[bytes]) -> List[int]:
        try:
            response: BlockNumberBatchedResponse = self.stub.GetBlockNumberBatched(
                BlockNumberBatchedRequest(blockHashes=block_hashes), wait_for_ready=True)
            return response.blockNumbers
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                self.logger.error(f"Block metadata for block_hash: {hash_to_hex_str(block_hash)} "
                    f"not found")
            else:
                self.logger.exception(e)
        except Exception as e:
            raise e

    def get_block(self, block_number: int, start_offset: int = 0,
            end_offset: int = 0) -> Optional[bytes]:
        try:
            response: BlockResponse = self.stub.GetBlock(
                BlockRequest(blockNumber=block_number, startOffset=start_offset,
                    endOffset=end_offset), wait_for_ready=True)
            return response.rawBlock
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                self.logger.error(f"Block for block num: {block_number} not found")
            else:
                self.logger.exception(e)
        except Exception as e:
            raise e

    def get_block_batched(self, block_requests: List[Tuple[int, Tuple[int, int]]], batch_id: int,
            sock_host: str, sock_port: int) \
            -> bool:
        cur_block_num = 0  # for exception logging
        try:
            proto_block_requests = []
            for block_number, slice_begin_and_end in block_requests:
                cur_block_num = block_number
                start_offset, end_offset = slice_begin_and_end
                proto_block_request = BlockRequest(blockNumber=block_number,
                    startOffset=start_offset, endOffset=end_offset)
                proto_block_requests.append(proto_block_request)

            response: BlockBatchedResponse = self.stub.GetBlockBatched(
                BlockBatchedRequest(blockRequests=proto_block_requests, batchId=batch_id,
                    sockHost=sock_host, sockPort=sock_port),
                wait_for_ready=True)

            return response.allSent  # all sent over raw socket
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                self.logger.error(f"Block for block num: {cur_block_num} not found")
            else:
                self.logger.exception(e)
        except Exception as e:
            raise e

    def get_mtree_row(self, block_hash: bytes, level: int):
        try:
            response = self.stub.GetMerkleTreeRow(
                MerkleTreeRowRequest(blockHash=block_hash, level=level),
                    wait_for_ready=True)
            return response.mtreeRow
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                self.logger.error(f"Merkle tree row for block_hash: {hash_to_hex_str(block_hash)} "
                    f"not found")
            else:
                self.logger.exception(e)
        except Exception as e:
            raise e

    def get_tx_offsets(self, block_hash: bytes) -> array.array:
        try:
            response: TransactionOffsetsResponse = self.stub.GetTransactionOffsets(
                TransactionOffsetsRequest(blockHash=block_hash), wait_for_ready=True)
            return array.array("Q", response.txOffsetsArray)
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                self.logger.error(f"Tx offsets for block_hash: {hash_to_hex_str(block_hash)} "
                    f"not found")
            else:
                self.logger.exception(e)
        except Exception as e:
            raise e

    def get_tx_offsets_batched(self, block_hashes: List[bytes]) -> List[array.array]:
        try:
            response: TransactionOffsetsBatchedResponse = self.stub.GetTransactionOffsetsBatched(
                TransactionOffsetsBatchedRequest(blockHashes=block_hashes), wait_for_ready=True)
            return [array.array("Q", r.txOffsetsArray) for r in response.batch]
        except grpc.RpcError as e:
            self.logger.exception(e)
        except Exception as e:
            raise e

    def get_block_metadata(self, block_hash: bytes) -> int:
        try:
            response: BlockMetadataResponse = self.stub.GetBlockMetadata(
                BlockMetadataRequest(blockHash=block_hash), wait_for_ready=True)
            return response.blockSizeBytes
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                self.logger.error(f"Block metadata for block_hash: {hash_to_hex_str(block_hash)} "
                    f"not found")
            else:
                self.logger.exception(e)
        except Exception as e:
            raise e

    def get_block_metadata_batched(self, block_hashes: List[bytes]) -> List[int]:
        try:
            response: BlockMetadataBatchedResponse = self.stub.GetBlockMetadataBatched(
                BlockMetadataBatchedRequest(blockHashes=block_hashes), wait_for_ready=True)
            return response.batch
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                self.logger.error(f"Block metadata for block_hash: {hash_to_hex_str(block_hash)} "
                    f"not found")
            else:
                self.logger.exception(e)
        except Exception as e:
            raise e

    def get_block_headers_batched(self, start_height: int = 0, batch_size: int = 500,
            wait_for_ready=True, timeout=None):
        """If end_height=0 it means give me the max batch size (500 headers)"""
        try:
            response: BlockHeadersBatchedResponse = self.stub.GetHeadersBatched(
                BlockHeadersBatchedRequest(startHeight=start_height, batchSize=batch_size),
                wait_for_ready=wait_for_ready, timeout=timeout)

            return response.headers
        except grpc.RpcError as e:
            if e.code() == grpc.StatusCode.NOT_FOUND:
                self.logger.error(f"Block metadata for block_hash: {hash_to_hex_str(block_hash)} "
                    f"not found")
            else:
                raise e
        except Exception as e:
            raise e


if __name__ == '__main__':
    block_hash = hex_str_to_hash("3063788ed0dbd70428d4f58c465e2acda71bbcf82cacb21375e1ce9f9bdb5437")

    client = ConduitRawAPIClient()

    # print(client.ping(0))
    #
    response = client.get_chain_tip()
    if response:
        print(response)
    #
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
