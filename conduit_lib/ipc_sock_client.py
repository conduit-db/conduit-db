import array
import logging
import os
import socket
import time
from pathlib import Path
from typing import Generator

import cbor2

from conduit_lib.basic_socket_io import send_msg, recv_msg
from conduit_lib import ipc_sock_msg_types, ipc_sock_commands
from conduit_lib.ipc_sock_msg_types import BlockMetadataBatchedResponse
from conduit_lib.types import BlockMetadata, BlockSliceRequestType
from conduit_lib.utils import cast_to_valid_ipv4


BatchedBlockSlices = bytearray


class SocketServerError(Exception):
    pass


class ServiceUnavailableError(Exception):
    """Only raised by ping() method"""
    pass



class IPCSocketClient:

    def __init__(self):
        CONDUIT_RAW_API_HOST: str = os.environ.get('CONDUIT_RAW_API_HOST', 'localhost:50000')
        self.HOST = cast_to_valid_ipv4(CONDUIT_RAW_API_HOST.split(":")[0])
        self.PORT = int(CONDUIT_RAW_API_HOST.split(":")[1])
        self.logger = logging.getLogger('raw-socket-client')
        self.wait_for_connection()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.sock.close()

    def wait_for_connection(self):
        while True:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                self.sock.connect((self.HOST, self.PORT))
                break
            except ConnectionRefusedError:
                self.logger.debug(f"ConduitRaw is currently unavailable. Retrying to connect...")
                time.sleep(5)
                self.sock.close()

    def close(self):
        self.sock.shutdown(socket.SHUT_RDWR)
        self.sock.close()

    def receive_data(self):
        data = recv_msg(self.sock)
        if not data:
            raise SocketServerError("No data in response from server")
        return data


    def ping(self) -> ipc_sock_msg_types.PingResponse:
        # Send
        msg_req = ipc_sock_msg_types.PingRequest()
        # self.logger.debug(f"Sending {ipc_sock_commands.PING} request: {msg_req}")
        send_msg(self.sock, msg_req.to_cbor())

        # Recv
        data = self.receive_data()
        cbor_obj = cbor2.loads(data)
        msg_resp = ipc_sock_msg_types.PingResponse(**cbor_obj)
        # self.logger.debug(f"Received {ipc_sock_commands.PING} response: {msg_resp}")
        return msg_resp

    def stop(self) -> ipc_sock_msg_types.StopResponse:
        # Send
        msg_req = ipc_sock_msg_types.StopRequest()
        # self.logger.debug(f"Sending {ipc_sock_commands.STOP} request: {msg_req}")
        send_msg(self.sock, msg_req.to_cbor())

        # Recv
        data = self.receive_data()
        cbor_obj = cbor2.loads(data)
        msg_resp = ipc_sock_msg_types.StopResponse(**cbor_obj)
        # self.logger.debug(f"Received {ipc_sock_commands.STOP} response: {msg_resp}")
        return msg_resp

    def chain_tip(self) -> tuple[bytes, int]:
        try:
            # Send
            msg_req = ipc_sock_msg_types.ChainTipRequest()
            # self.logger.debug(f"Sending {ipc_sock_commands.CHAIN_TIP} request: {msg_req}")
            send_msg(self.sock, msg_req.to_cbor())

            # Recv
            data = self.receive_data()
            cbor_obj = cbor2.loads(data)
            msg_resp = ipc_sock_msg_types.ChainTipResponse(**cbor_obj)
            # self.logger.debug(f"Received {ipc_sock_commands.CHAIN_TIP} response: {msg_resp}")
            return msg_resp.header, msg_resp.height
        except ConnectionResetError:
            self.wait_for_connection()
            return self.chain_tip()  # recurse

    def block_number_batched(self, block_hashes: list[bytes]) \
            -> ipc_sock_msg_types.BlockNumberBatchedResponse:
        try:
            # Request
            msg_req = ipc_sock_msg_types.BlockNumberBatchedRequest(block_hashes=block_hashes)
            # self.logger.debug(f"Sending {ipc_sock_commands.BLOCK_NUMBER_BATCHED} request: {msg_req}")
            send_msg(self.sock, msg_req.to_cbor())

            # Recv
            data = self.receive_data()
            cbor_obj = cbor2.loads(data)
            msg_resp = ipc_sock_msg_types.BlockNumberBatchedResponse(**cbor_obj)
            # self.logger.debug(f"Received {ipc_sock_commands.BLOCK_NUMBER_BATCHED} response: {msg_resp}")
            return msg_resp
        except ConnectionResetError:
            self.wait_for_connection()
            return self.block_number_batched(block_hashes)  # recurse

    def block_batched(self, block_requests: list[BlockSliceRequestType]) \
            -> BatchedBlockSlices:
        """The packing protocol is a contiguous array of:
             block_number uint32,
             len_slice uin64,
             raw_block_slice bytes"""
        try:
            # Request
            msg_req = ipc_sock_msg_types.BlockBatchedRequest(block_requests)
            self.logger.debug(f"Sending {ipc_sock_commands.BLOCK_BATCHED} request: {msg_req}")
            send_msg(self.sock, msg_req.to_cbor())

            # Recv
            data = self.receive_data()
            # NOTE: No cbor deserialization - this is a hot-path - needs to be fast!
            return data
        except ConnectionResetError:
            self.wait_for_connection()
            return self.block_batched(block_requests)  # recurse

    def merkle_tree_row(self, block_hash: bytes, level: int) \
            -> ipc_sock_msg_types.MerkleTreeRowResponse:
        try:
            # Request
            msg_req = ipc_sock_msg_types.MerkleTreeRowRequest(block_hash, level)
            # self.logger.debug(f"Sending {ipc_sock_commands.MERKLE_TREE_ROW} request: {msg_req}")
            send_msg(self.sock, msg_req.to_cbor())

            # Recv
            data = self.receive_data()
            command = ipc_sock_commands.MERKLE_TREE_ROW
            msg_resp = ipc_sock_msg_types.MerkleTreeRowResponse(mtree_row=data, command=command)
            self.logger.debug(f"Received {ipc_sock_commands.MERKLE_TREE_ROW} response: {msg_resp}")
            return msg_resp
        except ConnectionResetError:
            self.wait_for_connection()
            return self.merkle_tree_row(block_hashes)  # recurse

    def transaction_offsets_batched(self, block_hashes: list[bytes]) -> Generator:
        try:
            # Request
            msg_req = ipc_sock_msg_types.TransactionOffsetsBatchedRequest(block_hashes)
            # self.logger.debug(f"Sending {ipc_sock_commands.TRANSACTION_OFFSETS_BATCHED} request: {msg_req}")
            send_msg(self.sock, msg_req.to_cbor())

            # Recv
            data = self.receive_data()
            cbor_obj = cbor2.loads(data)
            msg_resp = ipc_sock_msg_types.TransactionOffsetsBatchedResponse(**cbor_obj)
            # self.logger.debug(f"Received {ipc_sock_commands.TRANSACTION_OFFSETS_BATCHED} response: {msg_resp}")
            for tx_offsets_array in msg_resp.tx_offsets_batch:
                yield array.array("Q", tx_offsets_array)
        except ConnectionResetError:
            self.wait_for_connection()
            return self.transaction_offsets_batched(block_hashes)  # recurse

    def block_metadata_batched(self, block_hashes: list[bytes]) -> BlockMetadataBatchedResponse:
        try:
            # Request
            msg_req = ipc_sock_msg_types.BlockMetadataBatchedRequest(block_hashes)
            self.logger.debug(f"Sending {ipc_sock_commands.BLOCK_METADATA_BATCHED} request: {msg_req}")
            send_msg(self.sock, msg_req.to_cbor())

            # Recv
            data = self.receive_data()
            cbor_obj = cbor2.loads(data)
            msg_resp = ipc_sock_msg_types.BlockMetadataBatchedResponse(**cbor_obj)
            self.logger.debug(f"Received {ipc_sock_commands.BLOCK_METADATA_BATCHED} response: {msg_resp}")
            return msg_resp
        except ConnectionResetError:
            self.wait_for_connection()
            return self.block_metadata_batched(block_hashes)  # recurse

    def headers_batched(self, start_height: int, batch_size: int) \
            -> ipc_sock_msg_types.HeadersBatchedResponse:
        try:
            # Request
            msg_req = ipc_sock_msg_types.HeadersBatchedRequest(start_height, batch_size)
            # self.logger.debug(f"Sending {ipc_sock_commands.HEADERS_BATCHED} request: {msg_req}")
            send_msg(self.sock, msg_req.to_cbor())

            # Recv
            data = self.receive_data()
            cbor_obj = cbor2.loads(data)
            msg_resp = ipc_sock_msg_types.HeadersBatchedResponse(**cbor_obj)
            # self.logger.debug(f"Received {ipc_sock_commands.HEADERS_BATCHED} response: {msg_resp}")
            return msg_resp
        except ConnectionResetError:
            self.wait_for_connection()
            return self.headers_batched(start_height, batch_size)  # recurse


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    from conduit_lib.store import setup_headers_store
    from conduit_lib.networks import RegTestNet
    MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
    storage_path = MODULE_DIR / "test_lmdb"
    block_headers = setup_headers_store(RegTestNet(),
        "../conduit_raw/conduit_raw/sock_server/test_headers.mmap")


    block_hash = block_headers.longest_chain().tip.hash
    block_hashes = [block_hash, block_hash, block_hash]

    # Short-lived connection
    client = IPCSocketClient()
    result = client.ping()
    print(result)

    # result = client.stop()
    # print(result)

    # result = client.chain_tip()
    # print(result)

    # result = client.block_number_batched(block_hashes)
    # print(result)

    # block_requests = [
    #     (1, 0, 0),
    #     (2, 0, 0),
    #     (3, 0, 0)
    # ]
    # result = client.block_batched(block_requests, batch_id=1)
    # mtree_row = client.merkle_tree_row(block_hash, level=0)

    # for tx_offsets_array in client.transaction_offsets_batched(block_hashes):
    #     print(tx_offsets_array)

    # result = client.block_metadata_batched(block_hashes)
    # print(result)

    # result = client.headers_batched(start_height=0, batch_size=2000)
    # print(result)

    client.close()

    # # Long-lived connection
    # client = IPCSocketClient()
    #
    # while True:
    #     client.ping()

