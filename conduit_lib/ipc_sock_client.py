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
from conduit_lib.ipc_sock_msg_types import BlockBatchedRequestType


class SocketServerError(Exception):
    pass


class ServiceUnavailableError(Exception):
    """Only raised by ping() method"""
    pass



class IPCSocketClient:

    HOST, PORT = "localhost", 50000

    def __init__(self):
        self.logger = logging.getLogger('raw-socket-client')
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

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
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

    def block_number_batched(self, block_hashes: list[bytes]) \
            -> ipc_sock_msg_types.BlockNumberBatchedResponse:
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

    def block_batched(self, block_requests: list[BlockBatchedRequestType]) \
            -> bytearray:
        # Request
        msg_req = ipc_sock_msg_types.BlockBatchedRequest(block_requests)
        # self.logger.debug(f"Sending {ipc_sock_commands.BLOCK_BATCHED} request: {msg_req}")
        send_msg(self.sock, msg_req.to_cbor())

        # Recv
        data = self.receive_data()
        # NOTE: No cbor deserialization - this is a hot-path - needs to be fast!
        return data

    def merkle_tree_row(self, block_hash: bytes, level: int) \
            -> ipc_sock_msg_types.MerkleTreeRowResponse:
        # Request
        msg_req = ipc_sock_msg_types.MerkleTreeRowRequest(block_hash, level)
        # self.logger.debug(f"Sending {ipc_sock_commands.MERKLE_TREE_ROW} request: {msg_req}")
        send_msg(self.sock, msg_req.to_cbor())

        # Recv
        data = self.receive_data()
        cbor_obj = cbor2.loads(data)
        msg_resp = ipc_sock_msg_types.MerkleTreeRowResponse(**cbor_obj)
        # self.logger.debug(f"Received {ipc_sock_commands.MERKLE_TREE_ROW} response: {msg_resp}")
        return msg_resp

    def transaction_offsets_batched(self, block_hashes: list[bytes]) -> Generator:
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

    def block_metadata_batched(self, block_hashes: list[bytes]) \
            -> list[int]:
        # Request
        msg_req = ipc_sock_msg_types.BlockMetadataBatchedRequest(block_hashes)
        # self.logger.debug(f"Sending {ipc_sock_commands.BLOCK_METADATA_BATCHED} request: {msg_req}")
        send_msg(self.sock, msg_req.to_cbor())

        # Recv
        data = self.receive_data()
        cbor_obj = cbor2.loads(data)
        msg_resp = ipc_sock_msg_types.BlockMetadataBatchedResponse(**cbor_obj)
        # self.logger.debug(f"Received {ipc_sock_commands.BLOCK_METADATA_BATCHED} response: {msg_resp}")
        return msg_resp.block_sizes_batch

    def headers_batched(self, start_height: int, batch_size: int) \
            -> ipc_sock_msg_types.HeadersBatchedResponse:

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

