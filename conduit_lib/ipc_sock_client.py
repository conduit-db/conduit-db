# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.

import array
import logging
import os
import socket
import time
from types import TracebackType
from typing import cast, Iterator, Type, Any

import cbor2
from bitcoinx import MissingHeader

from conduit_lib.basic_socket_io import send_msg, recv_msg
from conduit_lib import ipc_sock_msg_types, ipc_sock_commands
from conduit_lib.ipc_sock_msg_types import BlockMetadataBatchedResponse
from conduit_lib.types import BlockSliceRequestType, ChainHashes, BlockHeaderRow

BatchedBlockSlices = bytearray


class SocketServerError(Exception):
    pass


class ServiceUnavailableError(Exception):
    """Only raised by ping() method"""

    pass


class IPCSocketClient:
    is_open = True

    def __init__(self) -> None:
        self.HOST: str = os.environ.get("IPC_SOCKET_SERVER_HOST", "127.0.0.1")
        self.PORT: int = int(os.environ.get("IPC_SOCKET_SERVER_PORT", "50000"))
        self.logger: logging.Logger = logging.getLogger("raw-socket-client")
        self.wait_for_connection()

    def __enter__(self) -> "IPCSocketClient":
        return self

    def __exit__(
        self,
        exc_type: Type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None:
        self.sock.close()

    def wait_for_connection(self) -> None:
        while self.is_open:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            try:
                self.sock.connect((self.HOST, self.PORT))
                break
            except ConnectionRefusedError:
                self.logger.debug(f"ConduitRaw is currently unavailable. Retrying to connect...")
                time.sleep(5)
                self.sock.close()
            except Exception as e:
                self.logger.error(
                    f"Unexpected exception in wait_for_connection for "
                    f"host: {self.HOST}, port: {self.PORT}"
                )
                time.sleep(1)

    def close(self) -> None:
        self.is_open = False
        self.sock.shutdown(socket.SHUT_RDWR)
        self.sock.close()

    def receive_data(self) -> bytearray:
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
        cbor_obj = cast(dict[Any, Any], cbor2.loads(data))
        msg_resp = ipc_sock_msg_types.PingResponse(**cbor_obj)
        # self.logger.debug(f"Received {ipc_sock_commands.PING} response: {msg_resp}")
        return msg_resp

    def stop(self) -> ipc_sock_msg_types.StopResponse:
        # Send
        msg_req = ipc_sock_msg_types.StopRequest()
        # self.logger.debug(f"Sending {ipc_sock_commands.STOP} request: {msg_req}")
        send_msg(self.sock, msg_req.to_cbor())

        # Recv
        try:
            data = self.receive_data()
        except SocketServerError:
            self.logger.info("Server forcefully cancelled all requests")
            return ipc_sock_msg_types.StopResponse()

        cbor_obj = cast(dict[Any, Any], cbor2.loads(data))
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
            cbor_obj = cast(dict[Any, Any], cbor2.loads(data))
            msg_resp = ipc_sock_msg_types.ChainTipResponse(**cbor_obj)
            # self.logger.debug(f"Received {ipc_sock_commands.CHAIN_TIP} response: {msg_resp}")
            return msg_resp.header, msg_resp.height
        except ConnectionResetError:
            self.wait_for_connection()
            return self.chain_tip()  # recurse

    def block_number_batched(
        self, block_hashes: list[bytes]
    ) -> ipc_sock_msg_types.BlockNumberBatchedResponse:
        try:
            # Request
            msg_req = ipc_sock_msg_types.BlockNumberBatchedRequest(block_hashes=block_hashes)
            # self.logger.debug(f"Sending {ipc_sock_commands.BLOCK_NUMBER_BATCHED} request: {msg_req}")
            send_msg(self.sock, msg_req.to_cbor())

            # Recv
            data = self.receive_data()
            cbor_obj = cast(dict[Any, Any], cbor2.loads(data))
            msg_resp = ipc_sock_msg_types.BlockNumberBatchedResponse(**cbor_obj)
            # self.logger.debug(f"Received {ipc_sock_commands.BLOCK_NUMBER_BATCHED} response: {msg_resp}")
            return msg_resp
        except ConnectionResetError:
            self.wait_for_connection()
            return self.block_number_batched(block_hashes)  # recurse

    # Todo - make a streaming API for blocks to protect against freakishly large txs
    def block_batched(self, block_requests: list[BlockSliceRequestType]) -> BatchedBlockSlices:
        """The packing protocol is a contiguous array of:
        block_number uint32,
        len_slice uin64,
        raw_block_slice bytes"""
        try:
            # Request
            msg_req = ipc_sock_msg_types.BlockBatchedRequest(block_requests)
            # self.logger.debug(f"Sending {ipc_sock_commands.BLOCK_BATCHED} request: {msg_req}")
            send_msg(self.sock, msg_req.to_cbor())

            # Recv
            data = self.receive_data()
            # NOTE: No cbor deserialization - this is a hot-path - needs to be fast!
            return data
        except ConnectionResetError:
            self.wait_for_connection()
            return self.block_batched(block_requests)  # recurse

    def merkle_tree_row(self, block_hash: bytes, level: int) -> ipc_sock_msg_types.MerkleTreeRowResponse:
        try:
            msg_req = ipc_sock_msg_types.MerkleTreeRowRequest(block_hash, level)
            send_msg(self.sock, msg_req.to_cbor())

            # Recv
            data = self.receive_data()
            command = ipc_sock_commands.MERKLE_TREE_ROW
            msg_resp = ipc_sock_msg_types.MerkleTreeRowResponse(mtree_row=data, command=command)
            return msg_resp
        except ConnectionResetError:
            self.wait_for_connection()
            return self.merkle_tree_row(block_hash, level)  # recurse

    # typing(AustEcon) - array.ArrayType doesn't let me specify int or bytes
    def transaction_offsets_batched(self, block_hashes: list[bytes]) -> Iterator["array.ArrayType[int]"]:
        try:
            msg_req = ipc_sock_msg_types.TransactionOffsetsBatchedRequest(block_hashes)
            send_msg(self.sock, msg_req.to_cbor())

            data = self.receive_data()
            cbor_obj = cast(dict[Any, Any], cbor2.loads(data))
            msg_resp = ipc_sock_msg_types.TransactionOffsetsBatchedResponse(**cbor_obj)
            for tx_offsets_array in msg_resp.tx_offsets_batch:
                yield array.array("Q", tx_offsets_array)
        except ConnectionResetError:
            self.wait_for_connection()
            return self.transaction_offsets_batched(block_hashes)  # recurse

    def block_metadata_batched(self, block_hashes: list[bytes]) -> BlockMetadataBatchedResponse:
        try:
            # Request
            msg_req = ipc_sock_msg_types.BlockMetadataBatchedRequest(block_hashes)
            # self.logger.debug(f"Sending {ipc_sock_commands.BLOCK_METADATA_BATCHED} request: {msg_req}")
            send_msg(self.sock, msg_req.to_cbor())

            # Recv
            data = self.receive_data()
            cbor_obj = cast(dict[Any, Any], cbor2.loads(data))
            msg_resp = ipc_sock_msg_types.BlockMetadataBatchedResponse(**cbor_obj)
            # self.logger.debug(f"Received {ipc_sock_commands.BLOCK_METADATA_BATCHED} response: {msg_resp}")
            return msg_resp
        except ConnectionResetError:
            self.wait_for_connection()
            return self.block_metadata_batched(block_hashes)  # recurse

    def headers_batched(
        self, start_height: int, batch_size: int
    ) -> ipc_sock_msg_types.HeadersBatchedResponse:
        try:
            # Request
            msg_req = ipc_sock_msg_types.HeadersBatchedRequest(start_height, batch_size)
            # self.logger.debug(f"Sending {ipc_sock_commands.HEADERS_BATCHED} request: {msg_req}")
            send_msg(self.sock, msg_req.to_cbor())

            # Recv
            data = self.receive_data()
            cbor_obj = cast(dict[Any, Any], cbor2.loads(data))
            msg_resp = ipc_sock_msg_types.HeadersBatchedResponse(**cbor_obj)
            # self.logger.debug(f"Received {ipc_sock_commands.HEADERS_BATCHED} response: {msg_resp}")
            if not msg_resp.headers_batch:
                raise MissingHeader
            return msg_resp
        except ConnectionResetError:
            self.wait_for_connection()
            return self.headers_batched(start_height, batch_size)  # recurse

    def reorg_differential(
        self, old_hashes: ChainHashes, new_hashes: ChainHashes
    ) -> ipc_sock_msg_types.ReorgDifferentialResponse:
        try:
            # Request
            msg_req = ipc_sock_msg_types.ReorgDifferentialRequest(old_hashes, new_hashes)
            # self.logger.debug(f"Sending {ipc_sock_commands.REORG_DIFFERENTIAL} request: {msg_req}")
            send_msg(self.sock, msg_req.to_cbor())

            # Recv
            data = self.receive_data()
            cbor_obj = cast(dict[Any, Any], cbor2.loads(data))
            msg_resp = ipc_sock_msg_types.ReorgDifferentialResponse(**cbor_obj)
            self.logger.debug(f"Received {ipc_sock_commands.REORG_DIFFERENTIAL} response: {msg_resp}")
            return msg_resp
        except ConnectionResetError:
            self.wait_for_connection()
            return self.reorg_differential(old_hashes, new_hashes)  # recurse

    def delete_blocks_when_safe(
        self, header_rows: list[BlockHeaderRow], tip_hash: bytes
    ) -> ipc_sock_msg_types.DeleteBlocksResponse:
        try:
            # Request
            msg_req = ipc_sock_msg_types.DeleteBlocksRequest(header_rows, tip_hash)
            # self.logger.debug(f"Sending {ipc_sock_commands.DELETE_BLOCKS} request: {msg_req}")
            send_msg(self.sock, msg_req.to_cbor())

            # Recv
            data = self.receive_data()
            cbor_obj = cast(dict[Any, Any], cbor2.loads(data))
            msg_resp = ipc_sock_msg_types.DeleteBlocksResponse(**cbor_obj)
            # self.logger.debug(f"Received {ipc_sock_commands.DELETE_BLOCKS} response: {msg_resp}")
            return msg_resp
        except ConnectionResetError:
            self.wait_for_connection()
            return self.delete_blocks_when_safe(header_rows, tip_hash)  # recurse
