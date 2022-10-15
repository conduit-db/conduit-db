from __future__ import annotations

import logging
import os
import threading
import socket
import struct
import time
from pathlib import Path

import bitcoinx
import cbor2
import socketserver
from typing import Any, Callable, cast, Type

from bitcoinx import hash_to_hex_str

from conduit_lib.constants import REGTEST
from conduit_lib.database.lmdb.lmdb_database import LMDB_Database
from conduit_lib.ipc_sock_msg_types import REQUEST_MAP, BaseMsg
from conduit_lib import ipc_sock_msg_types, ipc_sock_commands
from conduit_lib.types import BlockMetadata, Slice

struct_be_Q = struct.Struct(">Q")
logger = logging.getLogger('rs-server')


HandlerType = Callable[[BaseMsg], None]


class ThreadedTCPRequestHandler(socketserver.BaseRequestHandler):
    """This uses a very simple messaging protocol
        - Every message is prefixed with an uint64 length of the message.
        - Any binary message matching this length can follow.
        - CBOR is used for most messages except where performance is critical (e.g. raw blocks).

    There is a very strict 1:1 relationship between the handler name and the rs_msg_types for
    the request and response.

    The naming convention is:
        - handler_name -> HandlerNameRequest / HandlerNameResponse

    This provides type safety without bringing in a heavy-weight dependency like protobufs.
    """

    server: ThreadedTCPServer
    request: socket.socket

    def recvall(self, n: int) -> bytearray | None:
        try:
            # Helper function to recv n bytes or return None if EOF is hit
            data = bytearray()
            while len(data) < n:
                packet = self.request.recv(n - len(data))
                if not packet:
                    return None
                data.extend(packet)
            return data
        except (ConnectionResetError, OSError):
            # This path happens when the remote connection is disconnected or disconnects.
            # > OSError: [WinError 10038] An operation was attempted on something that is not a
            # >   socket
            return None
        except Exception:
            logger.exception("Exception in ThreadedTCPRequestHandler.recvall")
            return None

    def recv_msg(self) -> bytearray | None:
        try:
            # Read message length and unpack it into an integer
            raw_msglen = self.recvall(8)
            if not raw_msglen:
                return None
            msglen = struct_be_Q.unpack(raw_msglen)[0]
            # Read the message data
            return self.recvall(msglen)
        except Exception:
            logger.exception("Exception in ThreadedTCPRequestHandler.recv_msg")
            return None

    def send_msg(self, msg: bytes) -> bool | None:
        try:
            # Prefix each message with a 8-byte length (network byte order)
            msg = struct_be_Q.pack(len(msg)) + msg
            self.request.sendall(msg)
            return True
        except (OSError, ConnectionAbortedError):
            # This path happens when the remote connection is disconnected or disconnects.
            # > OSError: [WinError 10038] An operation was attempted on something that is not a
            #     socket
            # > ConnectionAbortedError: [WinError 10053] An established connection was aborted by
            # >   the software in your host machine"
            return None
        except Exception:
            logger.exception("Exception in ThreadedTCPRequestHandler.send_msg")
            return None

    def handle(self) -> None:
        command: str | None = None
        try:
            # This handles a long-lived connection
            while True:
                data = self.recv_msg()
                if not data:
                    # logger.debug(f"Client: {self.request.getpeername()} closed connection")
                    return

                msg: dict[str, Any] = cbor2.loads(data)
                command = cast(str, msg['command'])
                # logger.debug(f"Socket server: command {command} received")

                request_type = REQUEST_MAP[command]
                handler: HandlerType = getattr(self, command)
                handler(request_type(**msg))
        except ConnectionResetError as e:
            logger.error(f"Connection was forcibly closed by the remote host ({self.request.getpeername()})")
        except Exception:
            if command:
                logger.exception(f"Exception in ThreadedTCPRequestHandler.handle (command={command})")
            else:
                logger.exception(
                    f"Exception in ThreadedTCPRequestHandler.handle")

    def ping(self, msg_req: ipc_sock_msg_types.PingRequest) -> None:
        msg_resp = ipc_sock_msg_types.PingResponse()
        self.send_msg(msg_resp.to_cbor())

    def stop(self, msg_req: ipc_sock_msg_types.StopRequest) -> None:
        logger.debug(f"Got {ipc_sock_commands.STOP} request: {msg_req}")
        self.server.lmdb.close()
        self.server.shutdown()

        msg_resp = ipc_sock_msg_types.StopResponse()
        logger.debug(f"Sending {ipc_sock_commands.STOP} response: {msg_resp}")
        self.send_msg(msg_resp.to_cbor())

    def chain_tip(self, msg_req: ipc_sock_msg_types.ChainTipRequest) -> None:
        try:
            with self.server.block_headers_lock:
                tip: bitcoinx.Header = self.server.block_headers.longest_chain().tip
                msg_resp = ipc_sock_msg_types.ChainTipResponse(header=tip.raw, height=tip.height)
                self.send_msg(msg_resp.to_cbor())
        except Exception:
            logger.exception("Exception in ThreadedTCPRequestHandler.chain_tip")

    def block_number_batched(self, msg_req: ipc_sock_msg_types.BlockNumberBatchedRequest) -> None:
        try:
            block_numbers = []
            for block_hash in msg_req.block_hashes:
                block_number = self.server.lmdb.get_block_num(block_hash)
                # NOTE(AustEcon): should the client handle errors / null results?
                block_numbers.append(cast(int, block_number))

            msg_resp = ipc_sock_msg_types.BlockNumberBatchedResponse(block_numbers=block_numbers)
            self.send_msg(msg_resp.to_cbor())
        except Exception:
            logger.exception("Exception in ThreadedTCPRequestHandler.block_number_batched")

    def block_batched(self, msg_req: ipc_sock_msg_types.BlockBatchedRequest) -> None:
        try:
            raw_blocks_array = bytearray()
            for block_request in msg_req.block_requests:
                block_number, (start_offset, end_offset) = block_request
                raw_block_slice = self.server.lmdb.get_block(block_number,
                    Slice(start_offset, end_offset))

                # NOTE(AustEcon): should the client handle errors / null results?
                len_slice = len(cast(bytes, raw_block_slice))
                raw_blocks_array += struct.pack(f"<IQ{len_slice}s",
                    block_number, len_slice, raw_block_slice)

            # logger.debug(f"Sending block_batched response: len(raw_blocks_array):
            # {len(raw_blocks_array)}")

            # NOTE: No cbor serialization - this is a hot-path - needs to be fast!
            if raw_blocks_array:
                self.send_msg(raw_blocks_array)
            else:
                self.send_msg(b"")
        except Exception:
            logger.exception("Exception in ThreadedTCPRequestHandler.block_batched")

    def merkle_tree_row(self, msg_req: ipc_sock_msg_types.MerkleTreeRowRequest) -> None:
        try:
            mtree_row = self.server.lmdb.get_mtree_row(msg_req.block_hash, msg_req.level)
            # NOTE: No cbor serialization - this is a hot-path - needs to be fast!
            if mtree_row:
                self.send_msg(mtree_row)
            else:
                logger.debug(
                    f"Transaction offsets not found for block_hash: "
                    f"{hash_to_hex_str(msg_req.block_hash)}; level: {msg_req.level}")
                self.send_msg(b"")
        except Exception:
            logger.exception("Exception in ThreadedTCPRequestHandler.merkle_tree_row")

    def transaction_offsets_batched(self,
            msg_req: ipc_sock_msg_types.TransactionOffsetsBatchedRequest) -> None:
        try:
            tx_offsets_batch = []
            for block_hash in msg_req.block_hashes:
                # NOTE(AustEcon): should the client handle errors / null results?
                tx_offsets_binary = cast(bytes, self.server.lmdb.get_tx_offsets(block_hash))
                tx_offsets_batch.append(tx_offsets_binary)

            msg_resp = ipc_sock_msg_types.TransactionOffsetsBatchedResponse(tx_offsets_batch=tx_offsets_batch)
            self.send_msg(msg_resp.to_cbor())
        except Exception:
            logger.exception("Exception in ThreadedTCPRequestHandler.transaction_offsets_batched")

    def block_metadata_batched(self, msg_req: ipc_sock_msg_types.BlockMetadataBatchedRequest) \
            -> None:
        try:
            block_metadata_batch = []
            for block_hash in msg_req.block_hashes:
                # NOTE(AustEcon): should the client handle errors / null results?
                block_metadata = cast(BlockMetadata,
                    self.server.lmdb.get_block_metadata(block_hash))
                block_metadata_batch.append(block_metadata)

                assert block_metadata is not None, "block_metadata is None"
            msg_resp = ipc_sock_msg_types.BlockMetadataBatchedResponse(
                block_metadata_batch=block_metadata_batch)
            self.send_msg(msg_resp.to_cbor())
        except Exception:
            logger.exception("Exception in ThreadedTCPRequestHandler.block_metadata_batched")

    def _get_header_for_height(self, height: int) -> bitcoinx.Header:
        with self.server.block_headers_lock:
            headers = self.server.block_headers
            chain = self.server.block_headers.longest_chain()
            header = headers.header_at_height(chain, height)
            return header

    def headers_batched(self, msg_req: ipc_sock_msg_types.HeadersBatchedRequest) -> None:
        # Todo - this should not be doing a while True / sleep. It should be waiting on
        #  a queue for new tip notifications
        try:
            start_height = msg_req.start_height
            batch_size = msg_req.batch_size
            while self.server.is_running:
                headers_batch = []
                for height in range(start_height, start_height + batch_size):
                    try:
                        header = self._get_header_for_height(height)
                    except bitcoinx.MissingHeader:
                        # logger.error(f"Missing header at height: {height}")
                        break
                    headers_batch.append(header.raw)

                if headers_batch:
                    break
                else:
                    time.sleep(0.2)  # Allows client to long-poll for latest tip
                    continue

            msg_resp = ipc_sock_msg_types.HeadersBatchedResponse(headers_batch=headers_batch)
            self.send_msg(msg_resp.to_cbor())
        except Exception:
            logger.exception("Exception in ThreadedTCPRequestHandler.headers_batched")

    def headers_batched2(self, msg_req: ipc_sock_msg_types.HeadersBatchedRequest) -> bool:
        start_height = msg_req.start_height
        desired_end_height = msg_req.start_height + (msg_req.batch_size - 1)
        with self.server.block_headers_lock:
            headers = self.server.block_headers
            chain = self.server.block_headers.longest_chain()
            end_height = min(chain.tip.height, desired_end_height)
            header_count = max(0, end_height - start_height + 1)

            # Send the number of headers that will follow.
            header_count_bytes = struct_be_Q.pack(header_count)
            self.request.sendall(header_count_bytes)

            for height in range(start_height, end_height+1):
                header = headers.header_at_height(chain, height)
                self.request.sendall(header.raw)
        return True

    def reorg_differential(self, msg_req: ipc_sock_msg_types.ReorgDifferentialRequest) -> None:
        try:
            removals_from_mempool, additions_to_mempool, orphaned_tx_hashes = \
                self.server.lmdb.get_reorg_differential(msg_req.old_hashes, msg_req.new_hashes)
            msg_resp = ipc_sock_msg_types.ReorgDifferentialResponse(removals_from_mempool,
                additions_to_mempool, orphaned_tx_hashes)
            self.send_msg(msg_resp.to_cbor())
        except Exception:
            logger.exception("Exception in ThreadedTCPRequestHandler.reorg_differential")


class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    is_running = True

    def __init__(self, addr: tuple[str, int], handler: Type[ThreadedTCPRequestHandler],
            storage_path: Path, block_headers: bitcoinx.Headers,
            block_headers_lock: threading.RLock) -> None:
        super(ThreadedTCPServer, self).__init__(addr, handler)

        logger.debug(f"ThreadedTCPServer LMDB_Database storage_path={storage_path}")
        self.lmdb = LMDB_Database(storage_path=str(storage_path))
        self.block_headers = block_headers
        self.block_headers_lock = block_headers_lock

        self._active_request_sockets = set[tuple[bytes, socket.socket] | socket.socket]()

    def finish_request(self, request_socket: tuple[bytes, socket.socket] | socket.socket,
            client_address: tuple[str, int] | str) -> None:
        # There are two types of open connection. The listen socket and the connected client
        # sockets. Closing the listen socket does not close the connected client sockets, and those
        # threads can hang indefinitely. We keep track of the active client sockets and manually
        # close them when we shut down the server.
        self._active_request_sockets.add(request_socket)
        super().finish_request(request_socket, client_address)
        self._active_request_sockets.remove(request_socket)

    def shutdown(self) -> None:
        self.is_running = False
        super().shutdown()
        for socket in list(self._active_request_sockets):
            self.shutdown_request(socket)


if __name__ == "__main__":
    os.environ["LMDB_DATABASE_DIR"] = "test_lmdb"
    os.environ["RAW_BLOCKS_DIR"] = "test_rawblocks"
    os.environ["RAW_BLOCKS_LOCKFILE"] = "test_rawblocks.lock"

    os.environ["MERKLE_TREES_DIR"] = "test_merkle_trees"
    os.environ["MERKLE_TREES_LOCKFILE"] = "test_merkle_trees.lock"

    os.environ["TX_OFFSETS_DIR"] = "test_tx_offsets"
    os.environ["TX_OFFSETS_LOCKFILE"] = "test_tx_offsets.lock"

    logging.basicConfig(level=logging.DEBUG)
    # Port 0 means to select an arbitrary unused port
    HOST, PORT = "127.0.0.1", 50000

    from conduit_lib.store import setup_headers_store
    from conduit_lib.networks import NetworkConfig

    MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
    storage_path = MODULE_DIR / "test_lmdb"
    net_config = NetworkConfig(network_type=REGTEST, node_host='127.0.0.1', node_port=18444)
    block_headers = setup_headers_store(net_config, "test_headers.mmap")
    block_headers_lock = threading.RLock()

    server = ThreadedTCPServer((HOST, PORT), ThreadedTCPRequestHandler, storage_path, block_headers,
        block_headers_lock)
    with server:
        ip, port = server.server_address
        server.serve_forever()
