import logging
import os
import threading
import time
from os import urandom
from pathlib import Path

import bitcoinx
import cbor2
import struct
import socketserver
from typing import Optional, Dict

from bitcoinx import hash_to_hex_str

from conduit_lib.database.lmdb.lmdb_database import LMDB_Database

try:
    pass
except ImportError:
    import rs_msg_types

from conduit_lib import ipc_sock_msg_types, ipc_sock_commands

struct_be_Q = struct.Struct(">Q")
logger = logging.getLogger('rs-server')


class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):

    def __init__(self, addr: tuple[str, int], handler, storage_path: Path,
            block_headers: bitcoinx.Headers):
        super(ThreadedTCPServer, self).__init__(addr, handler)

        self.lmdb = LMDB_Database(storage_path=str(storage_path))
        self.block_headers = block_headers
        self.block_headers_lock = threading.Lock()


class ThreadedTCPRequestHandler(socketserver.BaseRequestHandler):
    """This uses a very simple messaging protocol - namely every message is prefixed with an 8 byte
    uint64 to indicate the length of the message. Beyond that any binary message matching this
    length can follow. We use CBOR for serializing for convenience and efficiency.

    NOTE: There is a very strict 1:1 relationship between the handler name and the rs_msg_types for
    the request and response. The naming convention is:
        - handler_name -> HandlerNameRequest / HandlerNameResponse

    This provides type safety (and validation if we wanted to) in lieu of something akin to
    protobufs.
    """

    server: ThreadedTCPServer

    def recvall(self, n: int) -> bytearray:
        # Helper function to recv n bytes or return None if EOF is hit
        data = bytearray()
        while len(data) < n:
            packet = self.request.recv(n - len(data))
            if not packet:
                return None
            data.extend(packet)
        return data

    def recv_msg(self) -> Optional[bytearray]:
        # Read message length and unpack it into an integer
        raw_msglen = self.recvall(8)
        if not raw_msglen:
            return None
        msglen = struct_be_Q.unpack(raw_msglen)[0]
        # Read the message data
        return self.recvall(msglen)

    def send_msg(self, msg: bytes) -> bool:
        # Prefix each message with a 8-byte length (network byte order)
        msg = struct_be_Q.pack(len(msg)) + msg
        self.request.sendall(msg)
        return True

    def handle(self):
        try:
            # This handles a long-lived connection
            while True:
                data = self.recv_msg()
                if not data:
                    # logger.debug(f"Client: {self.request.getpeername()} closed connection")
                    return

                msg = cbor2.loads(data)
                command = msg['command']

                handler = getattr(self, command)
                handler(msg)
        except ConnectionResetError as e:
            logger.error(f"Connection was forcibly closed by the remote host ({self.request.getpeername()})")

    def ping(self, msg: Dict):
        # Request
        msg_req = ipc_sock_msg_types.PingRequest(**msg)
        # logger.debug(f"Got {ipc_sock_commands.PING} request: {msg_req}")

        # Response
        msg_resp = ipc_sock_msg_types.PingResponse()
        # logger.debug(f"Sending {ipc_sock_commands.PING} response: {msg_resp}")
        return self.send_msg(msg_resp.to_cbor())

    def stop(self, msg: Dict):
        # Request
        msg_req = ipc_sock_msg_types.StopRequest(**msg)
        # logger.debug(f"Got {ipc_sock_commands.STOP} request: {msg_req}")

        # Response
        msg_resp = ipc_sock_msg_types.StopResponse()
        # logger.debug(f"Sending {ipc_sock_commands.STOP} response: {msg_resp}")
        return self.send_msg(msg_resp.to_cbor())

    def chain_tip(self, msg: Dict):
        # Request
        msg_req = ipc_sock_msg_types.ChainTipRequest(**msg)
        # logger.debug(f"Got {ipc_sock_commands.CHAIN_TIP} request: {msg_req}")

        with self.server.block_headers_lock:
            tip: bitcoinx.Header = self.server.block_headers.longest_chain().tip
            # Response
            msg_resp = ipc_sock_msg_types.ChainTipResponse(header=tip.raw, height=tip.height)
            # logger.debug(f"Sending {ipc_sock_commands.CHAIN_TIP} response: {msg_resp}")
            return self.send_msg(msg_resp.to_cbor())

    def block_number_batched(self, msg: Dict):
        # Request
        msg_req = ipc_sock_msg_types.BlockNumberBatchedRequest(**msg)
        # logger.debug(f"Got {ipc_sock_commands.BLOCK_NUMBER_BATCHED} request: {msg_req}")

        # Response
        block_numbers = []
        for block_hash in msg_req.block_hashes:
            block_number = self.server.lmdb.get_block_num(block_hash)
            block_numbers.append(block_number)

        msg_resp = ipc_sock_msg_types.BlockNumberBatchedResponse(block_numbers=block_numbers)
        # logger.debug(f"Sending {ipc_sock_commands.BLOCK_NUMBER_BATCHED} response: {msg_resp}")
        return self.send_msg(msg_resp.to_cbor())

    def block_batched(self, msg: Dict):
        # Request
        msg_req = ipc_sock_msg_types.BlockBatchedRequest(**msg)
        # logger.debug(f"Got {ipc_sock_commands.BLOCK_BATCHED} request: {msg_req}")

        # Response
        raw_blocks_array = bytearray()
        for block_request in msg_req.block_requests:
            block_number, (start_offset, end_offset) = block_request
            raw_block_slice = self.server.lmdb.get_block(block_number,
                start_offset, end_offset)

            len_slice = len(raw_block_slice)
            raw_blocks_array += struct.pack(f"<IQ{len_slice}s",
                block_number, len_slice, raw_block_slice)

        # len_bytearray = len(raw_blocks_array)
        # payload = struct.pack(f"<{len_bytearray}s", len_bytearray, raw_blocks_array)
        # logger.debug(f"Sending block_batched response: len_bytearray: {len_bytearray}")
        # logger.debug(f"Sending block_batched response: len(payload): {len(payload)}")

        # NOTE: No cbor serialization - this is a hot-path - needs to be fast!
        return self.send_msg(raw_blocks_array)

    def merkle_tree_row(self, msg: Dict):
        # Request
        msg_req = ipc_sock_msg_types.MerkleTreeRowRequest(**msg)
        # logger.debug(f"Got {ipc_sock_commands.MERKLE_TREE_ROW} request: {msg_req}")

        # Response
        mtree_row = self.server.lmdb.get_mtree_row(msg_req.block_hash, msg_req.level)
        # msg_resp = rs_msg_types.MerkleTreeRowResponse(mtree_row=mtree_row)
        # logger.debug(f"Sending {rs_server_commands.MERKLE_TREE_ROW} response: {msg_resp}")

        # NOTE: No cbor serialization - this is a hot-path - needs to be fast!
        if mtree_row:
            return self.send_msg(mtree_row)
        logger.debug(f"Transaction offsets not found for block_hash: {hash_to_hex_str(msg_req.block_hash)}; level: {msg_req.level}")
        self.send_msg(b"")

    def transaction_offsets_batched(self, msg: Dict):
        # Request
        msg_req = ipc_sock_msg_types.TransactionOffsetsBatchedRequest(**msg)
        # logger.debug(f"Got {ipc_sock_commands.TRANSACTION_OFFSETS_BATCHED} request: {msg_req}")

        # Response
        tx_offsets_batch = []
        for block_hash in msg_req.block_hashes:
            tx_offsets_binary = self.server.lmdb.get_tx_offsets(block_hash)
            tx_offsets_batch.append(tx_offsets_binary)

        msg_resp = ipc_sock_msg_types.TransactionOffsetsBatchedResponse(tx_offsets_batch=tx_offsets_batch)
        # logger.debug(f"Sending {ipc_sock_commands.TRANSACTION_OFFSETS_BATCHED} response: {msg_resp}")
        return self.send_msg(msg_resp.to_cbor())

    def block_metadata_batched(self, msg: Dict):
        # Request
        msg_req = ipc_sock_msg_types.BlockMetadataBatchedRequest(**msg)
        # logger.debug(f"Got {ipc_sock_commands.BLOCK_METADATA_BATCHED} request: {msg_req}")

        # Response
        block_sizes_batch = []
        for block_hash in msg_req.block_hashes:
            block_size = self.server.lmdb.get_block_metadata(block_hash)
            block_sizes_batch.append(block_size)
        msg_resp = ipc_sock_msg_types.BlockMetadataBatchedResponse(block_sizes_batch=block_sizes_batch)
        # logger.debug(f"Sending {ipc_sock_commands.BLOCK_METADATA_BATCHED} response: {msg_resp}")
        return self.send_msg(msg_resp.to_cbor())

    def _get_header_for_height(self, height: int) -> bitcoinx.Header:
        with self.server.block_headers_lock:
            headers = self.server.block_headers
            chain = self.server.block_headers.longest_chain()
            header = headers.header_at_height(chain, height)
            return header

    def headers_batched(self, msg: Dict):
        # Request
        msg_req = ipc_sock_msg_types.HeadersBatchedRequest(**msg)
        # logger.debug(f"Got {ipc_sock_commands.HEADERS_BATCHED} request: {msg_req}")

        # Response
        start_height = msg_req.start_height
        batch_size = msg_req.batch_size
        while True:
            headers_batch = []
            for height in range(start_height, start_height + batch_size):
                try:
                    header = self._get_header_for_height(height)
                except bitcoinx.MissingHeader:
                    break
                headers_batch.append(header.raw)

            if headers_batch:
                break
            else:
                time.sleep(0.1)  # Allows client to long-poll for latest tip
                continue

        msg_resp = ipc_sock_msg_types.HeadersBatchedResponse(headers_batch=headers_batch)
        # logger.debug(f"Sending {ipc_sock_commands.HEADERS_BATCHED} response: {msg_resp}")
        return self.send_msg(msg_resp.to_cbor())


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    # Port 0 means to select an arbitrary unused port
    HOST, PORT = "127.0.0.1", 50000

    from conduit_lib.store import setup_headers_store
    from conduit_lib.networks import RegTestNet
    MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
    storage_path = MODULE_DIR / "test_lmdb"
    block_headers = setup_headers_store(RegTestNet(), "test_headers.mmap")

    server = ThreadedTCPServer((HOST, PORT), ThreadedTCPRequestHandler, storage_path, block_headers)
    with server:
        ip, port = server.server_address
        server.serve_forever()
