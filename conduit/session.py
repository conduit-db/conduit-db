import io
import json
import math
from asyncio import BufferedProtocol
from collections import namedtuple
from typing import Optional, List, Dict

import bitcoinx

from commands import VERSION
from handlers import Handlers
import logging
import struct
import asyncio

from constants import LOGGING_FORMAT, HEADER_LENGTH
from deserializer import Deserializer
from networks import NetworkConfig
from peers import Peer
from serializer import Serializer
from utils import is_block_msg

Header = namedtuple("Header", "magic command payload_size checksum")

logging.basicConfig(
    format=LOGGING_FORMAT, level=logging.DEBUG, datefmt="%Y-%m-%d %H-%M-%S"
)


class BufferedSession(BufferedProtocol):
    """Designed to sync the blockchain as fast as possible. Core features include:
        - zero-copy recv via asyncio.BufferedProtocols - can achieve 500-600MB/sec for bulk data
        - multiprocessing.shared_memory so that any number of processes can access a memory view
         of the full (or partially read) block at a time
        - """

    WORKER_COUNT = 1

    def __init__(self, config: NetworkConfig, peer: Peer, host, port):
        self.logger = logging.getLogger("session")
        self.loop = asyncio.get_event_loop()

        # Framer
        self.buffer: Optional[bytearray] = None
        self.buffer_view: Optional[memoryview] = None  # will change to shared_memory
        self.pos: int = 0

        # Bitcoin network/peer config
        self.config = config
        self.peer = peer
        self.host = host
        self.port = port
        self.protocol: BufferedProtocol = None
        self.protocol_factory = None
        self.con_lost_event = (
            asyncio.Event()
        )  # waited on by session manager at higher LOA

        # Serialization/Deserialization
        self.handlers = Handlers(self, self.config)
        self.serializer = Serializer(self.config)
        self.deserializer = Deserializer(self.config)

        # Block processing events (in sequential order) - for multiprocessing
        self._block_read_event = asyncio.Event()
        self._block_parsed_event = asyncio.Event()
        self._merkle_tree_done_event = asyncio.Event()
        self._block_committed_event = asyncio.Event()
        self._new_tip_event = asyncio.Event()  # buffer reset

        # managing processes
        self.partition_size: Optional[int] = None

    def connection_made(self, transport):
        self.transport = transport
        self._payload_size = None
        self._new_buffer(HEADER_LENGTH)  # bitcoin message header size
        self.logger.info("connection made")

    def connection_lost(self, exc):
        self.con_lost_event.set()
        self.logger.info("The server closed the connection:", exc)

    def _new_buffer(self, size):
        self.buffer = bytearray(
            size
        )  # not sure this is necessary. can maybe re-use if pos correct
        self.buffer_view = memoryview(
            self.buffer
        )  # can swap to shared_memory for multi-cpu reads
        self.pos = 0

    def reset_buffer(self):
        self._new_buffer(HEADER_LENGTH)  # header size
        self._payload_size = None

    def get_buffer(self, sizehint):
        return self.buffer_view[self.pos :]

    def _unpack_msg_header(self) -> Header:
        return Header(*struct.unpack_from("<4s12sI4s", self.buffer_view, offset=0))

    def buffer_updated(self, nbytes):
        self.pos += nbytes

        # get msg header
        if self._payload_size is None:
            if self.pos == HEADER_LENGTH:
                self.msg_header = self._unpack_msg_header()
                self._payload_size = self.msg_header.payload_size
                if self._payload_size == 0:  # e.g. ping/version etc
                    self.reset_buffer()
                else:
                    self._new_buffer(self._payload_size)

        # get payload
        else:
            current_command = self.msg_header.command.rstrip(b"\0")

            if is_block_msg(
                current_command
            ):  # start work early on memory view of the block
                self.allocate_work(
                    self.buffer_view, self.pos, self._payload_size, current_command
                )

            # full payload in buffer
            if self.pos == self._payload_size:
                message = self.buffer
                self.message_received(current_command, message)
                self.reset_buffer()

    def run_coro_threadsafe(self, coro, *args, **kwargs):
        asyncio.run_coroutine_threadsafe(coro(*args, **kwargs), self.loop)

    def message_received(self, command: bytes, message: bytes):
        self.logger.debug("client received message type: %s", command.decode("ascii"))
        self.run_coro_threadsafe(self.handle, command, message)

    async def handle(self, command, message=None):
        handler_func_name = "on_" + command.decode("ascii")
        handler_func = getattr(self.handlers, handler_func_name)
        await handler_func(message)

    def send_message(self, message):
        self.transport.write(message)

    async def send_request(self, command_name: str, message: bytes):
        self.logger.debug(f"Sending: {command_name} message to {self.peer}")
        self.send_message(message)

    def send_request_nowait(self, command_name, message):
        self.outbound_queue.put_nowait((command_name, message))

    # -- Message Types -- #
    async def send_version(
        self, recv_host=None, recv_port=None, send_host=None, send_port=None
    ):
        message = self.serializer.version(
            recv_host=recv_host,
            recv_port=recv_port,
            send_host=send_host,
            send_port=send_port,
        )
        await self.send_request(VERSION, message)

    async def send_inv(self, inv_vects: List[Dict]):
        await self.serializer.inv(inv_vects)

    def allocate_work(
        self, buffer_view: memoryview, pos: int, payload_size: int, current_command: str
    ):
        self.logger.debug(
            f"received message type: {current_command}, "
            f"still buffering & processing simultaneously..."
        )

        # Todo - defer multiprocessing component - just wait until whole block is in buffer
        # if the partition size is 50MB and pos = 70MB then only 1 worker can begin parsing
        self.partition_size = math.floor(payload_size / self.WORKER_COUNT)
        ready_partitions = math.floor(self.pos / self.partition_size)
        if ready_partitions > 0:
            from_worker_id, to_worker_id = 1, ready_partitions

        # buffer_updated won't be called again until we tell it to via self.reset_buffer()
        if self.pos == self._payload_size:
            self.logger.debug("block_read_event -> set")
            self._block_read_event.set()
            # for MVP just parse the entire block after it's all in buffer on the event loop!
            self.parse_block_partition(buffer_view, pos, payload_size, current_command)
        pass

    def parse_block_partition(
        self, buffer_view: memoryview, pos: int, payload_size: int, current_command: str
    ):
        def parse_block():
            raw_block_header = bitcoinx.unpack_header(buffer_view[0:80])
            self.logger.debug("raw_block_header = %s", raw_block_header)
            tx_count, offset = bitcoinx.read_varint(buffer_view[80:])
            stream: io.BytesIO = io.BytesIO.read(buffer_view[80 + offset :])
            txs = []
            stream.seek(0)
            for i in range(tx_count):
                txs.append(bitcoinx.Tx.read(stream.read))
            return txs

        txs = parse_block()
        with open("block1.json", "w") as f:
            f.write(json.dumps(txs))
