import asyncio
import io
from asyncio import BufferedProtocol

import bitcoinx
from bitcoinx import read_varint
from collections import namedtuple, Callable
from multiprocessing import shared_memory
import logging
import struct

from .commands import BLOCK_BIN, TX_BIN
from .constants import HEADER_LENGTH

Header = namedtuple("Header", "magic command payload_size checksum")


class BitcoinNetIO(BufferedProtocol):
    """
    A state machine with 'resumed' and 'paused' reading states and corresponding 'resume()'
    and 'pause()' methods. Contains a large contiguous shared memory buffer for multiprocess
    block/tx parsing.

    4 callbacks:
    - on_buffer_full
    - on_msg  (which provides a memoryview over the message in the shared memory buffer)
    - on_connection_made
    - on_connection_lost

    It is the callers responsibility to perform all sanity checks (i.e. process ACK messages from
    workers, flush to disc, handle reorgs etc. etc. before resetting/mutating the buffer via the
    "resume()' method. As far as I can tell, this arrangement is the fastest python has to offer
    and all reads from the socket and worker processes are zero-copy.
    """

    logger = logging.getLogger("bitcoin-framer")
    HIGH_WATER = int(1024 * 1024 * 1024 * 2)
    BUFFER_OVERFLOW_SIZE = 1024 * 1024 * 4
    BUFFER_SIZE = HIGH_WATER + BUFFER_OVERFLOW_SIZE
    shm_buffer = shared_memory.SharedMemory(create=True, size=BUFFER_SIZE)
    shm_buffer_view = shm_buffer.buf
    _pos = 0
    _last_msg_end_pos = 0

    def __init__(self, on_buffer_full: Callable, on_msg: Callable, on_connection_made: Callable,
            on_connection_lost: Callable):

        self.on_buffer_full_callback = on_buffer_full
        self.on_msg_callback = on_msg
        self.on_connection_made_callback = on_connection_made
        self.on_connection_lost_callback = on_connection_lost

    async def connect(self, host: str, port: int):
        loop = asyncio.get_event_loop()
        protocol_factory = lambda: self
        self.transport, self.session = await loop.create_connection(protocol_factory, host, port)
        return self.transport, self.session

    # Two states: 'resumed' & 'paused'
    def resume(self):
        self.logger.debug("resuming reading...")
        self._new_buffer()
        self.transport.resume_reading()

    def pause(self):
        self.logger.debug("pausing reading...")
        self.transport.pause_reading()

    def send_message(self, message):
        self.transport.write(message)

    def connection_made(self, transport):
        self.transport = transport
        self._payload_size = None
        self.logger.info("connection made")
        self.on_connection_made_callback()

    def connection_lost(self, exc):
        self.on_connection_lost_callback()
        self.logger.warning("The server closed the connection")

    # def _resize_buffer(self):
    #     try:
    #         self.logger.debug(f"resizing buffer sized: {self.BUFFER_SIZE}")
    #         self.HIGH_WATER = HEADER_LENGTH + self._payload_size
    #         self.BUFFER_SIZE = self.HIGH_WATER + self.BUFFER_OVERFLOW_SIZE
    #         self.shm_buffer = shared_memory.SharedMemory(create=True, size=self.BUFFER_SIZE)
    #         self.shm_buffer_view = self.shm_buffer.buf
    #         self.logger.debug(f"resized buffer size to: {self.BUFFER_SIZE}")
    #     except Exception:
    #         self.logger.exception("unexpected problem in '_resize_buffer'")

    def _new_buffer(self):
        """takes the remainder of buffer and places it at the start then extends to size"""

        assert self._pos >= self.HIGH_WATER

        remainder = self.shm_buffer_view[self._last_msg_end_pos:].tobytes()

        # # If buffer is not large enough to accommodate this block / message, increase buffer size
        # if HEADER_LENGTH + self._payload_size > self.HIGH_WATER:
        #     self._resize_buffer()

        self.shm_buffer_view[0 : len(remainder)] = remainder

        # new block_offset for most recent, partially read msg
        self._pos = self._pos - self._last_msg_end_pos
        self._last_msg_end_pos = 0

    def get_buffer(self, sizehint):
        return self.shm_buffer_view[self._pos :]

    def _unpack_msg_header(self) -> Header:
        cur_header_view = self.shm_buffer_view[
            self._last_msg_end_pos : self._last_msg_end_pos + HEADER_LENGTH
        ]
        return Header(*struct.unpack_from("<4s12sI4s", cur_header_view, offset=0))

    def is_next_header_available(self) -> bool:
        return self._pos - self._last_msg_end_pos >= HEADER_LENGTH

    def is_next_payload_available(self) -> bool:
        return self._pos - self._last_msg_end_pos >= HEADER_LENGTH + self._payload_size

    def get_block_tx_count(self, end_blk_header_pos):
        # largest varint (tx_count) is 9 bytes
        block_tx_count_view = self.shm_buffer_view[end_blk_header_pos : end_blk_header_pos + 10]
        return read_varint(io.BytesIO(block_tx_count_view).read)

    def make_special_blk_msg(self, cur_msg_start_pos, cur_msg_end_pos):
        end_blk_header_pos = cur_msg_start_pos + 80
        raw_block_header = self.shm_buffer_view[cur_msg_start_pos:end_blk_header_pos].tobytes()
        block_tx_count = self.get_block_tx_count(end_blk_header_pos)
        return (
            cur_msg_start_pos,
            cur_msg_end_pos,
            raw_block_header,
            block_tx_count,
        )

    def make_special_tx_msg(self, rawtx: memoryview):
        """returns a list in preparation for future batching of continuous mempool rawtxs"""
        return rawtx

    def buffer_updated(self, nbytes):

        self._pos += nbytes

        while self.is_next_header_available():
            cur_header = self._unpack_msg_header()
            self._payload_size = cur_header.payload_size
            if self.is_next_payload_available():
                cur_msg_end_pos = self._last_msg_end_pos + HEADER_LENGTH + self._payload_size
                cur_msg_start_pos = self._last_msg_end_pos + HEADER_LENGTH

                # Block messages
                if cur_header.command == BLOCK_BIN:
                    self.on_msg_callback(
                        cur_header.command,
                        self.make_special_blk_msg(cur_msg_start_pos, cur_msg_end_pos),
                    )

                # Tx messages (via mempool relay)
                elif cur_header.command == TX_BIN:
                    rawtx = self.shm_buffer_view[cur_msg_start_pos:cur_msg_end_pos]

                    self.on_msg_callback(
                        cur_header.command,
                        self.make_special_tx_msg(rawtx),
                    )

                # Non-block messages
                else:
                    sub_view_payload = self.shm_buffer_view[cur_msg_start_pos:cur_msg_end_pos]
                    self.on_msg_callback(cur_header.command, sub_view_payload)
                self._last_msg_end_pos = cur_msg_end_pos
            else:
                break  # recv more data until next full payload available

        if self._pos >= self.HIGH_WATER:
            self.logger.debug("buffer reached high-water mark, pausing reading...")
            self.pause()
            self.on_buffer_full_callback()