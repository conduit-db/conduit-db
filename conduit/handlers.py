import asyncio
import io
import logging
import multiprocessing
from typing import Tuple, List

import bitcoinx
from bitcoinx import double_sha256, hex_str_to_hash

from .constants import MsgType
from .commands import VERACK, GETDATA, PING, SENDCMPCT, PONG
from .deserializer import Deserializer
from .networks import NetworkConfig
from .serializer import Serializer
from .store import Storage

logger = logging.getLogger("handlers")


class Handlers:
    def __init__(
        self, session: "BufferedSession", net_config: NetworkConfig, storage: Storage
    ):
        self.net_config = net_config
        self.session = session
        self.serializer = Serializer(self.net_config, storage)
        self.deserializer = Deserializer(self.net_config, storage)
        self.storage = storage

    async def on_version(self, message):
        logger.debug("handling version...")
        version = self.session.deserializer.version(io.BytesIO(message))
        self.session.set_target_header_height(version["start_height"])
        logger.debug("received version message: %s", version)
        verack_message = self.session.serializer.verack()
        await self.session.send_request(VERACK, verack_message)

    async def on_verack(self, message):
        logger.debug("handling verack...")
        logger.debug("handshake complete")

    async def on_protoconf(self, message):
        logger.debug("handling protoconf...")
        protoconf = self.session.deserializer.protoconf(io.BytesIO(message))
        logger.debug(f"protoconf: {protoconf}")
        self.session.handshake_complete_event.set()

    async def on_sendheaders(self, message):
        logger.debug("handling sendheaders...")

    async def on_sendcmpct(self, message):
        message = message.tobytes()
        logger.debug("handling sendcmpct...")
        logger.debug("received sendcmpct message: %s", message)
        sendcmpct = self.session.serializer.sendcmpct()
        logger.debug("responding with message: %s", sendcmpct)
        await self.session.send_request(SENDCMPCT, sendcmpct)

    async def on_ping(self, message):
        logger.debug("handling ping...")
        pong_message = self.serializer.pong(message)
        await self.session.send_request(PONG, pong_message)

    async def on_addr(self, message):
        logger.debug("handling addr...")

    async def on_feefilter(self, message):
        logger.debug("handling feefilter...")

    async def on_inv(self, message):
        """Todo: Optimization
        This could be optimized by staying in bytes. No need to convert the inv_type
        to int to categorise it - rather just match to corresponding byte string.
        payload_len/36 == count and the index in bytearray of each hash is just every 36th byte
        with an offset of 4."""
        inv_vects = self.session.deserializer.inv(io.BytesIO(message))

        def have_header(inv_vect) -> bool:
            try:
                self.storage.headers.lookup(hex_str_to_hash(inv_vect['inv_hash']))
                return True
            except bitcoinx.MissingHeader:
                return False

        tx_inv_vect = []
        for inv in inv_vects:
            if inv["inv_type"] == 1:  # TX
                tx_inv_vect.append(inv)
            elif inv["inv_type"] == 2:  # BLOCK
                if not have_header(inv):
                    self.session._headers_event_new_tip.set()

                if hex_str_to_hash(inv["inv_hash"]) in self.session._pending_blocks_batch_set:
                    self.session._pending_blocks_inv_queue.put_nowait(inv)
                else:
                    logger.debug(f"got an unsolicited block {inv['inv_hash']}")

        if self.session._initial_block_download_event.is_set():
            getdata_msg = self.serializer.getdata(tx_inv_vect)
            await self.session.send_request(GETDATA, getdata_msg)

    async def on_getdata(self, message):
        logger.debug("handling getdata...")

    async def on_headers(self, message):
        # logger.debug("handling headers...")
        if self.deserializer.connect_headers(io.BytesIO(message)):
            self.session._headers_msg_processed_event.set()

    # ----- Special case messages ----- #

    async def on_tx(self, special_message: List[Tuple[int, int]]):
        msg_type = MsgType.MSG_TX
        self.session.worker_in_queue_tx_parse.put((msg_type, special_message))
        self.session._msg_handled_count += 1


    async def on_block(self, special_message: Tuple[int, int, bytes, int]):
        blk_start_pos, blk_end_pos, raw_block_header, tx_count = special_message
        blk_hash = double_sha256(raw_block_header)
        blk_height = self.storage.headers.lookup(blk_hash)[0].height
        self.session.add_pending_block(blk_hash, tx_count)

        self.session.worker_in_queue_preproc.put(
            (blk_hash, blk_height, blk_start_pos, blk_end_pos)
        )
        self.session.worker_in_queue_blk_writer.put(
            (blk_hash, blk_start_pos, blk_end_pos)
        )
