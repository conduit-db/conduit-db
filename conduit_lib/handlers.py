import io
import typing
import logging
import struct
from typing import Tuple

import bitcoinx
from bitcoinx import double_sha256, hex_str_to_hash, hash_to_hex_str

from .constants import MsgType
from .commands import VERACK, GETDATA, SENDCMPCT, PONG
from .deserializer import Deserializer
from .networks import NetworkConfig
from .serializer import Serializer
from .store import Storage

logger = logging.getLogger("handlers")


if typing.TYPE_CHECKING:
    from conduit_raw.conduit_raw.controller import Controller

class Handlers:
    def __init__(
        self, controller: 'Controller', net_config: NetworkConfig, storage: Storage
    ):
        self.net_config = net_config
        self.controller = controller
        self.serializer = Serializer(self.net_config, storage)
        self.deserializer = Deserializer(self.net_config, storage)
        self.storage = storage

    async def on_version(self, message):
        # logger.debug("handling version...")
        version = self.controller.deserializer.version(io.BytesIO(message))
        self.controller.sync_state.set_target_header_height(version["start_height"])
        logger.debug("received version message: %s", version)
        verack_message = self.controller.serializer.verack()
        await self.controller.send_request(VERACK, verack_message)

    async def on_verack(self, message):
        # logger.debug("handling verack...")
        logger.debug("handshake complete")
        self.controller.handshake_complete_event.set()

    async def on_protoconf(self, message):
        # logger.debug("handling protoconf...")
        protoconf = self.controller.deserializer.protoconf(io.BytesIO(message))
        # logger.debug(f"protoconf: {protoconf}")
        # self.controller.handshake_complete_event.set()

    async def on_sendheaders(self, message):
        # logger.debug("handling sendheaders...")
        pass

    async def on_sendcmpct(self, message):
        message = message.tobytes()
        # logger.debug("handling sendcmpct...")
        # logger.debug("received sendcmpct message: %s", message)
        sendcmpct = self.controller.serializer.sendcmpct()
        # logger.debug("responding with message: %s", sendcmpct)
        await self.controller.send_request(SENDCMPCT, sendcmpct)

    async def on_ping(self, message):
        # logger.debug("handling ping...")
        pong_message = self.serializer.pong(message)
        await self.controller.send_request(PONG, pong_message)

    async def on_addr(self, message):
        # logger.debug("handling addr...")
        pass

    async def on_feefilter(self, message):
        # logger.debug("handling feefilter...")
        pass

    async def on_inv(self, message):
        """Todo: Optimization
        This could be optimized by staying in bytes. No need to convert the inv_type
        to int to categorise it - rather just match to corresponding byte string.
        payload_len/36 == count and the index in bytearray of each hash is just every 36th byte
        with an block_offset of 4."""
        inv_vects = self.controller.deserializer.inv(io.BytesIO(message))

        def have_header(inv_vect) -> bool:
            try:
                self.storage.headers.lookup(hex_str_to_hash(inv_vect['inv_hash']))
                return True
            except bitcoinx.MissingHeader:
                return False

        tx_inv_vect = []
        for inv in inv_vects:
            if inv["inv_type"] == 1:  # TX
                # logger.debug(f"got inv: {inv}")
                tx_inv_vect.append(inv)
            elif inv["inv_type"] == 2:  # BLOCK
                if not have_header(inv):
                    self.controller.sync_state.headers_event_new_tip.set()

                if hex_str_to_hash(inv["inv_hash"]) in \
                        self.controller.sync_state.global_blocks_batch_set:
                    self.controller.sync_state.pending_blocks_inv_queue.put_nowait(inv)
                # else:
                    # logger.debug(f"got new block notification: {inv['inv_hash']}")

        if self.controller.sync_state.initial_block_download_event.is_set():
            getdata_msg = self.serializer.getdata(tx_inv_vect)
            await self.controller.send_request(GETDATA, getdata_msg)

    async def on_getdata(self, message):
        logger.debug("handling getdata...")

    async def on_headers(self, message):
        # logger.debug("handling headers...")
        if self.deserializer.connect_headers(io.BytesIO(message)):
            self.controller.sync_state.headers_msg_processed_event.set()

    # ----- Special case messages ----- #  # Todo - should probably be registered callbacks

    async def on_tx(self, rawtx: memoryview):
        size_tx = len(rawtx)
        packed_message = struct.pack(f"<II{size_tx}s", MsgType.MSG_TX, size_tx, rawtx.tobytes())
        if hasattr(self.controller, 'mempool_tx_producer'):
            # Todo - expensive debug logging here (remove when fixed):
            logger.debug(f"Got mempool tx: {bitcoinx.Tx.from_bytes(rawtx.tobytes()).hash()}")
            self.controller.mempool_tx_producer.produce(topic='mempool-txs', value=packed_message)
            self.controller.sync_state.incr_msg_handled_count()

    async def on_block(self, special_message: Tuple[int, int, bytes, int]):
        blk_start_pos, blk_end_pos, raw_block_header, tx_count = special_message
        blk_hash = double_sha256(raw_block_header)
        self.controller.sync_state.received_blocks.add(blk_hash)
        blk_height = self.storage.headers.lookup(blk_hash)[0].height
        if not blk_hash in self.controller.sync_state.global_blocks_batch_set:
            logger.debug(f"got an unsolicited block: {hash_to_hex_str(blk_hash)}, "
                         f"height={blk_height}. Discarding...")
            return

        # logger.debug(f"on_block: blk_hash={hash_to_hex_str(blk_hash)}; blk_height={blk_height}")

        # ---- PUSH WORK TO WORKERS ---- #
        self.controller.worker_in_queue_preproc.put(
            (blk_hash, blk_height, blk_start_pos, blk_end_pos)
        )
        self.controller.worker_in_queue_blk_writer.put(
            (blk_hash, blk_start_pos, blk_end_pos)
        )

        # ---- SHORT CIRCUIT WORKERS ---- #
        # self.controller.worker_ack_queue_blk_writer.put(blk_hash)
        # self.controller.worker_ack_queue_mtree.put(blk_hash)
        # self.controller.worker_ack_queue_preproc.put(blk_hash)
