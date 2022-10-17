import io
import os
import typing
import logging
import struct

import bitcoinx
from bitcoinx import double_sha256, hex_str_to_hash, hash_to_hex_str

from .constants import MsgType
from .commands import VERACK, GETDATA, SENDCMPCT, PONG
from .deserializer import Deserializer
from .deserializer_types import Inv
from .networks import NetworkConfig
from .serializer import Serializer
from .store import Storage
from .utils import connect_headers_reorg_safe

logger = logging.getLogger("handlers")


if typing.TYPE_CHECKING:
    from conduit_raw.conduit_raw.controller import Controller


class Handlers:
    def __init__(
        self, controller: 'Controller', net_config: NetworkConfig, storage: Storage
    ) -> None:
        self.net_config = net_config
        self.controller = controller
        self.serializer = Serializer(self.net_config, storage)
        self.deserializer = Deserializer(self.net_config, storage)
        self.storage = storage
        self.server_type = os.environ['SERVER_TYPE']

    async def on_version(self, message: bytes) -> None:
        logger.debug("handling version...")
        version = self.controller.deserializer.version(io.BytesIO(message))
        self.controller.sync_state.set_target_header_height(version["start_height"])
        logger.debug("Received version message: %s", version)
        verack_message = self.controller.serializer.verack()
        await self.controller.send_request(VERACK, verack_message)

    async def on_verack(self, message: bytes) -> None:
        logger.debug("handling verack...")
        logger.debug("handshake complete")
        self.controller.handshake_complete_event.set()
        pass

    async def on_protoconf(self, message: bytes) -> None:
        logger.debug("Handling protoconf...")
        protoconf = self.controller.deserializer.protoconf(io.BytesIO(message))
        logger.debug(f"Protoconf: {protoconf}")
        self.controller.handshake_complete_event.set()

    async def on_sendheaders(self, message: bytes) -> None:
        logger.debug("handling sendheaders...")
        pass

    async def on_sendcmpct(self, message: bytes) -> None:
        message_bytes = message
        logger.debug("handling sendcmpct...")
        logger.debug("received sendcmpct message: %s", message_bytes.decode('utf-8'))
        sendcmpct = self.controller.serializer.sendcmpct()
        logger.debug("responding with message: %s", sendcmpct)
        await self.controller.send_request(SENDCMPCT, sendcmpct)

    async def on_ping(self, message: bytes) -> None:
        logger.debug("handling ping...")
        pong_message = self.serializer.pong(message)
        await self.controller.send_request(PONG, pong_message)

    async def on_addr(self, message: bytes) -> None:
        logger.debug("handling addr...")
        pass

    async def on_feefilter(self, message: bytes) -> None:
        logger.debug("handling feefilter...")
        pass

    async def on_inv(self, message: bytes) -> None:
        """Todo: Optimization
        This could be optimized by staying in bytes. No need to convert the inv_type
        to int to categorise it - rather just match to corresponding byte string.
        payload_len/36 == count and the index in bytearray of each hash is just every 36th byte
        with an block_offset of 4."""
        inv_vect = self.controller.deserializer.inv(io.BytesIO(message))

        def have_header(inv: Inv) -> bool:
            try:
                self.storage.headers.lookup(hex_str_to_hash(inv['inv_hash']))
                return True
            except bitcoinx.MissingHeader:
                return False

        tx_inv_vect = []
        for inv in inv_vect:
            # TX
            if inv["inv_type"] == 1 and self.server_type == "ConduitIndex":
                # logger.debug(f"got inv: {inv}")
                tx_inv_vect.append(inv)

            # BLOCK
            elif inv["inv_type"] == 2 and self.server_type == "ConduitRaw":
                # logger.debug(f"got inv: {inv}")
                if not have_header(inv):
                    self.controller.sync_state.headers_new_tip_queue.put_nowait(inv)

                if hex_str_to_hash(inv["inv_hash"]) in \
                        self.controller.sync_state.all_pending_block_hashes:
                    self.controller.sync_state.pending_blocks_inv_queue.put_nowait(inv)
                # else:
                    # logger.debug(f"got new block notification: {inv['inv_hash']}")

        # if self.controller.sync_state.initial_block_download_event.is_set():
        if self.server_type == "ConduitIndex":
            max_getdata_size = 50_000
            num_getdatas = (len(tx_inv_vect) // max_getdata_size) + 1
            # e.g. if 100001 invs -> 3X getdata
            for i in range(num_getdatas):
                getdata_msg = self.serializer.getdata(tx_inv_vect[i:(i+1)*max_getdata_size])
                # logger.debug(f"sending getdata for tx_inv_vect={tx_inv_vect}")
                await self.controller.send_request(GETDATA, getdata_msg)

    async def on_getdata(self, message: bytes) -> None:
        logger.debug("handling getdata...")

    async def on_headers(self, message: bytes) -> None:
        logger.debug(f"got headers message")
        message_bytes: bytes = message
        if message_bytes[0:1] == b'\x00':
            self.controller.sync_state.headers_msg_processed_queue.put_nowait(None)
            return

        # message always includes headers far back enough to include common parent in the
        # event of a reorg
        is_reorg, start_header, stop_header, old_hashes, new_hashes = connect_headers_reorg_safe(
            message_bytes, self.storage.headers, headers_lock=self.storage.headers_lock)
        self.controller.sync_state.headers_msg_processed_queue.put_nowait((is_reorg, start_header, stop_header))

    # ----- Special case messages ----- #  # Todo - should probably be registered callbacks

    async def on_tx(self, rawtx: bytes) -> None:
        logger.debug(f"Got mempool tx")
        size_tx = len(rawtx)
        packed_message = struct.pack(f"<II{size_tx}s", MsgType.MSG_TX, size_tx, rawtx)
        if hasattr(self.controller, 'mempool_tx_socket'):  # Only conduit_index has this
            await self.controller.mempool_tx_socket.send(packed_message)

    async def on_block(self, special_message: tuple[int, int, bytes, int]) -> None:
        blk_start_pos, blk_end_pos, raw_block_header, tx_count = special_message
        blk_hash = double_sha256(raw_block_header)
        self.controller.sync_state.received_blocks.add(blk_hash)
        blk_height = self.storage.headers.lookup(blk_hash)[0].height
        if not blk_hash in self.controller.sync_state.all_pending_block_hashes:
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
