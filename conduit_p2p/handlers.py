# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.
import abc
import io
import logging
import typing
from asyncio import QueueFull

from bitcoinx import hash_to_hex_str, hex_str_to_hash

from .deserializer import Deserializer
from .networks import NetworkConfig
from .serializer import Serializer
from .types import BlockChunkData, BlockDataMsg, InvType, BitcoinClientMode, BlockType

if typing.TYPE_CHECKING:
    from .client_manager import BitcoinClientManager
    from .client import BitcoinClient


class HandlersDefault(abc.ABC):
    """For many use-cases, it's not necessary to flesh out all of these handlers but
    the bare minumum handlers have default implementations"""
    def __init__(
        self,
        network_type: str,
    ) -> None:
        self.net_config = NetworkConfig(network_type=network_type)
        self.serializer = Serializer(self.net_config)
        self.deserializer = Deserializer(self.net_config)
        self.logger = logging.getLogger("conduit.p2p.handlers")
        self.logger.setLevel(logging.DEBUG)
        self.client_manager: 'BitcoinClientManager | None' = None

    async def on_version(self, message: bytes, peer: 'BitcoinClient') -> None:
        version = self.deserializer.version(io.BytesIO(message))
        peer.remote_start_height = version['start_height']
        self.logger.debug("Received version: %s (peer_id=%s)", version, peer.id)
        verack_message = self.serializer.verack()
        self.logger.debug("Sending verack")
        peer.send_message(verack_message)

    async def on_verack(self, message: bytes, peer: 'BitcoinClient') -> None:
        self.logger.debug("Received verack (peer_id=%s)", peer.id)  # No payload

    async def on_protoconf(self, message: bytes, peer: 'BitcoinClient') -> None:
        protoconf = self.deserializer.protoconf(io.BytesIO(message))
        self.logger.debug("Received protoconf %r from peer: %s", protoconf, peer.id)

    async def on_sendheaders(self, message: bytes, peer: 'BitcoinClient') -> None:
        self.logger.debug("Received sendheaders (peer_id=%s)", peer.id)  # No payload

    async def on_sendcmpct(self, message: bytes, peer: 'BitcoinClient') -> None:
        self.logger.debug("Received sendcmpct (peer_id=%s)", peer.id)
        sendcmpct = self.serializer.sendcmpct()
        peer.send_message(sendcmpct)

    async def on_ping(self, message: bytes, peer: 'BitcoinClient') -> None:
        self.logger.debug(f"Received ping (peer_id=%s)", peer.id)
        pong_message = self.serializer.pong(message)
        self.logger.debug(f"Sending pong (peer_id=%s)", peer.id)
        peer.send_message(pong_message)

    async def on_pong(self, message: bytes, peer: 'BitcoinClient') -> None:
        pong = self.deserializer.pong(io.BytesIO(message))
        self.logger.debug("Received pong, nonce=%s (peer_id=%s)", pong, peer.id)

    async def on_addr(self, message: bytes, peer: 'BitcoinClient') -> None:
        new_addresses = 0
        addresses = self.deserializer.addr(io.BytesIO(message))
        if self.client_manager:
            for address in addresses:
                host_str = f"{address.node_addr.ip}:{address.node_addr.port}"
                if host_str not in self.client_manager.addresses:
                    client = self.client_manager.add_client(host_str)
                    if client and len(self.client_manager.get_connected_peers()) < self.client_manager.max_peers:
                        await self.client_manager.connect_client(client)
                    new_addresses += 1
            self.logger.debug("Received %s new addresses (peer_id=%s)", new_addresses, peer.id)
        else:
            self.logger.debug("Received %s addresses (peer_id=%s)", len(addresses), peer.id)

    async def on_feefilter(self, message: bytes, peer: 'BitcoinClient') -> None:
        feefilter = self.deserializer.feefilter(io.BytesIO(message))
        self.logger.debug("Received feefilter: %s (peer_id=%s)", feefilter, peer.id)

    async def on_authch(self, message: bytes, peer: 'BitcoinClient') -> None:
        self.logger.debug("Received authch (peer_id=%s)", peer.id)

    async def on_inv(self, message: bytes, peer: 'BitcoinClient') -> None:
        inv_vect = self.deserializer.inv(io.BytesIO(message))
        # self.logger.debug("Received inv: %s (peer_id=%s)", inv_vect, peer.id)

        # There's overhead to this but the pay-off is a high-level API
        # To opt-out of all overhead, just over-ride the whole handler
        if peer.mode == BitcoinClientMode.HIGH_LEVEL:
            inv_vect_txs = []
            inv_vect_blocks = []
            for inv in inv_vect:
                # TX
                if inv["inv_type"] == InvType.TX:
                    inv_vect_txs.append(inv)

                # BLOCK
                elif inv["inv_type"] == InvType.BLOCK:
                    inv_vect_blocks.append(inv)

            # TX
            if self.client_manager is not None:
                try:
                    if inv_vect_txs:
                        self.client_manager.tx_inv_queue.put_nowait((inv_vect_txs, peer))
                except QueueFull:
                    self.logger.warning("Inv queue is full. Are you draining it? "
                                        "This will block all handler tasks until it is cleared!")
                    await self.client_manager.tx_inv_queue.put((inv_vect_txs, peer))

            # BLOCK
            try:
                if inv_vect_blocks:
                    # This for waiting on new tip headers (filters out ones we already have)
                    peer.inv_queue_blocks.put_nowait(inv_vect_blocks)
            except QueueFull:
                self.logger.warning("Queue `inv_queue_blocks` is full. Are you draining it? "
                                    "This will block all handler tasks until it is cleared!")
                await peer.inv_queue_blocks.put(inv_vect_blocks)

            if inv_vect_blocks:
                for inv in inv_vect_blocks:
                    # self.logger.debug(f"Adding block: {inv['inv_hash']} to peer: {peer.id}")
                    peer.have_blocks.add(hex_str_to_hash(inv['inv_hash']))

    async def on_getdata(self, message: bytes, peer: 'BitcoinClient') -> None:
        getdata = self.deserializer.getdata(io.BytesIO(message))
        self.logger.debug("Received getdata: %s (peer_id=%s)", getdata, peer.id)

    async def on_headers(self, message: bytes, peer: 'BitcoinClient') -> None:
        optional_message: bytes | None = message
        if message[0:1] == b"\x00":
            optional_message = None

        if peer.mode == BitcoinClientMode.HIGH_LEVEL:
            try:
                peer.headers_queue.put_nowait(optional_message)
            except QueueFull:
                self.logger.warning("Headers queue is full. Are you draining it? "
                                    "This will block all handler tasks until it is cleared!")
                await peer.headers_queue.put(optional_message)
            return

        if optional_message:
            headers = self.deserializer.headers(io.BytesIO(message))
            self.logger.debug("Received headers: %s (peer_id=%s)", headers, peer.id)
        else:
            self.logger.debug("No headers returned (peer_id=%s)", peer.id)

    async def on_tx(self, rawtx: bytes, peer: 'BitcoinClient') -> None:
        tx = self.deserializer.tx(io.BytesIO(rawtx))
        self.logger.debug("Received rawtx: %s (peer_id=%s)", tx.hex_hash(), peer.id)

    async def on_reject(self, message: bytes, peer: 'BitcoinClient') -> None:
        reject_msg = self.deserializer.reject(io.BytesIO(message))
        if peer.mode == BitcoinClientMode.HIGH_LEVEL:
            peer.tx_reject_queue_map[hex_str_to_hash(reject_msg.item_hash)] = reject_msg
            return
        self.logger.debug("Received reject: %s (peer_id=%s)", reject_msg, peer.id)

    async def on_block_chunk(self, block_chunk_data: BlockChunkData, peer: 'BitcoinClient') -> None:
        """These block chunks are sized to the nearest whole transaction. This allows parallel processing.
        The transaction offsets are also provided for quick random access"""
        self.logger.debug("Received big block chunk number %s with block_hash: %s (peer_id=%s)",
            block_chunk_data.chunk_num, hash_to_hex_str(block_chunk_data.block_hash), peer.id)

    async def on_block(self, block_data_msg: BlockDataMsg, peer: 'BitcoinClient') -> None:
        """Small blocks are provided as one blob, whereas big blocks (exceeding LARGE_MESSAGE_LIMIT)
        are provided as chunks in the `on_block_chunk` callback."""

        block_hash = block_data_msg.block_hash
        if block_data_msg.block_type == BlockType.SMALL_BLOCK:
            self.logger.debug("Received small block with block_hash: %s (peer_id=%s)",
                hash_to_hex_str(block_hash), peer.id)
        else:
            self.logger.debug("Received all big block chunks for block_hash: %s (peer_id=%s)",
                hash_to_hex_str(block_hash), peer.id)

        if self.client_manager:
            self.client_manager.mark_block_done(block_hash)
