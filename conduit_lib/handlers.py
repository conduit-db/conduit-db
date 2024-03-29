# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.

import array
import asyncio
from math import ceil

import bitcoinx
import cbor2
from bitcoinx import hex_str_to_hash, hash_to_hex_str
import io
import os
import typing
from typing import cast
import logging
import struct

from .algorithms import double_sha256
from .constants import MsgType, CONDUIT_RAW_SERVICE_NAME, CONDUIT_INDEX_SERVICE_NAME
from .deserializer import Deserializer
from .deserializer_types import Inv
from .headers_api_threadsafe import HeadersAPIThreadsafe
from .networks import NetworkConfig
from .serializer import Serializer
from .store import Storage
from .types import DataLocation
from .utils import create_task
from .bitcoin_p2p_types import (
    BigBlock,
    BlockDataMsg,
    BlockType,
    BlockChunkData,
    BitcoinPeerInstance,
)

logger = logging.getLogger("handlers")

if typing.TYPE_CHECKING:
    from conduit_raw.conduit_raw.controller import Controller


def pack_block_chunk_message_for_worker(
    block_chunk_data: BlockChunkData,
) -> bytes:
    tx_offsets_bytes_for_chunk = block_chunk_data.tx_offsets_for_chunk
    return cast(  # type: ignore[redundant-cast]
        bytes,
        cbor2.dumps(
            (
                block_chunk_data.chunk_num,
                block_chunk_data.num_chunks,
                block_chunk_data.block_hash,
                tx_offsets_bytes_for_chunk.tobytes(),
                block_chunk_data.raw_block_chunk,
            )
        ),
    )


def pack_block_data_message_for_worker(block_chunk_data: BlockDataMsg) -> bytes:
    chunk_num = 1
    num_chunks = 1
    return cast(  # type: ignore[redundant-cast]
        bytes,
        cbor2.dumps(
            (
                chunk_num,
                num_chunks,
                block_chunk_data.block_hash,
                block_chunk_data.tx_offsets.tobytes(),
                block_chunk_data.small_block_data,
            )
        ),
    )


class MessageHandlerProtocol(typing.Protocol):
    """For many use-cases, it's not necessary to flesh out all of these handlers but the version,
    verack, protoconf, ping and pong are the bare minimum just to connect and stay connected
    """

    async def acquire_next_available_worker_id(self) -> int:
        ...

    async def release_worker_id(self, worker_id: int) -> None:
        ...

    async def on_version(self, message: bytes, peer: BitcoinPeerInstance) -> None:
        ...

    async def on_verack(self, message: bytes, peer: BitcoinPeerInstance) -> None:
        ...

    async def on_protoconf(self, message: bytes, peer: BitcoinPeerInstance) -> None:
        ...

    async def on_sendheaders(self, message: bytes, peer: BitcoinPeerInstance) -> None:
        ...

    async def on_sendcmpct(self, message: bytes, peer: BitcoinPeerInstance) -> None:
        ...

    async def on_ping(self, message: bytes, peer: BitcoinPeerInstance) -> None:
        ...

    async def on_pong(self, message: bytes, peer: BitcoinPeerInstance) -> None:
        ...

    async def on_addr(self, message: bytes, peer: BitcoinPeerInstance) -> None:
        ...

    async def on_feefilter(self, message: bytes, peer: BitcoinPeerInstance) -> None:
        ...

    async def on_authch(self, message: bytes, peer: BitcoinPeerInstance) -> None:
        pass

    async def on_inv(self, message: bytes, peer: BitcoinPeerInstance) -> None:
        ...

    async def on_getdata(self, message: bytes, peer: BitcoinPeerInstance) -> None:
        ...

    async def on_headers(self, message: bytes, peer: BitcoinPeerInstance) -> None:
        ...

    async def on_tx(self, rawtx: bytes, peer: BitcoinPeerInstance) -> None:
        ...

    async def on_block_chunk(
        self, block_chunk_data: BlockChunkData, peer: BitcoinPeerInstance, worker_id: int
    ) -> None:
        """The `worker_id` parameter gives control over whether big block chunks are routed to
        different workers or the same worker."""
        ...

    async def on_block(self, block_data_msg: BlockDataMsg, peer: BitcoinPeerInstance) -> None:
        ...


class Handlers(MessageHandlerProtocol):
    def __init__(
        self,
        controller: "Controller",
        net_config: NetworkConfig,
        storage: Storage,
    ) -> None:
        self.net_config = net_config
        self.controller = controller
        self.serializer = Serializer(self.net_config)
        self.deserializer = Deserializer(self.net_config)
        self.storage = storage
        self.headers_api_threadsafe = HeadersAPIThreadsafe(self.storage.headers, self.storage.headers_lock)
        self.server_type = os.environ["SERVER_TYPE"]
        self.small_blocks: list[bytes] = []
        self.batched_tx_offsets: list[tuple[bytes, "array.ArrayType[int]"]] = []
        # Allocated to workers at random
        self.mtree_worker_pool: asyncio.Queue[int] = asyncio.Queue()
        if controller.service_name == CONDUIT_RAW_SERVICE_NAME:
            for worker_id in range(1, self.controller.WORKER_COUNT_MTREE_CALCULATORS + 1):
                self.mtree_worker_pool.put_nowait(worker_id)

        # A ThreadPoolExecutor is used to flush to disc. These locks avoid
        # `blocks_flush_task_async` from emptying out newly appended `self.small_blocks` and
        # `self.batched_tx_offsets` when the await`ed threadpool returns. Bear in mind there are
        # multiple `handle_message_task_async` tasks running concurrently and calling `on_block`.
        self.small_blocks_lock = asyncio.Lock()
        self.batched_tx_offsets_lock = asyncio.Lock()  # uses ThreadPoolExecutor to flush to disc

        self.executor = self.controller.general_executor

    async def on_version(self, message: bytes, peer: BitcoinPeerInstance) -> None:
        version = self.deserializer.version(io.BytesIO(message))
        self.controller.sync_state.set_target_header_height(version["start_height"])
        verack_message = self.serializer.verack()
        await peer.send_message(verack_message)

    async def on_verack(self, message: bytes, peer: BitcoinPeerInstance) -> None:
        self.controller.tasks.append(create_task(self.blocks_flush_task_async()))

    async def on_protoconf(self, message: bytes, peer: BitcoinPeerInstance) -> None:
        protoconf = self.deserializer.protoconf(io.BytesIO(message))

    async def on_sendheaders(self, message: bytes, peer: BitcoinPeerInstance) -> None:
        pass

    async def on_sendcmpct(self, message: bytes, peer: BitcoinPeerInstance) -> None:
        sendcmpct = self.serializer.sendcmpct()
        await peer.send_message(sendcmpct)

    async def on_ping(self, message: bytes, peer: BitcoinPeerInstance) -> None:
        # logger.debug("handling ping...")
        pong_message = self.serializer.pong(message)
        await peer.send_message(pong_message)

    async def on_pong(self, message: bytes, peer: BitcoinPeerInstance) -> None:
        pass

    async def on_addr(self, message: bytes, peer: BitcoinPeerInstance) -> None:
        pass

    async def on_feefilter(self, message: bytes, peer: BitcoinPeerInstance) -> None:
        pass

    async def on_authch(self, message: bytes, peer: BitcoinPeerInstance) -> None:
        # New in https://github.com/bitcoin-sv/bitcoin-sv/releases/tag/v1.0.13
        # Ignoring all minerID related things at least for now
        pass

    async def on_inv(self, message: bytes, peer: BitcoinPeerInstance) -> None:
        inv_vect = self.controller.deserializer.inv(io.BytesIO(message))

        def have_header(inv: Inv) -> bool:
            try:
                self.headers_api_threadsafe.get_header_for_hash(hex_str_to_hash(inv["inv_hash"]))
                return True
            except bitcoinx.MissingHeader:
                return False

        tx_inv_vect = []
        for inv in inv_vect:
            # TX
            if inv["inv_type"] == 1 and self.server_type == "ConduitIndex":
                tx_inv_vect.append(inv)

            # BLOCK
            elif inv["inv_type"] == 2 and self.server_type == "ConduitRaw":
                if not have_header(inv):
                    self.controller.sync_state.headers_new_tip_queue.put_nowait(inv)

                # fmt: off
                if (hex_str_to_hash(inv["inv_hash"]) in
                        self.controller.sync_state.all_pending_block_hashes):  # type: ignore
                    self.controller.sync_state.pending_blocks_inv_queue.put_nowait(inv)
                # fmt: on

        if tx_inv_vect and self.server_type == "ConduitIndex":
            max_getdata_size = 50_000
            num_getdatas = ceil(len(tx_inv_vect) / max_getdata_size)
            for i in range(num_getdatas):
                getdata_msg = self.serializer.getdata(tx_inv_vect[i : (i + 1) * max_getdata_size])
                await peer.send_message(getdata_msg)

    async def on_getdata(self, message: bytes, peer: BitcoinPeerInstance) -> None:
        logger.debug("handling getdata...")

    async def on_headers(self, message: bytes, peer: BitcoinPeerInstance) -> None:
        # logger.debug(f"got headers message")
        message_bytes: bytes = message
        if message_bytes[0:1] == b"\x00":
            self.controller.sync_state.headers_msg_processed_queue.put_nowait(None)
            return

        # message always includes headers far back enough to include common parent in the
        # event of a reorg
        (
            is_reorg,
            start_header,
            stop_header,
            old_hashes,
            new_hashes,
        ) = self.headers_api_threadsafe.connect_headers_reorg_safe(message_bytes)
        self.controller.sync_state.headers_msg_processed_queue.put_nowait(
            (is_reorg, start_header, stop_header)
        )

    # ----- Special case messages ----- #  # Todo - should probably be registered callbacks

    async def on_tx(self, rawtx: bytes, peer: BitcoinPeerInstance) -> None:
        # logger.debug(f"Got mempool tx")
        size_tx = len(rawtx)
        packed_message = struct.pack(f"<II{size_tx}s", MsgType.MSG_TX, size_tx, rawtx)
        if self.controller.service_name == CONDUIT_INDEX_SERVICE_NAME:
            await self.controller.zmq_socket_listeners.socket_mempool_tx.send(packed_message)  # type: ignore
            self.controller.mempool_tx_count += 1  # type: ignore

    async def _lmdb_put_big_block_in_thread(self, big_block: BigBlock) -> None:
        assert self.controller.lmdb
        await asyncio.get_running_loop().run_in_executor(
            self.executor, self.controller.lmdb.put_big_block, big_block
        )

    async def _lmdb_put_small_blocks_in_thread(self, small_blocks: list[bytes]) -> None:
        assert self.controller.lmdb
        await asyncio.get_running_loop().run_in_executor(
            self.executor, self.controller.lmdb.put_blocks, small_blocks
        )

    async def _lmdb_put_tx_offsets_in_thread(
        self, batched_tx_offsets: list[tuple[bytes, "array.ArrayType[int]"]]
    ) -> None:
        assert self.controller.lmdb
        await asyncio.get_running_loop().run_in_executor(
            self.executor,
            self.controller.lmdb.put_tx_offsets,
            batched_tx_offsets,
        )

    async def release_worker_id(self, worker_id: int) -> None:
        await self.mtree_worker_pool.put(worker_id)

    async def acquire_next_available_worker_id(self) -> int:
        return await self.mtree_worker_pool.get()

    async def send_to_worker_async(self, packed_message: bytes, worker_id: int) -> None:
        merkle_tree_socket = self.controller.merkle_tree_worker_sockets[worker_id]
        if merkle_tree_socket:
            await merkle_tree_socket.send(packed_message)

    async def on_block_chunk(
        self, block_chunk_data: BlockChunkData, peer: BitcoinPeerInstance, worker_id: int
    ) -> None:
        """Any blocks that exceed the size of the network buffer are written to file but
        while the chunk of data is still in memory, it can be intercepted here to send to worker
        processes."""
        packed_message = pack_block_chunk_message_for_worker(block_chunk_data)
        await self.send_to_worker_async(packed_message, worker_id)

    def ack_for_loaded_blocks(self, small_blocks: list[bytes]) -> None:
        if len(small_blocks) == 0:
            return None
        # Ack for all flushed blocks
        total_batch_size = 0
        for raw_block in small_blocks:
            total_batch_size += len(raw_block)
            block_hash = double_sha256(raw_block[0:80])
            self.ack_for_block(block_hash)

        if total_batch_size > 0:
            logger.debug(f"total batch size for raw blocks=" f"{round(total_batch_size/1024/1024, 3)} MB")

    def ack_for_block(self, block_hash: bytes) -> None:
        self.controller.worker_ack_queue_blk_writer.put_nowait(block_hash)

    async def blocks_flush_task_async(self) -> None:
        assert self.small_blocks is not None
        assert self.small_blocks_lock is not None
        try:
            while True:
                async with self.batched_tx_offsets_lock:
                    if self.batched_tx_offsets:
                        # no specific ack'ing needs to be done for tx_offsets
                        # because the acks for writing raw blocks to disc has this covered
                        await self._lmdb_put_tx_offsets_in_thread(self.batched_tx_offsets)
                        self.batched_tx_offsets = []

                async with self.small_blocks_lock:
                    if self.small_blocks:
                        logger.debug(f"Acking for {len(self.small_blocks)} loaded small blocks")
                        await self._lmdb_put_small_blocks_in_thread(self.small_blocks)
                        self.ack_for_loaded_blocks(self.small_blocks)
                        self.small_blocks = []
                await asyncio.sleep(0.3)
        except Exception as e:
            logger.exception("Caught exception")
            raise

    async def on_block(self, block_data_msg: BlockDataMsg, peer: BitcoinPeerInstance) -> None:
        """Any blocks that exceed the size of the network buffer are instead written to file"""
        blk_height = self.headers_api_threadsafe.get_header_for_hash(block_data_msg.block_hash).height
        if block_data_msg.block_hash not in self.controller.sync_state.all_pending_block_hashes:  # type: ignore
            logger.debug(
                f"got an unsolicited block: {hash_to_hex_str(block_data_msg.block_hash)}, "
                f"height={blk_height}. Discarding..."
            )
            if block_data_msg.block_type & BlockType.BIG_BLOCK:
                assert block_data_msg.big_block_filepath is not None
                os.remove(block_data_msg.big_block_filepath)
            return
        if block_data_msg.block_type & BlockType.BIG_BLOCK:
            data_location = DataLocation(
                str(block_data_msg.big_block_filepath),
                start_offset=0,
                end_offset=block_data_msg.block_size,
            )
            big_block_location = BigBlock(
                block_data_msg.block_hash,
                data_location,
                len(block_data_msg.tx_offsets),
            )
            await self._lmdb_put_big_block_in_thread(big_block_location)
            await self._lmdb_put_tx_offsets_in_thread(
                [(block_data_msg.block_hash, block_data_msg.tx_offsets)]
            )
            logger.debug(f"Acking for 1 loaded big block")
            self.ack_for_block(block_data_msg.block_hash)
        else:
            # These are batched up to prevent HDD stutter
            async with self.batched_tx_offsets_lock:
                self.batched_tx_offsets.append((block_data_msg.block_hash, block_data_msg.tx_offsets))

            async with self.small_blocks_lock:
                assert block_data_msg.small_block_data is not None
                self.small_blocks.append(block_data_msg.small_block_data)
                packed_message = pack_block_data_message_for_worker(block_data_msg)
                try:
                    worker_id = await self.acquire_next_available_worker_id()
                    await self.send_to_worker_async(packed_message, worker_id)
                finally:
                    await self.release_worker_id(worker_id)
