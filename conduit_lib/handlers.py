import array
import asyncio
import threading
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
from .constants import MsgType
from .deserializer import Deserializer
from .deserializer_types import Inv
from .networks import NetworkConfig
from .serializer import Serializer
from .store import Storage
from .types import DataLocation
from .utils import connect_headers_reorg_safe, create_task
from .bitcoin_p2p_types import BigBlock, BlockDataMsg, BlockType, BlockChunkData, \
    BitcoinPeerInstance

logger = logging.getLogger("handlers")


if typing.TYPE_CHECKING:
    from conduit_raw.conduit_raw.controller import Controller


class MessageHandlerProtocol(typing.Protocol):
    """For many use-cases, it's not necessary to flesh out all of these handlers but the version,
    verack, protoconf, ping and pong are the bare minimum just to connect and stay connected"""

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

    async def on_inv(self, message: bytes, peer: BitcoinPeerInstance) -> None:
        ...

    async def on_getdata(self, message: bytes, peer: BitcoinPeerInstance) -> None:
        ...

    async def on_headers(self, message: bytes, peer: BitcoinPeerInstance) -> None:
        ...

    async def on_tx(self, rawtx: bytes, peer: BitcoinPeerInstance) -> None:
        ...

    async def on_block_chunk(self, block_chunk_data: BlockChunkData, peer: BitcoinPeerInstance) -> None:
        ...

    async def on_block(self, block_data_msg: BlockDataMsg, peer: BitcoinPeerInstance) -> None:
        ...


class Handlers(MessageHandlerProtocol):
    def __init__(
        self, controller: 'Controller', net_config: NetworkConfig, storage: Storage
    ) -> None:
        self.net_config = net_config
        self.controller = controller
        self.serializer = Serializer(self.net_config)
        self.deserializer = Deserializer(self.net_config)
        self.storage = storage
        self.server_type = os.environ['SERVER_TYPE']
        self.small_blocks: list[bytes] = []
        self.small_blocks_lock = threading.RLock()  # uses ThreadPoolExecutor to flush to disc
        self.batched_tx_offsets: list[tuple[bytes, 'array.ArrayType[int]']] = []
        self.batched_tx_offsets_lock = threading.RLock()  # uses ThreadPoolExecutor to flush to disc
        self.executor = self.controller.general_executor

    async def on_version(self, message: bytes, peer: BitcoinPeerInstance) -> None:
        version = self.deserializer.version(io.BytesIO(message))
        self.controller.sync_state.set_target_header_height(version["start_height"])
        verack_message = self.serializer.verack()
        await peer.send_message(verack_message)

    async def on_verack(self, message: bytes, peer: BitcoinPeerInstance) -> None:
        ping_message = self.serializer.ping()
        await peer.send_message(ping_message)
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

    async def on_inv(self, message: bytes, peer: BitcoinPeerInstance) -> None:
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
                tx_inv_vect.append(inv)

            # BLOCK
            elif inv["inv_type"] == 2 and self.server_type == "ConduitRaw":
                if not have_header(inv):
                    self.controller.sync_state.headers_new_tip_queue.put_nowait(inv)

                if hex_str_to_hash(inv["inv_hash"]) in \
                        self.controller.sync_state.all_pending_block_hashes:
                    self.controller.sync_state.pending_blocks_inv_queue.put_nowait(inv)

        if tx_inv_vect and self.server_type == "ConduitIndex":
            max_getdata_size = 50_000
            num_getdatas = ceil(len(tx_inv_vect) / max_getdata_size)
            for i in range(num_getdatas):
                getdata_msg = self.serializer.getdata(tx_inv_vect[i:(i+1)*max_getdata_size])
                await peer.send_message(getdata_msg)

    async def on_getdata(self, message: bytes, peer: BitcoinPeerInstance) -> None:
        logger.debug("handling getdata...")

    async def on_headers(self, message: bytes, peer: BitcoinPeerInstance) -> None:
        # logger.debug(f"got headers message")
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

    async def on_tx(self, rawtx: bytes, peer: BitcoinPeerInstance) -> None:
        # logger.debug(f"Got mempool tx")
        size_tx = len(rawtx)
        packed_message = struct.pack(f"<II{size_tx}s", MsgType.MSG_TX, size_tx, rawtx)
        if hasattr(self.controller, 'socket_mempool_tx'):  # Only conduit_index has this
            await self.controller.socket_mempool_tx.send(packed_message)

    async def _lmdb_put_big_block_in_thread(self, big_block: BigBlock) -> None:
        await asyncio.get_running_loop().run_in_executor(self.executor,
            self.controller.lmdb.put_big_block, big_block)
        os.remove(self.big_block.temp_file_location.file_path)

    async def _lmdb_put_small_blocks_in_thread(self, small_blocks: list[bytes]) -> None:
        with self.small_blocks_lock:
            await asyncio.get_running_loop().run_in_executor(self.executor,
                self.controller.lmdb.put_blocks, small_blocks)

    async def _lmdb_put_tx_offsets_in_thread(self,
            batched_tx_offsets: list[tuple[bytes, 'array.ArrayType[int]']]) -> None:
        with self.batched_tx_offsets_lock:
            await asyncio.get_running_loop().run_in_executor(self.executor,
                self.controller.lmdb.put_tx_offsets, batched_tx_offsets)

    def get_next_mtree_worker_id(self) -> None:
        self.controller.current_mtree_worker_id += 1
        if self.controller.current_mtree_worker_id == \
                self.controller.WORKER_COUNT_MTREE_CALCULATORS:
            self.controller.current_mtree_worker_id = 1  # keep cycling around 1 -> 4

    def pack_block_chunk_message_for_worker(self, block_chunk_data: BlockChunkData) -> bytes:
        tx_offsets_bytes_for_chunk = block_chunk_data.tx_offsets_for_chunk
        return cast(bytes, cbor2.dumps((block_chunk_data.chunk_num, block_chunk_data.num_chunks,
            block_chunk_data.block_hash, tx_offsets_bytes_for_chunk.tobytes(),
            block_chunk_data.raw_block_chunk)))

    def pack_block_data_message_for_worker(self, block_chunk_data: BlockDataMsg) -> bytes:
        chunk_num = 1
        num_chunks = 1
        return cast(bytes, cbor2.dumps((chunk_num, num_chunks,
            block_chunk_data.block_hash, block_chunk_data.tx_offsets.tobytes(),
            block_chunk_data.small_block_data)))

    async def send_to_worker_async(self, packed_message: bytes) -> None:
        merkle_tree_socket = self.controller.merkle_tree_worker_sockets[self.controller.current_mtree_worker_id]
        if merkle_tree_socket:
            await merkle_tree_socket.send(packed_message)

    async def on_block_chunk(self, block_chunk_data: BlockChunkData, peer: BitcoinPeerInstance) -> None:
        """Any blocks that exceed the size of the network buffer are written to file but
        while the chunk of data is still in memory, it can be intercepted here to send to worker
        processes."""
        packed_message = self.pack_block_chunk_message_for_worker(block_chunk_data)
        await self.send_to_worker_async(packed_message)

    def ack_for_loaded_blocks(self, small_blocks: list[bytes]) -> None:
        if len(small_blocks) == 0:
            return None
        # Ack for all flushed blocks
        total_batch_size = 0
        for raw_block in small_blocks:
            total_batch_size += len(raw_block)
            block_hash = double_sha256(raw_block[0:80])
            self.controller.worker_ack_queue_blk_writer.put_nowait(block_hash)

        if total_batch_size > 0:
            logger.debug(f"total batch size for raw blocks="
                f"{round(total_batch_size/1024/1024, 3)} MB")

    async def blocks_flush_task_async(self) -> None:
        assert self.small_blocks is not None
        assert self.small_blocks_lock is not None
        try:
            while True:
                with self.batched_tx_offsets_lock:
                    if self.batched_tx_offsets:
                        # no specific ack'ing needs to be done for tx_offsets
                        # because the acks for writing raw blocks to disc has this covered
                        await self._lmdb_put_tx_offsets_in_thread(self.batched_tx_offsets)

                with self.small_blocks_lock:
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
        blk_height = self.storage.headers.lookup(block_data_msg.block_hash)[0].height
        if block_data_msg.block_hash not in self.controller.sync_state.all_pending_block_hashes:
            logger.debug(f"got an unsolicited block: {hash_to_hex_str(block_data_msg.block_hash)}, "
                         f"height={blk_height}. Discarding...")
            if block_data_msg.block_type & BlockType.BIG_BLOCK:
                assert block_data_msg.big_block_filepath is not None
                os.remove(block_data_msg.big_block_filepath)
            return

        # These are batched up to prevent HDD stutter
        with self.batched_tx_offsets_lock:
            self.batched_tx_offsets.append((block_data_msg.block_hash, block_data_msg.tx_offsets))

        if block_data_msg.block_type & BlockType.BIG_BLOCK:
            data_location = DataLocation(str(block_data_msg.big_block_filepath), start_offset=0,
                end_offset=block_data_msg.block_size)
            self.big_block = BigBlock(block_data_msg.block_hash, data_location,
                len(block_data_msg.tx_offsets))
            await self._lmdb_put_big_block_in_thread(big_block=self.big_block)
        else:
            with self.small_blocks_lock:
                assert block_data_msg.small_block_data is not None
                self.small_blocks.append(block_data_msg.small_block_data)
                packed_message = self.pack_block_data_message_for_worker(block_data_msg)
                await self.send_to_worker_async(packed_message)
        self.get_next_mtree_worker_id()
