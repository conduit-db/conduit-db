import asyncio
import os
import typing
from pathlib import Path
from typing import cast

import cbor2
from bitcoinx import hash_to_hex_str
from pyzstd import SeekableZstdFile

from conduit_lib.algorithms import double_sha256
from conduit_lib.constants import CONDUIT_RAW_SERVICE_NAME
from conduit_lib.database.ffdb.compression import open_seekable_writer_zstd, \
    CompressionStats, update_compresson_stats, CompressionBlockInfo, write_compression_stats
from conduit_lib.utils import create_task
from conduit_p2p import BitcoinClient, HandlersDefault
from conduit_p2p.types import BlockChunkData, BlockDataMsg, BigBlock, DataLocation, BlockType

if typing.TYPE_CHECKING:
    import array
    from conduit_raw.conduit_raw.controller import Controller


MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))


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


class IndexerHandlers(HandlersDefault):

    def __init__(self, network_type: str, controller: "Controller"):
        super().__init__(network_type)
        self.controller = controller

        self.headers_api_threadsafe = controller.storage.headers
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

        self.big_blocks_worker_id_map: dict[bytes, int] = {}  # block_hash: worker_id
        # Any blocks that exceed the size of the network buffer are instead writted to file
        self.big_block_write_directory = Path(os.environ.get("DATADIR_HDD", MODULE_DIR)) / "big_blocks"
        if not self.big_block_write_directory.exists():
            os.makedirs(self.big_block_write_directory, exist_ok=True)

        self.zstd_file_handle_cache: dict[bytes, tuple[SeekableZstdFile, CompressionStats, Path]] = {}
        self.controller.tasks.append(create_task(self.blocks_flush_task_async()))

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

    def ack_for_loaded_blocks(self, small_blocks: list[bytes]) -> None:
        if len(small_blocks) == 0:
            return None
        # Ack for all flushed blocks
        total_batch_size = 0
        for raw_block in small_blocks:
            total_batch_size += len(raw_block)
            block_hash = double_sha256(raw_block[0:80])
            if self.client_manager:
                self.client_manager.mark_block_done(block_hash)

        if total_batch_size > 0:
            self.logger.debug(f"total batch size for raw blocks=" f"{round(total_batch_size/1024/1024, 3)} MB")

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
                        self.logger.debug(f"Acking for {len(self.small_blocks)} loaded small blocks")
                        await self._lmdb_put_small_blocks_in_thread(self.small_blocks)
                        self.ack_for_loaded_blocks(self.small_blocks)
                        self.small_blocks = []
                await asyncio.sleep(0.3)
        except Exception as e:
            self.logger.exception("Caught exception")
            raise

    def write_data_zstd(self, seekable_zstd: SeekableZstdFile, data: bytes) -> tuple[int, int]:
        uncompressed_start_offset = 0  # SeekableZstdFile.tell() always 0 in append mode
        compressed_start_offset = seekable_zstd._fp.tell()  # type: ignore[attr-defined]
        seekable_zstd.write(data)
        uncompressed_end_offset = len(data)
        seekable_zstd.flush(SeekableZstdFile.FLUSH_FRAME)
        uncompressed_size = uncompressed_end_offset - uncompressed_start_offset
        compressed_size = seekable_zstd._fp.tell() - compressed_start_offset  # type: ignore[attr-defined]
        return uncompressed_size, compressed_size

    async def write_file_async_zstd(self, file: SeekableZstdFile, data: bytes) -> tuple[int, int]:
        return await asyncio.get_running_loop().run_in_executor(
            self.executor, self.write_data_zstd, file, data
        )

    async def on_block_chunk(self, block_chunk_data: BlockChunkData, peer: BitcoinClient) -> None:
        """These block chunks are sized to the nearest whole transaction.
        This allows parallel processing. The transaction offsets are also provided for quick
        random access"""
        block_hash = block_chunk_data.block_hash

        # Raw Block Writing Work
        # Write 'Big blocks' to a temporary file and os.move it to its final destination
        filepath = self.big_block_write_directory / os.urandom(16).hex()

        # Accumulate Compression Statistics Across Chunks
        if block_hash not in self.zstd_file_handle_cache:
            file_zstd = open_seekable_writer_zstd(filepath, mode='wb')
            compression_stats = CompressionStats()
            self.zstd_file_handle_cache[block_hash] = (file_zstd, compression_stats, filepath)
        else:
            file_zstd, compression_stats, filepath = self.zstd_file_handle_cache[block_hash]

        uncompressed_size, compressed_size = await self.write_file_async_zstd(
            file_zstd, block_chunk_data.raw_block_chunk
        )
        update_compresson_stats(
            filepath, uncompressed_size, compressed_size, compression_stats
        )

        is_last_chunk = block_chunk_data.chunk_num == block_chunk_data.num_chunks
        if is_last_chunk:
            tx_count = len(block_chunk_data.tx_offsets_for_chunk)
            block_metadata = CompressionBlockInfo(
                block_id=hash_to_hex_str(block_chunk_data.block_hash),
                tx_count=tx_count,
                size_mb=(block_chunk_data.block_size // 1024**2),
            )
            compression_stats.block_metadata = [block_metadata]
            write_compression_stats(compression_stats)

        # Merkle Tree Work
        # Pin chunks to the same worker (where it has necessary context)
        if block_chunk_data.block_hash not in self.big_blocks_worker_id_map:
            worker_id = await self.acquire_next_available_worker_id()
            self.big_blocks_worker_id_map[block_chunk_data.block_hash] = worker_id
        else:
            worker_id = self.big_blocks_worker_id_map[block_chunk_data.block_hash]
        packed_message = pack_block_chunk_message_for_worker(block_chunk_data)
        await self.send_to_worker_async(packed_message, worker_id)

    async def on_block(self, block_data_msg: BlockDataMsg, peer: BitcoinClient) -> None:
        """Any blocks that exceed the size of the network buffer are instead written to file"""
        block_hash = block_data_msg.block_hash
        if block_data_msg.block_type == BlockType.SMALL_BLOCK:
            self.logger.debug("Received small block with block_hash: %s (peer_id=%s)",
                hash_to_hex_str(block_hash), peer.id)
        else:
            self.logger.debug("Received all big block chunks for block_hash: %s (peer_id=%s)",
                hash_to_hex_str(block_hash), peer.id)

        if block_data_msg.block_type & BlockType.BIG_BLOCK:
            file_zstd, compression_stats, filepath = self.zstd_file_handle_cache[block_hash]
            file_zstd.close()
            data_location = DataLocation(
                str(filepath),
                start_offset=0,
                end_offset=block_data_msg.block_size,
            )
            big_block_location = BigBlock(
                block_hash,
                data_location,
                len(block_data_msg.tx_offsets),
            )
            await self._lmdb_put_big_block_in_thread(big_block_location)
            await self._lmdb_put_tx_offsets_in_thread(
                [(block_data_msg.block_hash, block_data_msg.tx_offsets)]
            )
            self.logger.debug(f"Acking for 1 loaded big block")
            if self.client_manager:
                self.client_manager.mark_block_done(block_hash)

            del self.zstd_file_handle_cache[block_hash]
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
