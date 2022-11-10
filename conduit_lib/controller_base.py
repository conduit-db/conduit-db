import asyncio
import logging
import math
from concurrent.futures import ThreadPoolExecutor

from bitcoinx import Header

from conduit_lib import IPCSocketClient
from conduit_lib.ipc_sock_msg_types import BlockMetadataBatchedResponse


class ControllerBase:
    """For code sharing between ConduitRaw and ConduitIndex Controllers.
    This should be expanded upon"""

    loop: asyncio.AbstractEventLoop
    general_executor: ThreadPoolExecutor
    logger: logging.Logger
    estimated_moving_av_block_size_mb: float

    def get_header_for_height(self, height: int) -> Header:
        raise NotImplementedError()

    def get_ideal_block_batch_count(self, target_bytes: int) -> int:
        """If average batch size exceeds the target_bytes level then we will be at the point
        of requesting 1 block at a time.

        This is intended so that as block sizes increase we are not requesting 500 x 4GB blocks!
        As the average block size increases we should gradually reduce the number of raw blocks
        we request as a single batch."""
        estimated_ideal_block_count = math.ceil((target_bytes / (1024 ** 2)) /
                                                self.estimated_moving_av_block_size_mb)
        MAX_BLOCK_COUNT = 250
        estimated_ideal_block_count = min(estimated_ideal_block_count, MAX_BLOCK_COUNT)
        self.logger.debug(f"Using estimated_ideal_block_count: {estimated_ideal_block_count}")
        return estimated_ideal_block_count

    async def update_moving_average(self, current_tip_height: int) -> None:
        # sample every 72nd block for its size over the last 2 weeks (two samples per day)
        block_hashes = []
        for height in range(current_tip_height - 2016, current_tip_height, 72):
            header = self.get_header_for_height(height)
            block_hashes.append(header.hash)

        ipc_sock_client = await self.loop.run_in_executor(self.general_executor,
            IPCSocketClient)
        response: BlockMetadataBatchedResponse = await self.loop.run_in_executor(
            self.general_executor, ipc_sock_client.block_metadata_batched, block_hashes)
        block_metadata_batch = response.block_metadata_batch

        block_sizes = [m.block_size for m in block_metadata_batch]
        self.estimated_moving_av_block_size_mb = \
            (math.ceil(sum(block_sizes) / len(block_metadata_batch))) / (1024 ** 2)
        self.logger.debug(f"Updated estimated_moving_av_block_size: "
                          f"{self.estimated_moving_av_block_size_mb:.3f} MB")