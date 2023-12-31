# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.

import asyncio
import logging
import math
from concurrent.futures import ThreadPoolExecutor

from conduit_lib import IPCSocketClient
from conduit_lib.constants import MAX_BLOCK_PER_BATCH_COUNT_CONDUIT_INDEX, CONDUIT_RAW_SERVICE_NAME, \
    MAX_BLOCK_PER_BATCH_COUNT_CONDUIT_RAW
from conduit_lib.ipc_sock_msg_types import BlockMetadataBatchedResponse
from conduit_p2p import HeadersStore


class ControllerBase:
    """For code sharing between ConduitRaw and ConduitIndex Controllers.
    This should be expanded upon"""

    loop: asyncio.AbstractEventLoop
    general_executor: ThreadPoolExecutor
    logger: logging.Logger
    estimated_moving_av_block_size_mb: float
    headers_threadsafe: HeadersStore

    def get_ideal_block_batch_count(self, target_bytes: int, service_type: str, tip_height: int) -> int:
        """If average batch size exceeds the target_bytes level then we will be at the point
        of requesting 1 block at a time.

        This is intended so that as block sizes increase we are not requesting 500 x 4GB blocks!
        As the average block size increases we should gradually reduce the number of raw blocks
        we request as a single batch."""
        estimated_ideal_block_count = math.ceil(
            (target_bytes / (1024**2)) / self.estimated_moving_av_block_size_mb
        )
        # If MAX_BLOCK_COUNT is too small it wastes a lot of time on the checkpointing process
        # between each batch of blocks.
        # If MAX_BLOCK_COUNT is too large, the checkpoints are too infrequent and a crash
        # will result in a very large rollback (this should also be avoided).
        # When the blocks are small on average we want the MAX_BLOCK_COUNT to be relatively
        # large.
        # When the blocks are larger (and especially when they have a high density tx count per
        # GB of data) we want the MAX_BLOCK_COUNT to be relatively smaller to "lock-in" hard-won
        # progress more frequently.
        max_block_count = MAX_BLOCK_PER_BATCH_COUNT_CONDUIT_INDEX
        if service_type == CONDUIT_RAW_SERVICE_NAME:
            max_block_count = MAX_BLOCK_PER_BATCH_COUNT_CONDUIT_RAW
        if tip_height > 200000:
            max_block_count = 250
        if tip_height > 710000:
            max_block_count = 100
        if tip_height > 800000:
            max_block_count = 20
        estimated_ideal_block_count = min(estimated_ideal_block_count, max_block_count)
        self.logger.debug(f"Using estimated_ideal_block_count: {estimated_ideal_block_count}")
        return estimated_ideal_block_count

    async def update_moving_average(self, to_height: int) -> None:
        # sample every 72nd block for its size over the last 2 weeks (two samples per day)
        block_hashes = []
        for height in range(to_height - 2016, to_height, 72):
            header = self.headers_threadsafe.get_header_for_height(height)
            block_hashes.append(header.hash)

        ipc_sock_client = await self.loop.run_in_executor(self.general_executor, IPCSocketClient)
        response: BlockMetadataBatchedResponse = await self.loop.run_in_executor(
            self.general_executor,
            ipc_sock_client.block_metadata_batched,
            block_hashes,
        )
        block_metadata_batch = response.block_metadata_batch

        block_sizes = [m.block_size for m in block_metadata_batch]
        self.estimated_moving_av_block_size_mb = (math.ceil(sum(block_sizes) / len(block_metadata_batch))) / (
            1024**2
        )
        self.logger.debug(
            f"Updated estimated_moving_av_block_size: " f"{self.estimated_moving_av_block_size_mb:.3f} MB"
        )
