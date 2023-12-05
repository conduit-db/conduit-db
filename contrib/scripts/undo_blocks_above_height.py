# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.

"""
There is no reason to believe that database corruption should ever occur but I want to be prepared
in case it does.
"""

import asyncio
import logging
import os
import struct
import sys
from pathlib import Path

import bitcoinx

from conduit_index.conduit_index.workers.common import (
    convert_pushdata_rows_for_flush,
    convert_input_rows_for_flush,
)
from conduit_lib import IPCSocketClient, setup_storage, NetworkConfig, DBInterface
from conduit_lib.algorithms import parse_txs
from conduit_lib.headers_api_threadsafe import HeadersAPIThreadsafe
from conduit_lib.startup_utils import load_dotenv, resolve_hosts_and_update_env_vars
from conduit_lib.types import BlockSliceRequestType, Slice
from conduit_lib.utils import get_network_type

MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))


def setup() -> None:
    dotenv_path = MODULE_DIR.parent.parent / ".env"
    load_dotenv(dotenv_path)
    resolve_hosts_and_update_env_vars()


class DbRepairTool:
    def __init__(self) -> None:
        logging.basicConfig(level=logging.DEBUG)
        self.logger = logging.getLogger("db-repair-tool")
        setup()

        # Ensure we do not accidentally reset the databases!
        os.environ["SERVER_TYPE"] = "ConduitIndex"
        os.environ["RESET_CONDUIT_INDEX"] = "0"
        os.environ["RESET_CONDUIT_RAW"] = "0"

        self.db = DBInterface.load_db()
        headers_dir = MODULE_DIR.parent.parent / "conduit_index"
        net_config = NetworkConfig(get_network_type(), node_host="127.0.0.1", node_port=18444)
        self.storage = setup_storage(net_config, headers_dir)
        self.headers_threadsafe = HeadersAPIThreadsafe(self.storage.headers, self.storage.headers_lock)

    def get_header_for_hash(self, block_hash: bytes) -> bitcoinx.Header:
        return self.headers_threadsafe.get_header_for_hash(block_hash)

    def get_header_for_height(self, height: int) -> bitcoinx.Header:
        return self.headers_threadsafe.get_header_for_height(height)

    def get_local_block_tip(self) -> bitcoinx.Header:
        with self.storage.block_headers_lock:
            return self.storage.block_headers.longest_chain().tip

    async def maybe_do_db_repair(self) -> None:
        """If there were blocks that were only partially flushed that go beyond the check-pointed
        block hash, we need to purge those rows from the tables before we resume synchronization.
        """
        result = self.db.get_checkpoint_state()
        if not result:
            self.db.initialise_checkpoint_state()
            return None

        (
            best_flushed_block_height,
            best_flushed_block_hash,
            reorg_was_allocated,
            first_allocated_block_hash,
            last_allocated_block_hash,
            old_hashes_array,
            new_hashes_array,
        ) = result
        # Delete / Clean up all db entries for blocks above the best_flushed_block_hash
        self.logger.debug(
            f"ConduitDB did not shut down cleanly last session. " f"Performing automated database repair..."
        )

        # Drop and re-create mempool table
        self.db.drop_mempool_table()
        self.db.create_mempool_table()

        # Delete / Clean up all db entries for blocks above the best_flushed_block_hash
        if reorg_was_allocated:
            old_hashes: list[bytes] | None = [
                old_hashes_array[i * 32 : (i + 1) * 32] for i in range(len(old_hashes_array) // 32)
            ]
            new_hashes: list[bytes] | None = [
                new_hashes_array[i * 32 : (i + 1) * 32] for i in range(len(new_hashes_array) // 32)
            ]
            assert new_hashes is not None
            await self.undo_specific_block_hashes(new_hashes)
        else:
            old_hashes = None
            new_hashes = None
            best_header = self.headers_threadsafe.get_header_for_hash(best_flushed_block_hash)
            await self.undo_blocks_above_height(best_header.height)

        # # Re-attempt indexing of allocated work (that did not complete last time)
        # self.logger.debug(f"Re-attempting previously allocated work")
        # first_allocated_header = self.headers_threadsafe.get_header_for_hash(first_allocated_block_hash)
        # last_allocated_header = self.headers_threadsafe.get_header_for_hash(last_allocated_block_hash)
        #
        # best_flushed_tip_height = await self.index_blocks(
        #     reorg_was_allocated,
        #     first_allocated_header,
        #     last_allocated_header,
        #     old_hashes,
        #     new_hashes,
        # )
        # self.logger.debug(f"Database repair complete. " f"New tip: {best_flushed_tip_height}")

    async def undo_blocks_above_height(self, height: int) -> None:
        unsafe_blocks = []
        tip_header = self.get_local_block_tip()
        for height in range(height + 1, tip_header.height + 1):
            header = self.get_header_for_height(height)
            unsafe_blocks.append((header.hash, height))

        await self._undo_blocks(unsafe_blocks)

    async def undo_specific_block_hashes(self, block_hashes: list[bytes]) -> None:
        unsafe_blocks = []
        for block_hash in block_hashes:
            header = self.get_header_for_hash(block_hash)
            unsafe_blocks.append((block_hash, header.height))

        await self._undo_blocks(unsafe_blocks)

    async def _undo_blocks(self, blocks_to_undo: list[tuple[bytes, int]]) -> None:
        ipc_sock_client = IPCSocketClient()
        for block_hash, height in blocks_to_undo:
            tx_offsets = next(ipc_sock_client.transaction_offsets_batched([block_hash]))
            block_num = ipc_sock_client.block_number_batched([block_hash]).block_numbers[0]
            slice = Slice(start_offset=0, end_offset=0)
            raw_blocks_array = ipc_sock_client.block_batched([BlockSliceRequestType(block_num, slice)])
            block_num, len_slice = struct.unpack_from(f"<IQ", raw_blocks_array, 0)
            _block_num, _len_slice, raw_block = struct.unpack_from(f"<IQ{len_slice}s", raw_blocks_array, 0)
            (
                tx_rows,
                _tx_rows_mempool,
                in_rows,
                pd_rows,
                utxo_spends,
                pushdata_matches_tip_filter,
            ) = parse_txs(
                raw_block,
                tx_offsets,
                height,
                confirmed=True,
                first_tx_pos_batch=0,
            )
            pushdata_rows_for_flushing = convert_pushdata_rows_for_flush(pd_rows)
            input_rows_for_flushing = convert_input_rows_for_flush(in_rows)

            # Delete
            tx_hashes = [row[0] for row in tx_rows]
            self.db.delete_transaction_rows(tx_hashes)
            self.db.delete_pushdata_rows(pushdata_rows_for_flushing)
            self.db.delete_input_rows(input_rows_for_flushing)
            self.db.delete_header_rows([block_hash])


async def main() -> None:
    # There is no reason to believe that database corruption should ever occur but I want to
    # be prepared in case it does.
    if len(sys.argv) != 2:
        print(f"Expecting a single integer argument: block_height")
        sys.exit(1)
    if not sys.argv[1].isdigit():
        print(f"Argument needs to be an integer type")

    non_corrupted_block_height = int(sys.argv[1])

    db_repair_tool = DbRepairTool()
    await db_repair_tool.undo_blocks_above_height(non_corrupted_block_height)


if __name__ == "__main__":
    asyncio.run(main())
