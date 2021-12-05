import asyncio
import logging
import os
import typing
from typing import Optional, Tuple

from bitcoinx import MissingHeader, double_sha256, hash_to_hex_str

from conduit_lib import cast_to_valid_ipv4

if typing.TYPE_CHECKING:
    from conduit_raw.conduit_raw.controller import Controller


BITCOIN_HOST = cast_to_valid_ipv4(os.getenv('BITCOIN_HOST', '127.0.0.1'))
BITCOIN_RPC_PORT = os.getenv('BITCOIN_RPC_PORT', '18332')
REGTEST_BITCOIN_RPC_URL = f"http://rpcuser:rpcpassword@{BITCOIN_HOST}:{BITCOIN_RPC_PORT}"


class RegtestSupport:

    def __init__(self, controller: 'Controller'):
        self.logger = logging.getLogger('regtest-support')
        self.controller = controller
        self.aiohttp_client_session = self.controller.aiohttp_client_session
        self.storage = self.controller.storage

    async def regtest_fetch_block_header_for_hash(self, block_hash: str, verbose: bool=False) \
            -> str:
        body = {"jsonrpc": "2.0", "method": "getblockheader", "params": [block_hash, verbose],
            "id": 1}
        assert self.aiohttp_client_session is not None
        result = await self.aiohttp_client_session.post(REGTEST_BITCOIN_RPC_URL, json=body)
        response_json = await result.json()
        block_header = response_json['result']
        return block_header

    async def regtest_fetch_block_header_for_height(self, height: int) -> bytes:
        body = {"jsonrpc": "2.0", "method": "getblockhash", "params": [height], "id": 1}
        assert self.aiohttp_client_session is not None
        result = await self.aiohttp_client_session.post(REGTEST_BITCOIN_RPC_URL, json=body)
        response_json = await result.json()
        block_hash = response_json['result']
        block_header = await self.regtest_fetch_block_header_for_hash(block_hash)
        return bytes.fromhex(block_header)

    async def regtest_get_chain_tip(self):
        body = {"jsonrpc": "2.0", "method": "getchaintips", "params": [], "id": 1}
        result = await self.aiohttp_client_session.post(REGTEST_BITCOIN_RPC_URL, json=body)
        if result.status != 200:
            self.logger.error(f"regtest_poll_node_for_tip_job error. Status: {result.status} "
                              f"Reason: {result.reason}")
            return
        response_json: dict = await result.json()
        # self.logger.debug(f"regtest_poll_node_for_tip_job result: {response_json}")

        best_tip: Optional[dict[str, dict]] = None
        for tip in response_json['result']:
            if not best_tip:
                best_tip = tip
            else:
                if tip['height'] > best_tip['height']:  # pylint: disable=E1136
                    best_tip = tip

        return best_tip

    async def regtest_sync_headers(self, best_tip: dict) \
            -> Tuple[Optional[int], Optional[bytes], Optional[bytes], bool]:
        from_height = self.controller.sync_state.get_local_tip_height() + 1
        to_height = best_tip['height']
        height = None
        first_header_of_batch = None
        block_header = None
        try:
            for height in range(from_height, to_height + 1):
                block_header = await self.regtest_fetch_block_header_for_height(height)
                if height == from_height:
                    first_header_of_batch = block_header
                self.storage.headers.connect(block_header)
            return height, first_header_of_batch, block_header, True
        except MissingHeader:
            return height, first_header_of_batch, block_header, False

    async def regtest_poll_node_for_tip_job(self):
        """REGTEST ONLY WORKER TASK"""
        while True:
            stop_header = None
            start_header = None
            is_reorg = False
            try:
                best_tip = await self.regtest_get_chain_tip()
                if not best_tip:
                    await asyncio.sleep(5)
                    continue

                if best_tip['height'] > self.controller.sync_state.get_local_tip_height():
                    height, start_header, stop_header, succeeded = await self.regtest_sync_headers(best_tip)

                    if not succeeded:
                        is_reorg = True
                        not_connecting = True

                        # decrease height until it connects
                        backfill_headers = []
                        while not_connecting:
                            height -= 1
                            try:
                                stop_header = await self.regtest_fetch_block_header_for_height(height)
                                backfill_headers.append((stop_header, height))
                                with self.storage.headers_lock:
                                    self.storage.headers.connect(stop_header)
                                not_connecting = False
                            except MissingHeader:
                                continue

                        try:
                            start_header = backfill_headers[-1][0]
                            for header, height in reversed(backfill_headers):
                                # self.logger.debug(f"Connecting backfill header:
                                # {hash_to_hex_str(double_sha256(header))}, height: {height}")
                                with self.storage.headers_lock:
                                    self.storage.headers.connect(header)
                        except MissingHeader:
                            raise RuntimeError("Connecting the backfill headers is failing unexpectedly")

                        # Now finally this should work
                        _, _, stop_header, succeeded = await self.regtest_sync_headers(best_tip)
                        assert succeeded is True
                        assert stop_header == self.controller.sync_state.get_local_tip().raw

                if stop_header is not None:
                    start_header_obj = self.controller.get_header_for_hash(double_sha256(start_header))
                    stop_header_obj = self.controller.get_header_for_hash(double_sha256(stop_header))
                    if is_reorg:
                        common_parent_height = start_header_obj.height - 1
                        old_tip_height = backfill_headers[0][1]
                        depth = old_tip_height - common_parent_height
                        self.logger.debug(f"Reorg detected of depth: {depth}. "
                                          f"Syncing missing blocks from height: "
                                          f"{common_parent_height + 1} to {stop_header_obj.height}")

                    self.controller.sync_state.headers_event_initial_sync.set()
                    self.controller.sync_state.local_tip_height = self.controller.sync_state.update_local_tip_height()
                    self.logger.debug("New headers tip height: %s", self.controller.sync_state.local_tip_height)
                    self.controller.new_headers_queue.put_nowait((is_reorg, start_header_obj, stop_header_obj))
            except Exception:
                self.logger.exception("unexpected exception in regtest_poll_node_for_tip_job")
            finally:
                await asyncio.sleep(2)
