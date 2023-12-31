# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.

import asyncio
import logging
import os
import typing
from typing import Any, TypedDict

import aiohttp
from bitcoinx import MissingHeader, double_sha256

from conduit_lib import cast_to_valid_ipv4
from conduit_p2p.headers import NewTipResult

if typing.TYPE_CHECKING:
    from conduit_raw.conduit_raw.controller import Controller

NODE_HOST = cast_to_valid_ipv4(os.getenv("NODE_HOST", "127.0.0.1"))
BITCOIN_RPC_PORT = os.getenv("BITCOIN_RPC_PORT", "18332")
REGTEST_BITCOIN_RPC_URL = f"http://rpcuser:rpcpassword@{NODE_HOST}:{BITCOIN_RPC_PORT}"


class NodeRPCResult(TypedDict):
    result: dict[Any, Any]


class NodeRPCTipJson(TypedDict):
    height: int
    hash: str
    branchlen: int
    status: str


class RegtestSupport:
    def __init__(self, controller: "Controller") -> None:
        self.logger = logging.getLogger("regtest-support")
        self.controller = controller
        self.aiohttp_client_session: aiohttp.ClientSession | None = None
        self.storage = self.controller.storage

    async def close(self) -> None:
        if self.aiohttp_client_session:
            await self._close_aiohttp_client_session()

    async def _get_aiohttp_client_session(self) -> aiohttp.ClientSession:
        if not self.aiohttp_client_session:
            self.aiohttp_client_session = aiohttp.ClientSession()
        return self.aiohttp_client_session

    async def _close_aiohttp_client_session(self) -> None:
        assert self.aiohttp_client_session is not None
        await self.aiohttp_client_session.close()

    async def regtest_fetch_block_header_for_hash(self, block_hash: str, verbose: bool = False) -> str:
        if not self.aiohttp_client_session:
            await self._get_aiohttp_client_session()
        assert self.aiohttp_client_session is not None
        body = {
            "jsonrpc": "2.0",
            "method": "getblockheader",
            "params": [block_hash, verbose],
            "id": 1,
        }
        result = await self.aiohttp_client_session.post(REGTEST_BITCOIN_RPC_URL, json=body)
        response_json = await result.json()
        block_header: str = response_json["result"]
        return block_header

    async def regtest_fetch_block_header_for_height(self, height: int) -> bytes:
        if not self.aiohttp_client_session:
            await self._get_aiohttp_client_session()
        body = {
            "jsonrpc": "2.0",
            "method": "getblockhash",
            "params": [height],
            "id": 1,
        }
        assert self.aiohttp_client_session is not None
        result = await self.aiohttp_client_session.post(REGTEST_BITCOIN_RPC_URL, json=body)
        response_json = await result.json()
        block_hash = response_json["result"]
        block_header = await self.regtest_fetch_block_header_for_hash(block_hash)
        return bytes.fromhex(block_header)

    async def regtest_get_chain_tip(self) -> NodeRPCTipJson | None:
        if not self.aiohttp_client_session:
            await self._get_aiohttp_client_session()
        body = {
            "jsonrpc": "2.0",
            "method": "getchaintips",
            "params": [],
            "id": 1,
        }
        assert self.aiohttp_client_session is not None
        result = await self.aiohttp_client_session.post(REGTEST_BITCOIN_RPC_URL, json=body)
        if result.status != 200:
            self.logger.error(
                f"regtest_poll_node_for_tip_job error. Status: {result.status} " f"Reason: {result.reason}"
            )
            return None
        response_json: NodeRPCResult = await result.json()
        # self.logger.debug(f"regtest_poll_node_for_tip_job result: {response_json}")

        best_tip: NodeRPCTipJson | None = None
        for tip in response_json["result"]:
            if not best_tip:
                best_tip = tip
            else:
                if tip["height"] > best_tip["height"]:  # pylint: disable=E1136
                    best_tip = tip

        return best_tip

    async def regtest_sync_headers(self, best_tip: NodeRPCTipJson) -> tuple[int, bytes, bytes, bool]:
        # Types
        height: int = -1
        first_header_of_batch: bytes = bytes()
        block_header: bytes = bytes()

        from_height = self.controller.headers_threadsafe.tip().height + 1
        to_height = best_tip["height"]
        try:
            if not to_height > self.controller.headers_threadsafe.tip().height:
                raise ValueError("Already synchronized to tip")

            for height in range(from_height, to_height + 1):
                block_header = await self.regtest_fetch_block_header_for_height(height)
                if height == from_height:
                    first_header_of_batch = block_header
                self.storage.headers.headers.connect(block_header)
            return height, first_header_of_batch, block_header, True
        except MissingHeader:
            return height, first_header_of_batch, block_header, False

    async def regtest_poll_node_for_tip_job(self) -> None:
        """REGTEST ONLY WORKER TASK"""
        while True:
            stop_header = None
            last_known_header_raw = None
            is_reorg = False
            try:
                best_tip = await self.regtest_get_chain_tip()
                if not best_tip:
                    await asyncio.sleep(5)
                    continue

                if best_tip["height"] > self.controller.headers_threadsafe.tip().height:
                    (
                        height,
                        last_known_header_raw,
                        stop_header,
                        succeeded,
                    ) = await self.regtest_sync_headers(best_tip)

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
                                self.storage.headers.connect(stop_header)
                                not_connecting = False
                            except MissingHeader:
                                continue

                        try:
                            last_known_header_raw = backfill_headers[-1][0]
                            for header, height in reversed(backfill_headers):
                                # self.logger.debug(f"Connecting backfill header:
                                # {hash_to_hex_str(double_sha256(header))}, height: {height}")
                                self.storage.headers.connect(header)
                        except MissingHeader:
                            raise RuntimeError("Connecting the backfill headers is failing unexpectedly")

                        # Now finally this should work
                        (
                            _,
                            _,
                            stop_header,
                            succeeded,
                        ) = await self.regtest_sync_headers(best_tip)
                        assert succeeded is True
                        assert stop_header == self.controller.headers_threadsafe.tip().raw

                if stop_header is not None:
                    last_known_header = self.controller.headers_threadsafe.get_header_for_hash(
                        double_sha256(last_known_header_raw)
                    )
                    stop_header_obj = self.controller.headers_threadsafe.get_header_for_hash(
                        double_sha256(stop_header)
                    )
                    if is_reorg:
                        common_parent_height = last_known_header.height - 1
                        old_tip_height = backfill_headers[0][1]
                        depth = old_tip_height - common_parent_height
                        self.logger.debug(
                            f"Reorg detected of depth: {depth}. "
                            f"Syncing missing blocks from height: "
                            f"{common_parent_height + 1} to {stop_header_obj.height}"
                        )

                    self.logger.info(
                        "New headers tip height: %s",
                        self.controller.headers_threadsafe.tip().height,
                    )
                    self.controller.new_headers_queue.put_nowait(
                        NewTipResult(is_reorg, last_known_header, stop_header_obj, None, None, None)
                    )
            except Exception:
                self.logger.exception("unexpected exception in regtest_poll_node_for_tip_job")
            finally:
                await asyncio.sleep(2)
