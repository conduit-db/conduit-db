# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.
import os
from io import BytesIO
import logging
import threading
from typing import cast, NamedTuple

import bitcoinx
from bitcoinx import Headers, MissingHeader, Header, Chain, double_sha256, hash_to_hex_str

from conduit_p2p import NetworkConfig


logger = logging.getLogger("conduit.p2p.headers.store")


# A reference to this cursor must be maintained and passed to the Headers.unpersisted_headers
# function in order to determine which newly appended headers still need to be appended
# to disc
HeaderPersistenceCursor = dict[bitcoinx.Chain, int]


ChainHashes = list[bytes]


class ReorgInfo(NamedTuple):
    commmon_height: int
    old_tip: Header
    new_tip: Header


class NewTipResult(NamedTuple):
    is_reorg: bool
    last_known_header: Header
    stop_header: Header
    old_chain: ChainHashes | None
    new_chain: ChainHashes | None
    reorg_info: ReorgInfo | None


class HeadersStore:
    """Threadsafe wrapper of `bitcoinx.Headers` store with helper functions for
    dealing with reorgs.

    The optional `lock` argument is for read-only access or to avoid needlessly acquiring
    the re-acquiring the Rlock when it has already been acquired manually."""
    def __init__(self, file_path: str, network_type: str) -> None:
        if bitcoinx._version[1] != 8:
            raise ValueError("This script requires bitcoinx version >= 0.8")

        self.net_config = NetworkConfig(network_type=network_type)
        self.file_path = file_path
        self.headers, self.cursor = self.read_cached_headers()
        self.headers_lock = threading.RLock()

    def flush(self) -> None:
        self.write_cached_headers()

    def write_cached_headers(self) -> HeaderPersistenceCursor:
        with open(self.file_path, "ab") as hf:
            hf.write(self.headers.unpersisted_headers(self.cursor))
        self.cursor = self.headers.cursor()
        return cast(HeaderPersistenceCursor, self.cursor)

    def read_cached_headers(self) -> tuple[Headers, HeaderPersistenceCursor]:
        if not os.path.exists(self.file_path):
            open(self.file_path, 'wb').close()
        logger.debug("New headers storage file: %s found", self.file_path)
        with open(self.file_path, "rb") as f:
            raw_headers = f.read()
        headers = Headers(self.net_config.BITCOINX_COIN)
        cursor = headers.connect_many(raw_headers, check_work=False)
        return headers, cursor

    def tip(self) -> Header:
        with self.headers_lock:
            return self.headers.longest_chain().tip()

    def chain_work_for_chain_and_height(self, chain: Chain, height: int) -> int:
        return cast(int, chain.chainwork_at_height(height))

    def connect(self, raw_header: bytes, check_work: bool = True, lock: bool = True) -> None:
        """Thread-safe wrapper around bitcoinx.Headers.connect"""
        try:
            if lock:
                self.headers_lock.acquire()
            self.headers.connect(raw_header, check_work=check_work)
        finally:
            self.headers_lock.release()

    def connect_headers(self, stream: BytesIO, lock: bool = False) -> tuple[bytes, bool]:
        """This method does not factor in any potential for reorgs. Use this method when you know for
        sure that the headers are in sorted order and will definitely connect to the longest
        chain of the headers store. Example: Initial headers download."""
        count = bitcoinx.read_varint(stream.read)
        success = True
        first_header_of_batch = b""
        try:
            if lock:
                self.headers_lock.acquire()
            for i in range(count):
                try:
                    raw_header = stream.read(80)
                    _tx_count = bitcoinx.read_varint(stream.read)
                    self.headers.connect(raw_header, check_work=True)
                    if i == 0:
                        first_header_of_batch = raw_header
                except MissingHeader as e:
                    if str(e).find(self.net_config.GENESIS_BLOCK_HASH) != -1:
                        logger.debug("skipping prev_out == genesis block")
                        continue
                    else:
                        logger.error(e)
                        success = False
                        return first_header_of_batch, success
            self.write_cached_headers()
            return first_header_of_batch, success
        finally:
            if lock:
                self.headers_lock.release()

    def get_header_for_height(self, height: int, lock: bool = True) -> bitcoinx.Header:
        try:
            if lock:
                self.headers_lock.acquire()
            chain = self.headers.longest_chain()
            header = self.headers.header_at_height(chain, height)
            return header
        finally:
            if lock:
                self.headers_lock.release()

    def get_header_for_hash(self, block_hash: bytes, lock: bool = True) -> bitcoinx.Header:
        """raises `bitcoinx.errors.MissingHeader`"""
        try:
            if lock:
                self.headers_lock.acquire()
            # The bitcoinx Headers.lookup method API has changed in v0.8
            # it used to return a tuple[Header, Chain] and raise MissingHeader if no header
            # was found. This allows us to expose the same API from app_state.lookup as before.
            chain: Chain
            chain, height = self.headers.lookup(block_hash)
            if chain is None:
                raise MissingHeader(f"No header found for hash: "
                    f"{hash_to_hex_str(block_hash)}")
            header = chain.header_at_height(height)
            return header
        finally:
            if lock:
                self.headers_lock.release()

    def find_common_parent(
        self,
        reorg_node_tip: Header,
        orphaned_tip: Header,
        chains: list[Chain],
        lock: bool = False,
    ) -> tuple[bitcoinx.Chain, int]:
        try:
            if lock:
                self.headers_lock.acquire()
            # Get orphan an reorg chains
            orphaned_chain = None
            reorg_chain = None
            for chain in chains:
                if chain.tip().hash == reorg_node_tip.hash:
                    reorg_chain = chain
                elif chain.tip().hash == orphaned_tip.hash:
                    orphaned_chain = chain

            if reorg_chain is not None and orphaned_chain is not None:
                (
                    chain,
                    common_parent_height,
                ) = reorg_chain.common_chain_and_height(orphaned_chain)
                return reorg_chain, common_parent_height
            elif reorg_chain is not None and orphaned_chain is None:
                return reorg_chain, 0
            else:
                # Should never happen
                raise ValueError("No common parent block header could be found")
        finally:
            if lock:
                self.headers_lock.release()

    def _reorg_detect(
        self,
        old_tip: bitcoinx.Header,
        new_tip: bitcoinx.Header,
        chains: list[Chain],
        lock: bool = False,
    ) -> ReorgInfo | None:
        try:
            if lock:
                self.headers_lock.acquire()
            assert new_tip.height > old_tip.height
            common_parent_chain, common_parent_height = self.find_common_parent(
                new_tip, old_tip, chains, lock
            )

            if common_parent_height < old_tip.height:
                depth = old_tip.height - common_parent_height
                logger.debug(
                    f"Reorg detected of depth: {depth}. "
                    f"Syncing missing blocks from height: "
                    f"{common_parent_height + 1} to {new_tip.height}"
                )
                return ReorgInfo(common_parent_height, old_tip, new_tip)
            return None
        except Exception:
            logger.exception("unexpected exception in reorg_detect")
            return None
        finally:
            if lock:
                self.headers_lock.release()

    def _get_chain_hashes_common_parent_to_tip(self, tip: Header, common_parent_height: int) -> ChainHashes:
        common_parent = self.get_header_for_height(common_parent_height)
        chain_hashes = []
        cur_header = tip
        while common_parent.hash != cur_header.hash:
            cur_header = self.get_header_for_hash(cur_header.hash, lock=False)
            chain_hashes.append(cur_header.hash)
            cur_header = self.get_header_for_hash(cur_header.prev_hash, lock=False)

        return chain_hashes

    def connect_headers_reorg_safe(self, stream: BytesIO) -> NewTipResult:
        """This method takes reorgs into account. Use this method when it's possible
        the header(s) will not connect to the local longest chain. Example: New inv
        received from remote node for updated chain tip.

        This needs to ingest a p2p messaging protocol style headers message and if a
        reorg is detected, it will find the common parent and return the old vs new chain
        starting from this common parent. This is needed for things like atomically
        removing & adding the affected txs from/to the mempool to achieve correct
        state after a reorg event.

        NOTE: Based on the locator hashes, the remote node will be sure to send all
        necessary headers between the common parent and the new tip.

        raises ValueError if it cannot connect the header"""
        with self.headers_lock:
            old_tip = self.headers.longest_chain().tip()
            count_chains_before = len(self.headers.chains())
            first_header_of_batch, success = self.connect_headers(stream, lock=False)
            if not success:
                raise ValueError("Could not connect p2p headers")

            count_chains_after = len(self.headers.chains())
            new_tip: Header = self.headers.longest_chain().tip()

            is_reorg = False
            old_chain = None
            new_chain = None
            reorg_info = None
            if count_chains_before < count_chains_after:
                is_reorg = True
                reorg_info = self._reorg_detect(old_tip, new_tip, self.headers.chains(), lock=False)
                assert reorg_info is not None
                common_parent_height, old_tip, new_tip = reorg_info
                last_known_header = self.get_header_for_height(common_parent_height, lock=False)
                stop_header = new_tip
                old_chain = self._get_chain_hashes_common_parent_to_tip(old_tip, common_parent_height)
                new_chain = self._get_chain_hashes_common_parent_to_tip(new_tip, common_parent_height)
            else:
                header = self.get_header_for_hash(double_sha256(first_header_of_batch))
                last_known_header = self.get_header_for_height(max(header.height-1, 0))
                stop_header = new_tip
            return NewTipResult(is_reorg, last_known_header, stop_header, old_chain, new_chain, reorg_info)

    def have_header(self, block_hash: bytes) -> bool:
        try:
            self.get_header_for_hash(block_hash)
            return True
        except bitcoinx.MissingHeader:
            return False
