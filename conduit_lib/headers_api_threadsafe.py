import io
import logging
import threading
from typing import cast

import bitcoinx
from bitcoinx import Headers, MissingHeader, Header, Chain, double_sha256

from conduit_lib.constants import GENESIS_BLOCK
from conduit_lib.types import ChainHashes

logger = logging.getLogger("headers-threadsafe-api")


class HeadersAPIThreadsafe:
    """Some methods have a boolean `lock` optional argument in the case of read-only
    access or to avoid needlessly acquiring the re-acquiring the Rlock when it has already been
    acquired."""

    def __init__(self, headers: Headers, headers_lock: threading.RLock) -> None:
        self.headers = headers
        self.headers_lock = headers_lock

    def tip(self) -> Header:
        with self.headers_lock:
            return self.headers.longest_chain().tip

    def chain_work_for_chain_and_heigth(self, chain: Chain, height: int) -> int:
        return cast(int, self.headers.chainwork_to_height(chain, height))

    def connect_headers(self, stream: io.BytesIO, lock: bool=False) -> tuple[bytes, bool]:
        count = bitcoinx.read_varint(stream.read)
        success = True
        first_header_of_batch = b""
        try:
            if lock:
                self.headers_lock.acquire()
            for i in range(count):
                try:
                    raw_header = stream.read(80)
                    # logger.debug(f"Connecting {hash_to_hex_str(
                    #   bitcoinx.double_sha256(raw_header))} ({i+1} of ({count})")
                    _tx_count = bitcoinx.read_varint(stream.read)
                    self.headers.connect(raw_header)
                    if i == 0:
                        first_header_of_batch = raw_header
                except MissingHeader as e:
                    if str(e).find(GENESIS_BLOCK) != -1:
                        logger.debug("skipping prev_out == genesis block")
                        continue
                    else:
                        logger.error(e)
                        success = False
                        return first_header_of_batch, success
            self.headers.flush()
            return first_header_of_batch, success
        finally:
            if lock:
                self.headers_lock.release()

    def get_header_for_height(self, height: int, lock: bool=True) -> bitcoinx.Header:
        try:
            if lock:
                self.headers_lock.acquire()
            chain = self.headers.longest_chain()
            header = self.headers.header_at_height(chain, height)
            return header
        finally:
            if lock:
                self.headers_lock.release()

    def get_header_for_hash(self, block_hash: bytes, lock: bool=True) -> bitcoinx.Header:
        try:
            if lock:
                self.headers_lock.acquire()
            header, chain = self.headers.lookup(block_hash)
            return header
        finally:
            if lock:
                self.headers_lock.release()

    def find_common_parent(self, reorg_node_tip: Header, orphaned_tip: Header, chains: list[Chain],
            lock: bool=False) -> tuple[bitcoinx.Chain, int]:
        try:
            if lock:
                self.headers_lock.acquire()
            # Get orphan an reorg chains
            orphaned_chain = None
            reorg_chain = None
            for chain in chains:
                if chain.tip.hash == reorg_node_tip.hash:
                    reorg_chain = chain
                elif chain.tip.hash == orphaned_tip.hash:
                    orphaned_chain = chain

            if reorg_chain is not None and orphaned_chain is not None:
                chain, common_parent_height = reorg_chain.common_chain_and_height(orphaned_chain)
                return reorg_chain, common_parent_height
            elif reorg_chain is not None and orphaned_chain is None:
                return reorg_chain, 0
            else:
                # Should never happen
                raise ValueError("No common parent block header could be found")
        finally:
            if lock:
                self.headers_lock.release()

    def reorg_detect(self, old_tip: bitcoinx.Header, new_tip: bitcoinx.Header, chains: list[Chain],
            lock: bool=False) -> tuple[int, Header, Header] | None:
        try:
            if lock:
                self.headers_lock.acquire()
            assert new_tip.height > old_tip.height
            common_parent_chain, common_parent_height = self.find_common_parent(
                new_tip, old_tip, chains, lock)

            if common_parent_height < old_tip.height:
                depth = old_tip.height - common_parent_height
                logger.debug(f"Reorg detected of depth: {depth}. "
                             f"Syncing missing blocks from height: "
                             f"{common_parent_height + 1} to {new_tip.height}")
                return common_parent_height, new_tip, old_tip
            return None
        except Exception:
            logger.exception("unexpected exception in reorg_detect")
            return None
        finally:
            if lock:
                self.headers_lock.release()

    def _get_chain_hashes_back_to_common_parent(self, tip: Header, common_parent_height: int) \
            -> ChainHashes:
        """Used in reorg handling see: lmdb.get_reorg_differential"""
        common_parent = self.get_header_for_height(common_parent_height)

        chain_hashes = []
        cur_header = tip
        while common_parent.hash != cur_header.hash:
            cur_header = self.get_header_for_hash(cur_header.hash, lock=False)
            chain_hashes.append(cur_header.hash)
            cur_header = self.get_header_for_hash(cur_header.prev_hash, lock=False)

        return chain_hashes

    def connect_headers_reorg_safe(self, message: bytes) -> tuple[
        bool, Header, Header, ChainHashes | None, ChainHashes | None]:
        """This needs to ingest a p2p messaging protocol style headers message and if they do indeed
        constitute a reorg event, they need to go far back enough to include the common parent
        height, so it can connect to our local headers longest chain. Otherwise, raises
        ValueError"""
        with self.headers_lock:
            headers_stream = io.BytesIO(message)
            old_tip = self.headers.longest_chain().tip
            count_chains_before = len(self.headers.chains())
            first_header_of_batch, success = self.connect_headers(headers_stream, lock=False)
            if not success:
                raise ValueError("Could not connect p2p headers")

            count_chains_after = len(self.headers.chains())
            new_tip: Header = self.headers.longest_chain().tip

            # Todo: consider what would happen if rogue BTC or BCH block headers were received
            # On reorg we want the block pre-fetcher to start further back at the common parent
            # height
            is_reorg = False
            old_chain = None
            new_chain = None
            if count_chains_before < count_chains_after:
                is_reorg = True
                reorg_info = self.reorg_detect(old_tip, new_tip, self.headers.chains(), lock=False)
                assert reorg_info is not None
                common_parent_height, new_tip, old_tip = reorg_info
                start_header = self.get_header_for_height(common_parent_height + 1, lock=False)
                stop_header = new_tip
                logger.debug(f"Reorg detected - common parent height: {common_parent_height}; "
                             f"old_tip={old_tip}; new_tip={new_tip}")
                old_chain = self._get_chain_hashes_back_to_common_parent(old_tip,
                    common_parent_height)
                new_chain = self._get_chain_hashes_back_to_common_parent(new_tip,
                    common_parent_height)
            else:
                start_header = self.get_header_for_hash(double_sha256(first_header_of_batch))
                stop_header = new_tip
            return is_reorg, start_header, stop_header, old_chain, new_chain
