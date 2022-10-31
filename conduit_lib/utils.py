from __future__ import annotations

import asyncio
import bitcoinx
from bitcoinx import (read_le_uint64, read_be_uint16, double_sha256, MissingHeader, Headers, Header,
    Chain, Network, sha256)
import io
import ipaddress
import logging
import math
import os
from pathlib import Path
import socket
import struct
import threading
import time
from typing import Any, cast, Callable, Coroutine, Optional, TypeVar
import zmq

from .commands import BLOCK_BIN
from .constants import PROFILING, CONDUIT_INDEX_SERVICE_NAME, CONDUIT_RAW_SERVICE_NAME, \
    GENESIS_BLOCK, TESTNET, SCALINGTESTNET, REGTEST, MAINNET
from .deserializer_types import NodeAddr
from .types import ChainHashes


MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))

logger = logging.getLogger("conduit-lib-utils")
logger.setLevel(logging.DEBUG)


def is_docker() -> bool:
    path = '/proc/self/cgroup'
    return (
        os.path.exists('/.dockerenv') or
        os.path.isfile(path) and any('docker' in line for line in open(path))
    )


def payload_to_checksum(payload: bytearray | bytes) -> bytes:
    return cast(bytes, double_sha256(payload)[:4])


def is_block_msg(command: bytes) -> bool:
    return command == BLOCK_BIN.rstrip(b"\0")


def pack_null_padded_ascii(string: str, num_bytes: int) -> bytes:
    return struct.pack("%s" % num_bytes, string.encode("ASCII"))


def ipv4_to_mapped_ipv6(ipv4: str) -> bytes:
    return bytes(10) + bytes.fromhex("ffff") + ipaddress.IPv4Address(ipv4).packed


def mapped_ipv6_to_ipv4(f: io.BytesIO) -> NodeAddr:
    services = read_le_uint64(f.read)
    reserved = f.read(12)
    ipv4 = socket.inet_ntoa(f.read(4))
    port = read_be_uint16(f.read)
    return NodeAddr(services, ipv4, port)


# NOTE(AustEcon) - This is untested and probably wrong - I've never used it
def calc_bloom_filter_size(n_elements: int, false_positive_rate: int) -> int:
    """two parameters that need to be chosen. One is the size of the filter in bytes. The other
        is the number of hash functions to use. See bip37."""
    filter_size = (
        -1 / math.pow(math.log(2), 2) * n_elements * math.log(false_positive_rate)
    ) / 8
    return int(filter_size)


def unpack_varint_from_mv(buffer: bytes) -> tuple[int, int]:
    """buffer argument should be a memory view ideally and returns the value and how many bytes
    were read as a tuple"""
    (n,) = struct.unpack_from("B", buffer)
    if n < 253:
        return n, 1
    if n == 253:
        return struct.unpack_from("<H", buffer, offset=1)[0], 3
    if n == 254:
        return struct.unpack_from("<I", buffer, offset=1)[0], 5
    return struct.unpack_from("<Q", buffer, offset=1)[0], 9


def get_log_level(service_name: str) -> int:
    if service_name == CONDUIT_INDEX_SERVICE_NAME:
        level = os.getenv(f'CONDUIT_INDEX_LOG_LEVEL', 'DEBUG')
    elif service_name == CONDUIT_RAW_SERVICE_NAME:
        level = os.getenv(f'CONDUIT_RAW_LOG_LEVEL', 'DEBUG')
    else:
        level = 'DEBUG'

    if level == 'CRITICAL':
        return logging.CRITICAL
    if level == 'ERROR':
        return logging.ERROR
    if level == 'WARNING':
        return logging.WARNING
    if level == 'INFO':
        return logging.INFO
    if level == 'DEBUG':
        return logging.DEBUG
    if level == 'PROFILING':
        return PROFILING
    else:
        return logging.DEBUG


def get_conduit_raw_host_and_port() -> tuple[str, int]:
    CONDUIT_RAW_API_HOST: str = os.environ.get('CONDUIT_RAW_API_HOST', 'localhost')
    CONDUIT_RAW_API_PORT: int = int(os.environ.get('CONDUIT_RAW_API_PORT', '50000'))
    return CONDUIT_RAW_API_HOST, CONDUIT_RAW_API_PORT


def headers_to_p2p_struct(headers: list[bytes]) -> bytearray:
    count = len(headers)
    ba = bytearray()
    ba += bitcoinx.pack_varint(count)
    for header in headers:
        ba += header
        ba += bitcoinx.pack_varint(0)  # tx count
    return ba


def connect_headers(stream: io.BytesIO, headers_store: Headers) -> tuple[bytes, bool]:
    """Two mmap files - one for "headers-first download" and the other for the
    blocks we then download."""
    count = bitcoinx.read_varint(stream.read)
    success = True
    first_header_of_batch = b""
    for i in range(count):
        try:
            raw_header = stream.read(80)
            # logger.debug(f"Connecting {hash_to_hex_str(bitcoinx.double_sha256(raw_header))} "
            #              f"({i+1} of ({count})")
            _tx_count = bitcoinx.read_varint(stream.read)
            headers_store.connect(raw_header)
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
    headers_store.flush()
    return first_header_of_batch, success


def get_header_for_height(height: int, headers_store: Headers,
        lock: Optional[threading.RLock]) -> bitcoinx.Header:
    try:
        if lock:
            lock.acquire()
        chain = headers_store.longest_chain()
        header = headers_store.header_at_height(chain, height)
        return header
    finally:
        if lock:
            lock.release()


def get_header_for_hash(block_hash: bytes, headers_store: Headers,
        lock: Optional[threading.RLock]) -> bitcoinx.Header:
    try:
        if lock:
            lock.acquire()
        header, chain = headers_store.lookup(block_hash)
        return header
    finally:
        if lock:
            lock.release()


def find_common_parent(reorg_node_tip: Header, orphaned_tip: Header,
        chains: list[Chain], lock: Optional[threading.RLock]) -> tuple[bitcoinx.Chain, int]:
    try:
        if lock:
            lock.acquire()
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
            lock.release()


def reorg_detect(old_tip: bitcoinx.Header, new_tip: bitcoinx.Header, chains: list[Chain],
        lock: Optional[threading.RLock]) \
        -> tuple[int, Header, Header] | None:
    try:
        if lock:
            lock.acquire()
        assert new_tip.height > old_tip.height
        common_parent_chain, common_parent_height = find_common_parent(new_tip, old_tip, chains, lock)

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
            lock.release()


def _get_chain_hashes_back_to_common_parent(tip: Header, common_parent_height: int,
        headers_store: Headers, lock: threading.RLock) -> ChainHashes:
    """Used in reorg handling see: lmdb.get_reorg_differential"""
    common_parent = get_header_for_height(common_parent_height, headers_store, lock)

    chain_hashes = []
    cur_header = tip
    while common_parent.hash != cur_header.hash:
        cur_header = get_header_for_hash(cur_header.hash, headers_store, lock)
        chain_hashes.append(cur_header.hash)
        cur_header = get_header_for_hash(cur_header.prev_hash, headers_store, lock)

    return chain_hashes


def connect_headers_reorg_safe(message: bytes, headers_store: Headers,
        headers_lock: threading.RLock) \
            -> tuple[bool, Header, Header, ChainHashes | None, ChainHashes | None]:
    """This needs to ingest a p2p messaging protocol style headers message and if they do indeed
    constitute a reorg event, they need to go far back enough to include the common parent
    height so it can connect to our local headers longest chain. Otherwise, raises ValueError"""
    with headers_lock:
        headers_stream = io.BytesIO(message)
        old_tip = headers_store.longest_chain().tip
        count_chains_before = len(headers_store.chains())
        first_header_of_batch, success = connect_headers(headers_stream, headers_store)
        if not success:
            raise ValueError("Could not connect p2p headers")

        count_chains_after = len(headers_store.chains())
        new_tip: Header = headers_store.longest_chain().tip

        # Todo: consider what would happen if rogue BTC or BCH block headers were received
        # On reorg we want the block pre-fetcher to start further back at the common parent height
        is_reorg = False
        old_chain = None
        new_chain = None
        if count_chains_before < count_chains_after:
            is_reorg = True
            reorg_info = reorg_detect(old_tip, new_tip, headers_store.chains(), lock=None)
            assert reorg_info is not None
            common_parent_height, new_tip, old_tip = reorg_info
            start_header = get_header_for_height(common_parent_height + 1, headers_store, lock=None)
            stop_header = new_tip
            logger.debug(f"Reorg detected - common parent height: {common_parent_height}; "
                         f"old_tip={old_tip}; new_tip={new_tip}")

            old_chain = _get_chain_hashes_back_to_common_parent(old_tip, common_parent_height,
                headers_store, headers_lock)
            new_chain = _get_chain_hashes_back_to_common_parent(new_tip, common_parent_height,
                headers_store, headers_lock)
        else:
            start_header = get_header_for_hash(double_sha256(first_header_of_batch), headers_store,
                lock=None)
            stop_header = new_tip
        return is_reorg, start_header, stop_header, old_chain, new_chain


class InvalidNetworkException(Exception):
    pass


def get_network_type() -> str:
    nets = [TESTNET, SCALINGTESTNET, REGTEST, MAINNET]
    for key, val in os.environ.items():
        if key == 'NETWORK':
            if val.lower() not in nets:
                raise InvalidNetworkException(f"Network not found: must be one of: {nets}")
            return val.lower()
    raise ValueError("There is no 'NETWORK' key in os.environ")


def zmq_send_no_block(sock: zmq.Socket[bytes], msg: bytes, on_blocked_msg: str) -> None:
    while True:
        try:
            sock.send(msg, zmq.NOBLOCK)
            break
        except zmq.error.Again:
            logger.debug(on_blocked_msg)
            time.sleep(0.1)


def maybe_process_batch(process_batch_func: Callable[[list[bytes]], None],
        work_items: list[bytes], prev_time_check: float, batching_rate: float=0.3) \
            -> tuple[list[bytes], float]:
    time_diff = time.time() - prev_time_check
    if time_diff > batching_rate:
        prev_time_check = time.time()
        if work_items:
            process_batch_func(work_items)
        work_items = []
    return work_items, prev_time_check


def zmq_recv_and_process_batchwise_no_block(sock: zmq.Socket[bytes],
        process_batch_func: Callable[[list[bytes]], None], on_blocked_msg: str | None=None,
        batching_rate: float=0.3, poll_timeout_ms: int=1000) -> None:
    work_items: list[bytes] = []
    prev_time_check: float = time.time()
    try:
        while True:
            try:
                if sock.poll(poll_timeout_ms, zmq.POLLIN):
                    msg: bytes = sock.recv(zmq.NOBLOCK)
                    if not msg:
                        return  # poison pill
                    work_items.append(bytes(msg))

                    time_diff = time.time() - prev_time_check
                    if time_diff > batching_rate:
                        work_items, prev_time_check = maybe_process_batch(process_batch_func,
                            work_items, prev_time_check, batching_rate)
                else:
                    if on_blocked_msg:
                        logger.debug(on_blocked_msg)
                    work_items, prev_time_check = maybe_process_batch(process_batch_func,
                        work_items, prev_time_check, batching_rate)
            except zmq.error.Again:
                logger.debug(f"zmq.error.Again")
                continue
    finally:
        logger.info("Closing thread")
        sock.close()


def get_headers_dir_conduit_index() -> Path:
    return Path(os.getenv("HEADERS_DIR_CONDUIT_INDEX", str(MODULE_DIR.parent)))


T1 = TypeVar("T1")


def asyncio_future_callback(future: asyncio.Task[Any]) -> None:
    if future.cancelled():
        return
    try:
        future.result()
    except asyncio.CancelledError:
        pass
    except Exception:
        logger.exception(f"Unexpected exception in task")



def create_task(coro: Coroutine[Any, Any, T1]) -> asyncio.Task[T1]:
    task = asyncio.create_task(coro)
    # Futures catch all exceptions that are raised and store them. A task is a future. By adding
    # this callback we reraise any encountered exception in the callback and ensure it is visible.
    task.add_done_callback(asyncio_future_callback)
    return task


def bin_p2p_command_to_ascii(bin_command: bytes) -> str:
    return bin_command.rstrip(bytes(1)).decode()


def address_to_pushdata_hash(p2pkh_address: str, network: Network) -> bytes:
    address = bitcoinx.P2PKH_Address.from_string(p2pkh_address, network)
    return cast(bytes, sha256(address.hash160()))
