import asyncio
import bitcoinx
from bitcoinx import (read_le_uint64, read_be_uint16, double_sha256, Network, sha256)
import stat
import concurrent.futures
import io
import ipaddress
import logging
import math
import os
from pathlib import Path
import socket
import struct
import time
from typing import Any, cast, Callable, Coroutine, TypeVar
import zmq
from zmq.asyncio import Socket as AsyncZMQSocket


from .commands import BLOCK_BIN
from .constants import PROFILING, CONDUIT_INDEX_SERVICE_NAME, CONDUIT_RAW_SERVICE_NAME, \
    TESTNET, SCALINGTESTNET, REGTEST, MAINNET
from .deserializer_types import NodeAddr

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
    IPC_SOCKET_SERVER_HOST: str = os.environ.get('IPC_SOCKET_SERVER_HOST', 'localhost')
    IPC_SOCKET_SERVER_PORT: int = int(os.environ.get('IPC_SOCKET_SERVER_PORT', '50000'))
    return IPC_SOCKET_SERVER_HOST, IPC_SOCKET_SERVER_PORT


def headers_to_p2p_struct(headers: list[bytes]) -> bytearray:
    count = len(headers)
    ba = bytearray()
    ba += bitcoinx.pack_varint(count)
    for header in headers:
        ba += header
        ba += bitcoinx.pack_varint(0)  # tx count
    return ba


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


def zmq_send_no_block(sock: zmq.Socket[bytes], msg: bytes, on_blocked_msg: str | None = None,
        timeout: float | None = None) -> None:
    start_time = time.time()
    while True:
        try:
            sock.send(msg, zmq.NOBLOCK)
            break
        except zmq.error.Again:
            if timeout and (time.time() - start_time) > timeout:
                raise TimeoutError("failed sending zmq message")
            if on_blocked_msg:
                logger.debug(on_blocked_msg)
            time.sleep(0.1)


async def zmq_send_no_block_async(sock: 'AsyncZMQSocket', msg: bytes,
        on_blocked_msg: str | None = None, timeout: float | None = None) -> None:
    start_time = time.time()
    while True:
        try:
            await sock.send(msg, zmq.NOBLOCK)
            break
        except zmq.error.Again:
            if timeout and (time.time() - start_time) > timeout:
                raise TimeoutError("failed sending zmq message")
            if on_blocked_msg:
                logger.debug(on_blocked_msg)
            await asyncio.sleep(0.1)


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


def future_callback(future: concurrent.futures.Future[None]) -> None:
    if future.cancelled():
        return
    future.result()


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


def network_str_to_bitcoinx_network(network: str) -> bitcoinx.Network:
    if network == MAINNET:
        return bitcoinx.Bitcoin
    elif network == TESTNET:
        return bitcoinx.BitcoinTestnet
    elif network == SCALINGTESTNET:
        return bitcoinx.BitcoinScalingTestnet
    elif network == REGTEST:
        return bitcoinx.BitcoinRegtest
    else:
        raise NotImplementedError(f"Unrecognized network type: '{network}'")


def remove_readonly(func: Callable[[Path], None], path: Path,
        excinfo: BaseException | None) -> None:
    os.chmod(path, stat.S_IWRITE)
    func(path)
