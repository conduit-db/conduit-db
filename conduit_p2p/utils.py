# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.

import asyncio
import concurrent.futures
import ipaddress
import logging
import math
import os
import sys
from pathlib import Path
import socket
import struct
from typing import Any, cast, Coroutine, TypeVar

import bitcoinx
from bitcoinx import double_sha256

from .commands import BLOCK_BIN
from .constants import (
    TESTNET,
    SCALINGTESTNET,
    REGTEST,
    MAINNET,
)

MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))

logger = logging.getLogger("conduit.p2p.utils")
logger.setLevel(logging.DEBUG)


def is_docker() -> bool:
    path = "/proc/self/cgroup"
    return (
        os.path.exists("/.dockerenv") or os.path.isfile(path) and any("docker" in line for line in open(path))
    )


def payload_to_checksum(payload: bytearray | bytes) -> bytes:
    return cast(bytes, double_sha256(payload)[:4])


def is_block_msg(command: bytes) -> bool:
    return command == BLOCK_BIN.rstrip(b"\0")


def pack_null_padded_ascii(string: str, num_bytes: int) -> bytes:
    return struct.pack("%s" % num_bytes, string.encode("ASCII"))


def ipv4_to_mapped_ipv6(ipv4: str) -> bytes:
    return bytes(10) + bytes.fromhex("ffff") + ipaddress.IPv4Address(ipv4).packed


# NOTE(AustEcon) - This is untested and probably wrong - I've never used it
def calc_bloom_filter_size(n_elements: int, false_positive_rate: int) -> int:
    """two parameters that need to be chosen. One is the size of the filter in bytes. The other
    is the number of hash functions to use. See bip37."""
    filter_size = (-1 / math.pow(math.log(2), 2) * n_elements * math.log(false_positive_rate)) / 8
    return int(filter_size)


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
        if key == "NETWORK":
            if val.lower() not in nets:
                raise InvalidNetworkException(f"Network not found: must be one of: {nets}")
            return val.lower()
    raise ValueError("There is no 'NETWORK' key in os.environ")


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


def cast_to_valid_ipv4(ipv4: str) -> str:
    """Resolve the IP address - important for docker usage"""
    try:
        ipaddress.ip_address(ipv4)
        return ipv4
    except ValueError:
        # Need to resolve dns name to get ipv4
        try:
            ipv4 = socket.gethostbyname(ipv4)
        except socket.gaierror:
            logger.error(f"Failed to resolve ip address for hostname: {ipv4}")
        return ipv4


async def cancel_tasks(tasks: list[asyncio.Task[T1]]) -> None:
    logger = logging.getLogger("task-cancellation")

    for task in tasks:
        if not task.done():
            task.cancel()

    await asyncio.sleep(0)
    results = await asyncio.gather(*tasks, return_exceptions=True)
    for result in results:
        if isinstance(result, asyncio.CancelledError):
            logger.debug(f"Task cancelled")
        elif isinstance(result, Exception):
            tb = sys.exc_info()[2]
            logger.debug(f"Task cancellation exception: {result.with_traceback(tb)}")
        elif not result:
            logger.debug(f"Task completed")
        else:
            logger.debug(f"Task cancellation result: {result}")
