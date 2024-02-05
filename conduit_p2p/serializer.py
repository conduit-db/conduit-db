# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.

"""https://en.bitcoin.it/wiki/Protocol_documentation#Message_structure"""
import logging
import math
import random
import time
from enum import IntEnum
from os import urandom
from typing import List, cast

from bitcoinx import (
    pack_le_uint32,
    pack_varint,
    pack_le_uint64,
    pack_le_int64,
    pack_varbytes,
    hex_str_to_hash,
    int_to_be_bytes,
    pack_be_uint16,
    pack_byte, pack_le_int32,
)

from .commands import (
    VERSION_BIN,
    VERACK_BIN,
    GETADDR_BIN,
    FILTERCLEAR_BIN,
    GETHEADERS_BIN,
    INV_BIN,
    TX_BIN,
    GETDATA_BIN,
    GETBLOCKS_BIN,
    PING_BIN,
    MEMPOOL_BIN,
    PONG_BIN,
    SENDCMPCT_BIN,
    FILTERLOAD_BIN,
)
from .deserializer import Inv
from .networks import NetworkConfig

from .utils import (
    payload_to_checksum,
    ipv4_to_mapped_ipv6,
    calc_bloom_filter_size,
)
from .constants import ZERO_HASH

logger = logging.getLogger("conduit.p2p.serializer")


class CompactBlockMode(IntEnum):
    LOW_BANDWIDTH = 0
    HIGH_BANDWIDTH = 1


# ----- MESSAGES ----- #
class Serializer:
    """Generates serialized messages for the 'outbound_queue'.
    - Only message types that make sense for a client are implemented here."""

    def __init__(self, net_config: NetworkConfig) -> None:
        self.net_config = net_config

    # ----- ADD HEADER ----- #

    def payload_to_message(self, command: bytes, payload: bytes) -> bytes:
        magic = cast(bytes, int_to_be_bytes(self.net_config.MAGIC))
        length = cast(bytes, pack_le_uint32(len(payload)))
        checksum = payload_to_checksum(payload)
        return magic + command + length + checksum + payload

    # ----- MAKE PAYLOAD ----- #

    def version(
            self,
            recv_host: str,
            send_host: str,
            recv_port: int = 8333,
            send_port: int = 8333,
            version: int = 70016,
            relay: int = 1,
            user_agent: str = "",
            height: int = 0
    ) -> bytes:
        version = pack_le_uint32(version)
        services = pack_le_uint64(0)
        timestamp = pack_le_int64(int(time.time()))
        addr_recv = services + ipv4_to_mapped_ipv6(recv_host) + pack_be_uint16(recv_port)
        addr_sndr = services + ipv4_to_mapped_ipv6(send_host) + pack_be_uint16(send_port)
        nonce = pack_le_uint64(random.getrandbits(64))
        user_agent = pack_varbytes(user_agent.encode())
        height = pack_le_int32(height)
        relay = pack_byte(relay)
        payload = version + services + timestamp + addr_recv + addr_sndr + nonce + user_agent + height + relay
        return self.payload_to_message(VERSION_BIN, payload)

    def verack(self) -> bytes:
        return self.payload_to_message(VERACK_BIN, b"")

    def tx(self, rawtx: bytes) -> bytes:
        return self.payload_to_message(TX_BIN, rawtx)

    async def inv(self, inv_vects: List[Inv]) -> bytes:
        payload = bytearray()
        payload += pack_varint(len(inv_vects))
        for inv_vect in inv_vects:
            payload += pack_le_uint32(inv_vect["inv_type"])
            payload += hex_str_to_hash(inv_vect["inv_hash"])
        return self.payload_to_message(INV_BIN, bytes(payload))

    def getdata(self, inv_vects: List[Inv]) -> bytes:
        payload = bytearray()
        count = len(inv_vects)
        payload += pack_varint(count)
        for inv_vect in inv_vects:
            payload += pack_le_uint32(inv_vect["inv_type"])
            payload += hex_str_to_hash(inv_vect["inv_hash"])
        return self.payload_to_message(GETDATA_BIN, bytes(payload))

    def getheaders(
            self,
            hash_count: int,
            block_locator_hashes: List[bytes],
            hash_stop: bytes = ZERO_HASH,
    ) -> bytes:
        version = pack_le_uint32(70016)
        hash_count = pack_varint(hash_count)
        hashes = bytearray()
        for _hash in block_locator_hashes:
            hashes += _hash
        payload = version + hash_count + hashes + hash_stop
        return self.payload_to_message(GETHEADERS_BIN, payload)

    def getblocks(
            self,
            hash_count: int,
            block_locator_hashes: List[bytes],
            hash_stop: bytes = ZERO_HASH,
    ) -> bytes:
        assert hash_count == len(block_locator_hashes)
        version = pack_le_uint32(70016)
        hash_count = pack_varint(hash_count)
        hashes = bytearray()
        for _hash in block_locator_hashes:
            hashes += _hash
        payload = version + hash_count + hashes + hash_stop
        return self.payload_to_message(GETBLOCKS_BIN, payload)

    def getaddr(self) -> bytes:
        return self.payload_to_message(GETADDR_BIN, b"")

    def mempool(self) -> bytes:
        return self.payload_to_message(MEMPOOL_BIN, b"")

    def ping(self) -> bytes:
        return self.payload_to_message(PING_BIN, urandom(8))

    def pong(self, nonce: bytes) -> bytes:
        return self.payload_to_message(PONG_BIN, nonce)

    def filterclear(self) -> bytes:
        return self.payload_to_message(FILTERCLEAR_BIN, b"")

    def filterload(self, n_elements: int, false_positive_rate: int) -> bytes:  # incomplete...
        """Two parameters that need to be chosen. One is the size of the filter in bytes. The other
        is the number of hash functions to use. See bip37."""
        filter_size = calc_bloom_filter_size(n_elements, false_positive_rate)
        n_hash_functions = filter_size * 8 / n_elements * math.log(2)
        assert filter_size <= 36000, "bloom filter size must not exceed 36000 bytes."
        filter = b"00" * filter_size
        return self.payload_to_message(FILTERLOAD_BIN, b"")

    def merkleblock(self) -> None:
        raise NotImplementedError()

    def getblocktxn(self) -> None:
        raise NotImplementedError()

    def sendheaders(self) -> None:
        raise NotImplementedError()

    def sendcmpct(self, mode: int = CompactBlockMode.LOW_BANDWIDTH) -> bytes:
        payload = pack_byte(mode) + pack_le_uint64(1)
        return self.payload_to_message(SENDCMPCT_BIN, payload)
