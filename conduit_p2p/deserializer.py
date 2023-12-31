# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.
import io
import logging
from typing import cast, TypedDict, NamedTuple
import socket
import time

from bitcoinx import (
    hash_to_hex_str,
    Header,
    read_le_int32,
    read_le_uint64,
    read_le_int64,
    read_varbytes,
    read_le_uint32,
    read_varint,
    unpack_header,
    read_be_uint16,
    double_sha256,
    Tx,
)

from .constants import CCODES
from .networks import NetworkConfig
from io import BytesIO

from .types import InvType, BlockHeader, Reject

logger = logging.getLogger("conduit.p2p.deserializer")


Hash256 = bytes


class MessageHeader(NamedTuple):
    magic: str
    command: str
    length: int
    checksum: str


class NodeAddr(NamedTuple):
    services: int
    ip: str
    port: int


def mapped_ipv6_to_ipv4(f: io.BytesIO) -> NodeAddr:
    services = read_le_uint64(f.read)
    reserved = f.read(12)
    ipv4 = socket.inet_ntoa(f.read(4))
    port = read_be_uint16(f.read)
    return NodeAddr(services, ipv4, port)


class SendCmpct(NamedTuple):
    enable: bool
    version: int


class NodeAddrListItem(NamedTuple):
    timestamp: str
    node_addr: NodeAddr


class Version(TypedDict):
    version: int
    services: str
    timestamp: str
    addr_recv: NodeAddr
    addr_from: NodeAddr
    nonce: int
    user_agent: str
    start_height: int
    relay: bool


class Protoconf(TypedDict):
    number_of_fields: int
    max_recv_payload_length: int


class Inv(TypedDict):
    inv_type: InvType
    inv_hash: str


class BlockLocator(NamedTuple):
    version: int
    block_locator_hashes: list[Hash256]
    hash_stop: Hash256


class Deserializer:
    def __init__(self, net_config: NetworkConfig) -> None:
        self.net_config = net_config

    def deserialize_message_header(self, stream: BytesIO) -> MessageHeader:
        magic = stream.read(4).hex()
        command = stream.read(12).decode("ascii").strip("\x00")
        length = read_le_uint32(stream.read)
        checksum = stream.read(4).hex()
        return MessageHeader(magic, command, length, checksum)

    def deserialize_extended_message_header(self, stream: BytesIO) -> MessageHeader:
        magic = stream.read(4).hex()
        command = stream.read(12).decode("ascii").strip("\x00")
        length = read_le_uint32(stream.read)
        checksum = stream.read(4).hex()
        assert command == "extmsg"
        assert length == 0xFFFFFFFF
        assert checksum == "00000000"
        command = stream.read(12).decode("ascii").strip("\x00")
        length = read_le_uint64(stream.read)
        return MessageHeader(magic, command, length, checksum)

    def version(self, f: BytesIO) -> Version:
        version = read_le_int32(f.read)
        services = read_le_uint64(f.read)
        timestamp = time.ctime(read_le_int64(f.read))
        addr_recv = mapped_ipv6_to_ipv4(f)
        addr_from = mapped_ipv6_to_ipv4(f)
        nonce = read_le_uint64(f.read)
        user_agent = read_varbytes(f.read).decode()
        start_height = read_le_int32(f.read)
        relay = cast(bool, f.read(1))
        return Version(
            version=version,
            services=services,
            timestamp=timestamp,
            addr_recv=addr_recv,
            addr_from=addr_from,
            nonce=nonce,
            user_agent=user_agent,
            start_height=start_height,
            relay=relay,
        )

    def verack(self, f: BytesIO) -> None:
        return None  # No payload

    def protoconf(self, f: BytesIO) -> Protoconf:
        number_of_fields = read_varint(f.read)
        max_recv_payload_length = read_le_uint32(f.read)
        return Protoconf(
            number_of_fields=number_of_fields,
            max_recv_payload_length=max_recv_payload_length,
        )

    def sendheaders(self, f: BytesIO) -> None:
        return  # No payload

    def ping(self, f: BytesIO) -> int:
        nonce: int = read_le_uint64(f.read)
        return nonce

    def pong(self, f: BytesIO) -> int:
        nonce: int = read_le_uint64(f.read)
        return nonce

    def addr(self, f: BytesIO) -> list[NodeAddrListItem]:
        count = read_varint(f.read)
        addresses = []
        for i in range(count):
            timestamp = time.ctime(read_le_uint32(f.read))
            services = read_le_uint64(f.read)
            reserved = f.read(12)  # IPv6
            ipv4 = socket.inet_ntoa(f.read(4))
            port = read_be_uint16(f.read)
            addresses.append(
                NodeAddrListItem(
                    timestamp=timestamp,
                    node_addr=NodeAddr(services, ipv4, port),
                )
            )
        return addresses

    def sendcmpct(self, f: BytesIO) -> SendCmpct:
        enable = cast(bool, f.read(1))
        version = read_le_int64(f.read)
        return SendCmpct(enable, version)

    def feefilter(self, f: BytesIO) -> int:
        feerate: int = read_le_int64(f.read)
        return feerate

    def reject(self, f: BytesIO) -> Reject:
        message = read_varbytes(f.read).decode()
        ccode = f.read(1)
        reason = read_varbytes(f.read).decode()
        ccode_translation = CCODES["0x" + ccode.hex()]
        item_hash = f.read(32)
        return Reject(message, ccode_translation, reason, hash_to_hex_str(item_hash))

    def inv(self, f: BytesIO) -> list[Inv]:
        inv_vector = []
        count = read_varint(f.read)
        for i in range(count):
            inv_type = read_le_uint32(f.read)
            inv_hash = hash_to_hex_str(f.read(32))
            inv: Inv = {"inv_type": inv_type, "inv_hash": inv_hash}
            inv_vector.append(inv)
        return inv_vector

    def headers(self, f: BytesIO) -> list[BlockHeader]:
        count = read_varint(f.read)
        headers = []
        for i in range(count):
            raw_header = f.read(80)
            _tx_count = read_varint(f.read)
            version, prev_block, merkle_root, timestamp, bits, nonce = unpack_header(raw_header)
            hash = double_sha256(raw_header)
            headers.append(BlockHeader(version, prev_block, merkle_root, timestamp, bits, nonce, raw_header, hash))
        return headers

    def getdata(self, f: BytesIO) -> list[Inv]:
        message = []
        count = read_varint(f.read)
        for i in range(count):
            inv_type = read_le_uint32(f.read)
            inv_hash = hash_to_hex_str(f.read(32))
            inv_vector = Inv(inv_type=inv_type, inv_hash=inv_hash)
            message.append(inv_vector)
        return message

    def getheaders(self, f: BytesIO) -> BlockLocator:
        """for checking my own getheaders request"""
        version = read_le_uint32(f.read)
        hash_count = read_varint(f.read)
        block_locator_hashes = []
        for i in range(hash_count):
            block_locator_hashes.append(hash_to_hex_str(f.read(32)))
        hash_stop = hash_to_hex_str(f.read(32))
        return BlockLocator(
            version=version,
            block_locator_hashes=block_locator_hashes,
            hash_stop=hash_stop,
        )

    def tx(self, f: BytesIO) -> Tx:
        return Tx.read(f.read)

    def block(self, f: BytesIO) -> tuple[Header, list[Tx]]:
        """This method is merely included for completion but is unlikely to be used"""
        raw_header = f.read(80)
        tx_count = read_varint(f.read)
        transactions = []
        for tx_pos in range(tx_count):
            transactions.append(Tx.read(f.read))
        return unpack_header(raw_header), transactions
