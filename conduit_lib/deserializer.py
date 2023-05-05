import logging
from typing import cast

from bitcoinx import (
    hash_to_hex_str,
    Header,
    read_le_int32,
    read_le_uint64,
    read_le_int64,
    read_varbytes,
    read_le_uint32,
    read_varint,
    read_le_uint16,
    unpack_header,
    Tx,
)
import socket
import time

from .deserializer_types import (
    Inv,
    NodeAddr,
    NodeAddrListItem,
    SendCmpct,
    BlockLocator,
)
from .constants import CCODES
from .utils import mapped_ipv6_to_ipv4
from conduit_lib.deserializer_types import MessageHeader, Protoconf, Version
from conduit_lib.networks import NetworkConfig
from io import BytesIO

logger = logging.getLogger("deserializer")


class Deserializer:
    def __init__(self, net_config: NetworkConfig) -> None:
        self.net_config = net_config

    def deserialize_message_header(self, stream: BytesIO) -> MessageHeader:
        magic = stream.read(4).hex()
        command = stream.read(12).decode("ascii").strip("\x00")
        length = read_le_uint32(stream.read)
        checksum = stream.read(4).hex()
        decoded_header: MessageHeader = {
            "magic": magic,
            "command": command,
            "length": length,
            "checksum": checksum,
        }
        return decoded_header

    def deserialize_extended_message_header(self, stream: BytesIO) -> MessageHeader:
        magic = stream.read(4).hex()
        command = stream.read(12).decode("ascii").strip("\x00")
        length = read_le_uint32(stream.read)
        checksum = stream.read(4).hex()
        assert command == "extmsg"
        assert length == 0xFFFFFFFF
        assert checksum == "00000000"
        ext_command = stream.read(12).decode("ascii").strip("\x00")
        ext_length = read_le_uint64(stream.read)
        decoded_header: MessageHeader = {
            "magic": magic,
            "command": ext_command,
            "length": ext_length,
            "checksum": checksum,
        }
        return decoded_header

    def version(self, f: BytesIO) -> Version:
        version = read_le_int32(f.read)
        services = read_le_uint64(f.read)
        timestamp = time.ctime(read_le_int64(f.read))
        addr_recv = mapped_ipv6_to_ipv4(f)
        addr_from = mapped_ipv6_to_ipv4(f)
        nonce = read_le_uint64(f.read)
        user_agent = read_varbytes(f.read).decode("utf-8")
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
            port = read_le_uint16(f.read)
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

    def reject(self, f: BytesIO) -> tuple[str, str, str]:
        message = read_varbytes(f.read).decode("utf-8")
        ccode = f.read(1)
        reason = read_varbytes(f.read).decode("utf-8")
        # TODO different ccodes will / won't have "extra data"
        # data = read_varbytes(f.read) # no data

        ccode_translation = CCODES["0x" + ccode.hex()]
        return message, ccode_translation, reason

    def inv(self, f: BytesIO) -> list[Inv]:
        inv_vector = []
        count = read_varint(f.read)
        for i in range(count):
            inv_type = read_le_uint32(f.read)
            inv_hash = hash_to_hex_str(f.read(32))
            inv: Inv = {"inv_type": inv_type, "inv_hash": inv_hash}
            inv_vector.append(inv)
        return inv_vector

    def headers(self, f: BytesIO) -> list[Header]:
        count = read_varint(f.read)
        headers = []
        for i in range(count):
            raw_header = f.read(80)
            _tx_count = read_varint(f.read)
            headers.append(unpack_header(raw_header))
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
