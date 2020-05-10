import hashlib
import ipaddress
import math
import socket
import struct
from binascii import hexlify
from typing import Tuple
from bitcoinx import (
    read_le_uint64,
    read_be_uint16,
    double_sha256,
    int_to_be_bytes,
    pack_le_uint32,
)

from commands import BLOCK


def payload_to_checksum(payload):
    return double_sha256(payload)[:4]


def payload_to_message(command, payload):
    MAGIC = 0xE3E1F3E8
    magic = int_to_be_bytes(MAGIC)
    length = pack_le_uint32(len(payload))
    checksum = payload_to_checksum(payload)
    return magic + command + length + checksum + payload


def is_block_msg(command: bytes):
    return command == BLOCK


def pack_null_padded_ascii(string, num_bytes):
    return struct.pack("%ss" % num_bytes, string.encode("ASCII"))


def ipv4_to_mapped_ipv6(ipv4: str) -> bytes:
    return bytes(10) + bytes.fromhex("ffff") + ipaddress.IPv4Address(ipv4).packed


def mapped_ipv6_to_ipv4(f):
    services = read_le_uint64(f.read)
    reserved = f.read(12)
    ipv4 = socket.inet_ntoa(f.read(4))
    port = read_be_uint16(f.read)
    return services, ipv4, port


def bytes_to_hex(bytestr, upper=False):
    hexed = hexlify(bytestr).decode()
    return hexed.upper() if upper else hexed


def hex_to_bytes(hexed):

    if len(hexed) & 1:
        hexed = "0" + hexed

    return bytes.fromhex(hexed)


def get_block_hash(header_bin):
    """takes in bytes outputs hex string"""
    _hash = hashlib.sha256(hashlib.sha256(header_bin).digest()).digest()
    return bytes(reversed(_hash)).hex()


def int_to_hex(num, upper=False):
    """Ensures that there is an even number of characters in the hex string"""
    hexed = hex(num)[2:]
    if len(hexed) % 2 != 0:
        hexed = "0" + hexed
    return hexed.upper() if upper else hexed


def flip_hex_byte_order(string):
    return bytes_to_hex(hex_to_bytes(string)[::-1])


def calc_bloom_filter_size(n_elements, false_positive_rate):
    """two parameters that need to be chosen. One is the size of the filter in bytes. The other
        is the number of hash functions to use. See bip37."""
    filter_size = (
        -1 / math.pow(math.log(2), 2) * n_elements * math.log(false_positive_rate)
    ) / 8
    return filter_size


def read_varint(buffer) -> Tuple[int, int]:
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
