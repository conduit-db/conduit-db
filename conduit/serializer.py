"""https://en.bitcoin.it/wiki/Protocol_documentation#Message_structure"""
import logging
import math
import random
import time
from typing import List, Dict

from bitcoinx import (
    pack_le_uint32,
    pack_varint,
    pack_le_uint64,
    pack_le_int64,
    pack_varbytes,
    hex_str_to_hash,
    int_to_be_bytes,
    pack_be_uint16,
    pack_byte,
)

from commands import (
    VERSION_BIN,
    VERACK_BIN,
    GETADDR_BIN,
    FILTERCLEAR_BIN,
    GETHEADERS_BIN,
    FILTERLOAD,
    INV_BIN,
    TX_BIN,
    GETDATA_BIN,
)
from networks import NetworkConfig
from store import Storage
from utils import (
    payload_to_checksum,
    ipv4_to_mapped_ipv6,
    flip_hex_byte_order,
    calc_bloom_filter_size,
    hex_to_bytes,
)
from constants import LOGGING_FORMAT, ZERO_HASH

logging.basicConfig(
    format=LOGGING_FORMAT, level=logging.DEBUG, datefmt="%Y-%m-%d %H-%M-%S"
)
logger = logging.getLogger("serializer")

# ----- MESSAGES ----- #
class Serializer:
    """Generates serialized messages for the 'outbound_queue'.
    - Only message types that make sense for a client are implemented here."""

    def __init__(self, net_config: NetworkConfig, storage: Storage):
        self.net_config = net_config
        self.storage = storage

    # ----- ADD HEADER ----- #

    def payload_to_message(self, command, payload):
        magic = int_to_be_bytes(self.net_config.MAGIC)
        length = pack_le_uint32(len(payload))
        checksum = payload_to_checksum(payload)
        return magic + command + length + checksum + payload

    # ----- MAKE PAYLOAD ----- #

    def version(
        self,
        recv_host: str,
        send_host: str,
        recv_port: int = 8333,
        send_port: int = 8333,
        version=70015,
        relay=1,
    ):
        version = pack_le_uint32(version)
        services = pack_le_uint64(0)
        timestamp = pack_le_int64(int(time.time()))
        addr_recv = (
            services + ipv4_to_mapped_ipv6(recv_host) + pack_be_uint16(recv_port)
        )
        addr_sndr = (
            services + ipv4_to_mapped_ipv6(send_host) + pack_be_uint16(send_port)
        )
        nonce = pack_le_uint64(random.getrandbits(64))
        user_agent = pack_varbytes(b"")
        height = pack_le_uint32(0)
        relay = pack_byte(relay)
        payload = (
            version
            + services
            + timestamp
            + addr_recv
            + addr_sndr
            + nonce
            + user_agent
            + height
            + relay
        )
        return self.payload_to_message(VERSION_BIN, payload)

    def verack(self):
        return self.payload_to_message(VERACK_BIN, b"")

    def tx(self, rawtx: str):
        payload = hex_to_bytes(rawtx)
        return self.payload_to_message(TX_BIN, payload)

    def inv(self, inv_vects: List[Dict]):
        payload = bytearray
        payload += pack_varint(len(inv_vects))
        for inv_vect in inv_vects:
            payload += pack_le_uint32(inv_vect["inv_type"])
            payload += hex_str_to_hash(inv_vect["inv_hash"])
        return self.payload_to_message(INV_BIN, payload)

    def getdata(self, inv_vects: List[Dict]):
        payload = bytearray()
        count = len(inv_vects)
        payload += pack_varint(count)
        for inv_vect in inv_vects:
            payload += pack_le_uint32(inv_vect["inv_type"])
            payload += hex_str_to_hash(inv_vect["inv_hash"])
        return self.payload_to_message(GETDATA_BIN, payload)

    def getheaders(
        self, hash_count: int, block_locator_hashes: List[bytes], hash_stop=ZERO_HASH
    ):
        version = pack_le_uint32(70015)
        hash_count = pack_varint(hash_count)
        hashes = bytearray()
        for _hash in block_locator_hashes:
            # hashes += hex_str_to_hash(flip_hex_byte_order(_hash))
            hashes += _hash
        payload = version + hash_count + hashes + hash_stop
        return self.payload_to_message(GETHEADERS_BIN, payload)

    def getaddr(self):
        return self.payload_to_message(GETADDR_BIN, b"")

    def mempool(self):
        return self.payload_to_message(GETADDR_BIN, b"")

    def ping(self):
        nonce = random.randint(0, 2 ** 64 - 1)
        return self.payload_to_message(GETADDR_BIN, pack_le_uint64(nonce))

    def pong(self):
        nonce = random.randint(0, 2 ** 64 - 1)
        return self.payload_to_message(GETADDR_BIN, pack_le_uint64(nonce))

    def filterclear(self):
        return self.payload_to_message(FILTERCLEAR_BIN, b"")

    def filterload(self, n_elements, false_positive_rate):  # incomplete...
        """two parameters that need to be chosen. One is the size of the filter in bytes. The other
        is the number of hash functions to use. See bip37."""
        filter_size = calc_bloom_filter_size(n_elements, false_positive_rate)
        n_hash_functions = filter_size * 8 / n_elements * math.log(2)
        assert filter_size <= 36000, "bloom filter size must not exceed 36000 bytes."
        filter = b"00" * filter_size
        return self.payload_to_message(FILTERLOAD, b"")

    def merkleblock(self):
        return NotImplementedError

    def getblocktxn(self):
        return NotImplementedError

    def sendheaders(self):
        return NotImplementedError

    def getblock(self):
        return NotImplementedError
