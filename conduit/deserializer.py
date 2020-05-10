import io
import logging
import time
import socket

import bitcoinx
from bitcoinx import (
    read_le_int32,
    read_le_uint64,
    read_le_int64,
    read_varbytes,
    read_le_uint32,
    read_varint,
    read_le_uint16,
    hash_to_hex_str,
)

import utils
from constants import CCODES, LOGGING_FORMAT, HEADER_LENGTH
from networks import NetworkConfig
from utils import mapped_ipv6_to_ipv4

logging.basicConfig(
    format=LOGGING_FORMAT, level=logging.DEBUG, datefmt="%Y-%m-%d %H-%M-%S"
)
logger = logging.getLogger("deserializer")


class Deserializer:
    def __init__(self, net_config: NetworkConfig):
        self.net_config = net_config

    def deserialize_message_header(self, stream):
        magic = utils.bytes_to_hex(stream.read(4))
        command = stream.read(12).decode("ascii").strip("\x00")
        length = read_le_uint32(stream.read)
        checksum = utils.bytes_to_hex(stream.read(4))
        decoded_header = {
            "magic": magic,
            "command": command,
            "length": length,
            "checksum": checksum,
        }
        return decoded_header

    def version(self, f):
        version = read_le_int32(f.read)
        services = read_le_uint64(f.read)
        timestamp = time.ctime(read_le_int64(f.read))
        addr_recv = mapped_ipv6_to_ipv4(f)
        addr_from = mapped_ipv6_to_ipv4(f)
        nonce = read_le_uint64(f.read)
        user_agent = read_varbytes(f.read)
        start_height = read_le_int32(f.read)
        relay = f.read(1)

        return {
            "version": version,
            "services": services,
            "timestamp": timestamp,
            "addr_recv": addr_recv,
            "addr_from": addr_from,
            "nonce": nonce,
            "user_agent": user_agent,
            "start_height": start_height,
            "relay": relay,
        }

    def verack(self, f):
        return {}  # No payload

    def protoconf(self, f):
        number_of_fields = read_varint(f.read)
        max_recv_payload_length = read_le_uint32(f.read)
        return {
            "number_of_fields": number_of_fields,
            "max_recv_payload_length": max_recv_payload_length,
        }

    def sendheaders(self, f):
        return  # No payload

    def ping(self, f):
        nonce = read_le_uint64(f.read)
        return nonce

    def pong(self, f):
        nonce = read_le_uint64(f.read)
        return nonce

    def addr(self, f):
        count = read_varint(f.read)
        addresses = []
        for i in range(count):
            timestamp = time.ctime(read_le_uint32(f.read))
            services = read_le_uint64(f.read)
            reserved = f.read(12)  # IPv6
            IPv4 = socket.inet_ntoa(f.read(4))
            port = read_le_uint16(f.read)
            addresses.append(
                {
                    "timestamp": timestamp,
                    "services": services,
                    "IPv4": IPv4,
                    "port": port,
                }
            )
        return addresses  # count not returned by choice

    def sendcmpct(self, f):
        boolean = f.read(1)
        version = read_le_int64(f.read)
        return  # ignore

    def feefilter(self, f):
        feerate = read_le_int64(f.read)
        return

    def reject(self, f):
        message = read_varbytes(f.read)
        ccode = f.read(1)
        reason = read_varbytes(f.read)
        # TODO different ccodes will / won't have "extra data"
        # data = read_varbytes(f.read) # no data

        ccode_translation = CCODES["0x" + utils.bytes_to_hex(ccode)]

        return message, ccode_translation, reason

    def inv(self, f):
        message = []
        count = read_varint(f.read)
        for i in range(count):
            inv_type = read_le_uint32(f.read)
            inv_hash = hash_to_hex_str(f.read(32))
            inv_vector = {"inv_type": inv_type, "inv_hash": inv_hash}
            message.append(inv_vector)
        return message

    def getdata(self, f):
        message = []
        count = read_varint(f.read)
        for i in range(count):
            inv_type = read_le_uint32(f.read)
            inv_hash = hash_to_hex_str(f.read(32))
            inv_vector = {"inv_type": inv_type, "inv_hash": inv_hash}
            message.append(inv_vector)
        return message

    def getheaders(self, f):
        """for checking my own getheaders request"""
        version = read_le_uint32(f.read)
        hash_count = read_varint(f.read)
        block_locator_hashes = []
        for i in range(hash_count):
            block_locator_hashes.append(hash_to_hex_str(f.read(32)))
        hash_stop = hash_to_hex_str(f.read(32))

        message = {
            "version": version,
            "hash_count": hash_count,
            "block_locator_hashes": block_locator_hashes,
            "hash_stop": hash_stop,
        }
        return message

    def headers(self, f):
        """deserialize block headers into a list of dicts"""
        lst_headers = []
        headers_stream = io.BytesIO()
        hashes_stream = io.BytesIO()
        # Store headers temporarily to memory as binary stream
        headers_stream.seek(0)
        headers_stream.write(f.read())

        # make a list of block hashes for validating
        headers_stream.seek(0)
        count = read_varint(headers_stream.read)  # count of headers
        for i in range(count):
            header = headers_stream.read(80)  # minus final txn count (1 byte)
            headers_stream.read(1)  # discard txn count
            _hash = utils.get_block_hash(
                header
            )  # calculates hash as part of validation
            hashes_stream.write(_hash + "\n")

        f.seek(0)
        number_headers = read_varint(f.read)
        for i in range(number_headers):
            # TODO make into single function call for readability and reuse
            version = read_le_int32(f.read)
            prev_block = hash_to_hex_str(f.read(32))
            merkle_root = hash_to_hex_str(f.read(32))
            timestamp = read_le_uint32(f.read)
            bits = utils.int_to_hex(read_le_uint32(f.read))
            nonce = read_le_uint32(f.read)
            txn_count = read_varint(f.read)

            block_header = {
                "version": version,
                "prev_block_hash": prev_block,
                "merkle_root": merkle_root,
                "timestamp": timestamp,
                "bits": bits,
                "nonce": nonce,
                "txn_count": txn_count,
            }

            lst_headers.append(block_header)

        return lst_headers

    def tx(self, f):
        return bitcoinx.Tx.read(f.read)

    def getblocks(self, buffer_view: memoryview):
        """earliest, naive implementation!"""
        raw_block_header = bitcoinx.unpack_header(buffer_view[0:HEADER_LENGTH])
        logger.debug("raw_block_header = %s", raw_block_header)
        tx_count, offset = bitcoinx.read_varint(buffer_view[80:])
        stream: io.BytesIO = io.BytesIO.read(buffer_view[80 + offset :])
        txs = []
        stream.seek(0)
        for i in range(tx_count):
            txs.append(bitcoinx.Tx.read(stream.read))
        return txs
