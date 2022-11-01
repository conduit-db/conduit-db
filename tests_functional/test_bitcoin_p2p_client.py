import asyncio
import bitcoinx
from bitcoinx import double_sha256, hash_to_hex_str, pack_varint
import shutil
import stat
import time
import io
import unittest
from math import ceil
from pathlib import Path
from typing import cast, Callable
import unittest.mock
import electrumsv_node
import pytest
import logging
import os

from conduit_lib.algorithms import unpack_varint
from conduit_lib.bitcoin_p2p_types import BlockChunkData, BlockDataMsg, BitcoinPeerInstance, \
    BlockType
from conduit_lib.commands import INV, BLOCK_BIN
from conduit_lib.constants import REGTEST, ZERO_HASH
from conduit_lib import commands
from conduit_lib.handlers import MessageHandlerProtocol
from conduit_lib import BitcoinP2PClient, NetworkConfig, Serializer, Deserializer
from conduit_lib.utils import create_task
from contrib.scripts.import_blocks import import_blocks

from .data.big_data_carrier_tx import DATA_CARRIER_TX

MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
os.environ['GENESIS_ACTIVATION_HEIGHT'] = "0"
os.environ['NETWORK_BUFFER_SIZE'] = "1000000"
os.environ['DATADIR'] = str(MODULE_DIR)
BIG_BLOCK_WRITE_DIRECTORY = Path(os.environ["DATADIR"]) / "big_blocks"
os.makedirs(BIG_BLOCK_WRITE_DIRECTORY, exist_ok=True)



logger = logging.getLogger("bitcoin-p2p-socket")
logger.setLevel(logging.DEBUG)

REGTEST_NODE_HOST = "127.0.0.1"
REGTEST_NODE_PORT = 18444



class MockHandlers(MessageHandlerProtocol):
    """The intent here is to make assertions that the appropriate handlers are being called and in
    the right way.

    `got_message_queue` would not be a part of this handler class under normal circumstances"""

    net_config = NetworkConfig(REGTEST, REGTEST_NODE_HOST, REGTEST_NODE_PORT)
    serializer = Serializer(net_config)
    deserializer = Deserializer(net_config)

    def __init__(self, got_message_queue: asyncio.Queue) -> None:
        self.got_message_queue = got_message_queue

    async def on_version(self, message: bytes, peer: BitcoinPeerInstance) -> None:
        version = self.deserializer.version(io.BytesIO(message))
        logger.debug(f"Got version: {version}")
        verack_message = self.serializer.verack()
        self.got_message_queue.put_nowait((commands.VERSION, message))
        await peer.send_message(verack_message)

    async def on_verack(self, message: bytes, peer: BitcoinPeerInstance) -> None:
        ping_message = self.serializer.ping()
        await peer.send_message(ping_message)
        self.got_message_queue.put_nowait((commands.VERACK, message))

    async def on_protoconf(self, message: bytes, peer: BitcoinPeerInstance) -> None:
        self.got_message_queue.put_nowait((commands.PROTOCONF, message))

    async def on_sendheaders(self, message: bytes, peer: BitcoinPeerInstance) -> None:
        self.got_message_queue.put_nowait((commands.SENDHEADERS, message))

    async def on_sendcmpct(self, message: bytes, peer: BitcoinPeerInstance) -> None:
        self.got_message_queue.put_nowait((commands.SENDCMPCT, message))

    async def on_ping(self, message: bytes, peer: BitcoinPeerInstance) -> None:
        pong_message = self.serializer.pong(message)
        await peer.send_message(pong_message)
        self.got_message_queue.put_nowait((commands.PING, message))

    async def on_pong(self, message: bytes, peer: BitcoinPeerInstance) -> None:
        self.got_message_queue.put_nowait((commands.PONG, message))

    async def on_addr(self, message: bytes, peer: BitcoinPeerInstance) -> None:
        self.got_message_queue.put_nowait((commands.ADDR, message))

    async def on_feefilter(self, message: bytes, peer: BitcoinPeerInstance) -> None:
        self.got_message_queue.put_nowait((commands.FEEFILTER, message))

    async def on_inv(self, message: bytes, peer: BitcoinPeerInstance) -> None:
        inv_vect = self.deserializer.inv(io.BytesIO(message))
        tx_inv_vect = []
        block_inv_vect = []
        for inv in inv_vect:
            # TX
            if inv["inv_type"] == 1:
                tx_inv_vect.append(inv)

            # BLOCK
            elif inv["inv_type"] == 2:
                block_inv_vect.append(inv)


        if block_inv_vect:
            max_getdata_size = 50_000
            num_getdatas = ceil(len(block_inv_vect) / max_getdata_size)
            for i in range(num_getdatas):
                getdata_msg = self.serializer.getdata(block_inv_vect[i:(i+1)*max_getdata_size])
                await peer.send_message(getdata_msg)

        if tx_inv_vect:
            max_getdata_size = 50_000
            num_getdatas = ceil(len(tx_inv_vect) / max_getdata_size)
            for i in range(num_getdatas):
                getdata_msg = self.serializer.getdata(tx_inv_vect[i:(i+1)*max_getdata_size])
                await peer.send_message(getdata_msg)

        self.got_message_queue.put_nowait((commands.INV, message))

    async def on_getdata(self, message: bytes, peer: BitcoinPeerInstance) -> None:
        self.got_message_queue.put_nowait((commands.GETDATA, message))

    async def on_headers(self, message: bytes, peer: BitcoinPeerInstance) -> None:
        self.got_message_queue.put_nowait((commands.HEADERS, message))

    async def on_tx(self, rawtx: bytes, peer: BitcoinPeerInstance) -> None:
        self.got_message_queue.put_nowait((commands.TX, rawtx))

    async def on_block_chunk(self, block_chunk_data: BlockChunkData, peer: BitcoinPeerInstance) -> None:
        self.got_message_queue.put_nowait((commands.BLOCK, block_chunk_data))

    async def on_block(self, block_data_msg: BlockDataMsg, peer: BitcoinPeerInstance) -> None:
        self.got_message_queue.put_nowait((commands.BLOCK, block_data_msg))


async def _drain_handshake_messages(client: BitcoinP2PClient, message_handler: MockHandlers):
    expected_message_commands = {commands.VERSION, commands.VERACK, commands.PING, commands.PONG,
        commands.PROTOCONF, commands.SENDHEADERS, commands.SENDCMPCT}
    for i in range(len(expected_message_commands)):
        command, message = await message_handler.got_message_queue.get()
        expected_message_commands.remove(command)
    assert len(expected_message_commands) == 0
    assert client.handshake_complete_event.is_set()


def setup_module(module) -> None:
    blockchain_dir = MODULE_DIR.parent / "contrib" / "blockchains" / "blockchain_116_7c9cd2"
    import_blocks(str(blockchain_dir))
    time.sleep(5)


def remove_readonly(func: Callable[[Path], None], path: Path,
        excinfo: BaseException | None) -> None:
    os.chmod(path, stat.S_IWRITE)
    func(path)


def teardown_module(module) -> None:
    shutil.rmtree(BIG_BLOCK_WRITE_DIRECTORY, onerror=remove_readonly)


@pytest.mark.asyncio
async def test_handshake():
    client = None
    try:
        got_message_queue = asyncio.Queue()
        message_handler = MockHandlers(got_message_queue)
        net_config = NetworkConfig(REGTEST, REGTEST_NODE_HOST, REGTEST_NODE_PORT)
        client = BitcoinP2PClient(REGTEST_NODE_HOST, REGTEST_NODE_PORT, message_handler,
            net_config)
        await client.connect()
        await client.handshake("127.0.0.1", 18444)

        expected_message_commands = { commands.VERSION, commands.VERACK, commands.PING,
            commands.PONG, commands.PROTOCONF, commands.SENDHEADERS, commands.SENDCMPCT }
        for i in range(len(expected_message_commands)):
            command, message = await message_handler.got_message_queue.get()
            expected_message_commands.remove(command)
        assert len(expected_message_commands) == 0
        assert client.handshake_complete_event.is_set()
    finally:
        if client:
            await client.close_connection()


@pytest.mark.asyncio
async def test_getheaders_request_and_headers_response():
    # Otherwise the node might still be in initial block download mode (ignores header requests)
    electrumsv_node.call_any('generate', 1)

    client = None
    try:
        got_message_queue = asyncio.Queue()
        message_handler = MockHandlers(got_message_queue)
        net_config = NetworkConfig(REGTEST, REGTEST_NODE_HOST, REGTEST_NODE_PORT)
        client = BitcoinP2PClient(REGTEST_NODE_HOST, REGTEST_NODE_PORT, message_handler,
            net_config)
        await client.connect()
        await client.handshake("127.0.0.1", 18444)

        await _drain_handshake_messages(client, message_handler)

        serializer = Serializer(net_config)
        message = serializer.getheaders(1, block_locator_hashes=[ZERO_HASH], hash_stop=ZERO_HASH)
        await client.send_message(message)

        deserializer = Deserializer(net_config)
        command, message = await message_handler.got_message_queue.get()
        headers = deserializer.headers(io.BytesIO(message))
        node_rpc_result = electrumsv_node.call_any('getinfo').json()['result']
        height = node_rpc_result['blocks']
        assert len(headers) == height
        headers_count, offset = unpack_varint(message, 0)
        assert headers_count == len(headers)
    finally:
        if client:
            await client.close_connection()


@pytest.mark.asyncio
async def test_getblocks_request_and_blocks_response():
    client = None
    try:
        got_message_queue = asyncio.Queue()
        message_handler = MockHandlers(got_message_queue)
        net_config = NetworkConfig(REGTEST, REGTEST_NODE_HOST, REGTEST_NODE_PORT)
        client = BitcoinP2PClient(REGTEST_NODE_HOST, REGTEST_NODE_PORT, message_handler,
            net_config)
        await client.connect()
        await client.handshake("127.0.0.1", 18444)

        await _drain_handshake_messages(client, message_handler)

        serializer = Serializer(net_config)
        message = serializer.getblocks(1, block_locator_hashes=[ZERO_HASH], hash_stop=ZERO_HASH)
        await client.send_message(message)

        deserializer = Deserializer(net_config)

        node_rpc_result = electrumsv_node.call_any('getinfo').json()['result']
        height = node_rpc_result['blocks']

        count_blocks_received = 0
        expected_message_count = height + 1  # +1 for inv message
        for count in range(expected_message_count):
            command, message = await message_handler.got_message_queue.get()
            if command == INV:
                continue

            message = cast(BlockDataMsg, message)
            raw_header = message.small_block_data[0:80]
            block_hash = double_sha256(raw_header)
            node_rpc_result = electrumsv_node.call_any('getblock', hash_to_hex_str(block_hash)).json()['result']

            header, block_txs = deserializer.block(io.BytesIO(message.small_block_data))
            assert node_rpc_result['num_tx'] == len(block_txs)
            count_blocks_received += 1
            txs_count, offset = unpack_varint(message.small_block_data[80:89], 0)
            assert txs_count == len(block_txs)

        assert count_blocks_received == height
    finally:
        if client:
            await client.close_connection()


def _parse_txs_with_bitcoinx(message: BlockChunkData) -> None:
    correct_tx_hash = bitcoinx.double_sha256(bytes.fromhex(DATA_CARRIER_TX))
    size_data_carrier_tx = len(bytes.fromhex(DATA_CARRIER_TX))
    normalizer = 0
    if message.chunk_num != 1:
        normalizer = message.tx_offsets_for_chunk[0]
    for i in range(len(message.tx_offsets_for_chunk)):
        if i < len(message.tx_offsets_for_chunk) - 1:
            offset_start = message.tx_offsets_for_chunk[i] - normalizer
            offset_end = message.tx_offsets_for_chunk[i + 1] - normalizer
            tx = bitcoinx.Tx.from_bytes(message.raw_block_chunk[offset_start:offset_end])
        else:
            offset_start = message.tx_offsets_for_chunk[i] - normalizer
            tx = bitcoinx.Tx.from_bytes(message.raw_block_chunk[offset_start:])
        print(len(tx.to_bytes()))
        assert len(tx.to_bytes()) == size_data_carrier_tx
        assert tx.hash() == correct_tx_hash


@pytest.mark.asyncio
async def test_big_block_exceeding_network_buffer_capacity():
    os.environ['NETWORK_BUFFER_SIZE'] = "500000"
    client = None
    task = None
    try:
        net_config = NetworkConfig(REGTEST, REGTEST_NODE_HOST, REGTEST_NODE_PORT)
        serializer = Serializer(net_config)
        fake_header_block_116 = bytes.fromhex("000000201b94e4366e4d283d1cd3834aed03b4fd0be15fcc6ab4e387df04f08ddff47736bc86ff7435135f70a33a9105551b0ea7719b9fb2c0a7e882976b3b977985adab2189f461ffff7f2001000000")
        block_hash = double_sha256(fake_header_block_116)
        logger.debug(f"Expected block_hash: {hash_to_hex_str(block_hash)}")
        size_data_carrier_tx = len(bytes.fromhex(DATA_CARRIER_TX))
        big_block = bytearray(fake_header_block_116)
        tx_count_to_exceed_buffer = ceil(1000000 / size_data_carrier_tx)
        big_block += pack_varint(tx_count_to_exceed_buffer)
        for i in range(tx_count_to_exceed_buffer):
            big_block += bytes.fromhex(DATA_CARRIER_TX)

        message_to_send = serializer.payload_to_message(BLOCK_BIN, big_block)
        got_message_queue = asyncio.Queue()

        message_handler = MockHandlers(got_message_queue)

        client = BitcoinP2PClient(REGTEST_NODE_HOST, REGTEST_NODE_PORT, message_handler,
            net_config)
        client.reader = asyncio.StreamReader()
        client.reader.feed_data(message_to_send)
        client.writer = unittest.mock.Mock()
        client.peer = unittest.mock.Mock()

        task = create_task(client.start_session())
        expected_msg_count = 3  # 2 x BlockChunkData; 1 x BlockDataMsg for the full block
        msg_count = 0
        for i in range(expected_msg_count):
            command, message = await message_handler.got_message_queue.get()
            msg_count += 1
            logger.debug(f"Got '{command}' message_type: {type(message)}")
            if msg_count == 1:
                assert isinstance(message, BlockChunkData)
                assert message.chunk_num == 1
                assert message.num_chunks == 3
                assert message.block_hash == block_hash
                assert len(message.raw_block_chunk) == 460317
                assert message.tx_offsets_for_chunk.tolist() == [81, 65829, 131577, 197325, 263073,
                    328821, 394569]

                _parse_txs_with_bitcoinx(message)

            if msg_count == 2:
                assert isinstance(message, BlockChunkData)
                assert message.chunk_num == 2
                assert message.num_chunks == 3
                assert message.block_hash == block_hash

                # This chunk should be 525984 bytes due to the remainder of the previous chunk
                assert len(message.raw_block_chunk) == (986301 - 460317)
                assert message.tx_offsets_for_chunk.tolist() == [460317, 526065, 591813, 657561, 723309,
                    789057, 854805, 920553]

                _parse_txs_with_bitcoinx(message)

            if msg_count == 3:
                assert isinstance(message, BlockChunkData)
                assert message.chunk_num == 3
                assert message.num_chunks == 3
                assert message.block_hash == block_hash
                assert len(message.raw_block_chunk) == 65748
                assert message.tx_offsets_for_chunk.tolist() == [986301]

                _parse_txs_with_bitcoinx(message)

            if msg_count == 4:
                assert isinstance(message, BlockDataMsg)
                assert message.block_type == BlockType.BIG_BLOCK
                assert message.block_hash == block_hash
                assert message.tx_offsets.tolist() == [81, 65829, 131577, 197325, 263073, 328821, 394569,
                    460317, 526065, 591813, 657561, 723309, 789057, 854805, 920553, 986301]
                assert message.block_size == 1052049
                assert message.small_block_data == None
                with open(message.big_block_filepath, 'rb') as file:
                    raw_block = file.read()
                assert raw_block == big_block

    finally:
        if client:
            await client.close_connection()
