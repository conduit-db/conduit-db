import ipaddress
from ipaddress import IPv4Address
import logging
import socket

from bitcoinx import (
    Coin,
    CheckPoint,
    Bitcoin,
    BitcoinTestnet,
    BitcoinScalingTestnet,
    BitcoinRegtest,
    Headers,
    MissingHeader,
)
from typing import Optional, List

from .constants import MAINNET, TESTNET, SCALINGTESTNET, REGTEST
from .peers import Peer
from .utils import cast_to_valid_ipv4

logger = logging.getLogger("networks")


class HeadersRegTestMod(Headers):
    def connect(self, raw_header):
        """overwrite Headers method to skip checking of difficulty target"""
        header = self.coin.deserialized_header(raw_header, -1)
        prev_header, chain = self.lookup(header.prev_hash)
        header.height = prev_header.height + 1
        # If the chain tip is the prior header then this header is new.  Otherwise we must check.
        if chain.tip.hash != prev_header.hash:
            try:
                return self.lookup(header.hash)
            except MissingHeader:
                pass
        header_index = self._storage.append(raw_header)
        chain = self._read_header(header_index)
        return header, chain


class AbstractNetwork:
    NET = ""
    PUBKEY_HASH = 0x00
    PRIVATEKEY = 0x00
    SCRIPTHASH = 0x00
    XPUBKEY = 0x00000000
    XPRIVKEY = 0x00000000
    MAGIC = 0x00000000
    PORT = 0000
    DNS_SEEDS = [""]
    BITCOINX_COIN: Optional[Coin] = None
    CHECKPOINT: Optional[CheckPoint] = None


class MainNet(AbstractNetwork):
    NET = MAINNET
    PUBKEY_HASH = 0x00
    PRIVATEKEY = 0x80
    SCRIPTHASH = 0x05
    XPUBKEY = 0x0488B21E
    XPRIVKEY = 0x0488ADE4
    MAGIC = 0xE3E1F3E8
    PORT = 8333
    DNS_SEEDS = ["seed.bitcoinsv.io"]
    BITCOINX_COIN = Bitcoin

    # i.e. Genesis block
    CHECKPOINT = CheckPoint(
        bytes.fromhex(
            "010000000000000000000000000000000000000000000000000000000000000000000000"
            "3ba3edfd7a7b12b27ac72c3e67768f617fc81bc3888a51323a9fb8aa4b1e5e4a29ab5f49"
            "ffff001d1dac2b7c"
        ),
        height=0,
        prev_work=0,
    )


class TestNet(AbstractNetwork):
    NET = TESTNET
    PUBKEY_HASH = 0x6F
    PRIVATEKEY = 0xEF
    SCRIPTHASH = 0xC4
    XPUBKEY = 0x043587CF
    XPRIVKEY = 0x04358394
    MAGIC = 0xF4E5F3F4
    PORT = 18333
    DNS_SEEDS = ["testnet-seed.bitcoinsv.io"]
    BITCOINX_COIN = BitcoinTestnet

    # i.e. Genesis block
    CHECKPOINT = CheckPoint(
        bytes.fromhex(
            "010000000000000000000000000000000000000000000000000000000000000000000000"
            "3ba3edfd7a7b12b27ac72c3e67768f617fc81bc3888a51323a9fb8aa4b1e5e4adae5494"
            "dffff001d1aa4ae18"
        ),
        height=0,
        prev_work=0,
    )


class ScalingTestNet(AbstractNetwork):
    NET = SCALINGTESTNET
    PUBKEY_HASH = 0x6F
    PRIVATEKEY = 0xEF
    SCRIPTHASH = 0xC4
    XPUBKEY = 0x043587CF
    XPRIVKEY = 0x04358394
    MAGIC = 0xFBCEC4F9
    PORT = 9333
    DNS_SEEDS = ["stn-seed.bitcoinsv.io"]
    BITCOINX_COIN = BitcoinScalingTestnet

    # i.e. Genesis block
    CHECKPOINT = CheckPoint(
        bytes.fromhex(
            "010000000000000000000000000000000000000000000000000000000000000000000000"
            "3ba3edfd7a7b12b27ac72c3e67768f617fc81bc3888a51323a9fb8aa4b1e5e4adae5494"
            "dffff001d1aa4ae18"
        ),
        height=0,
        prev_work=0,
    )
    VERIFICATION_BLOCK_MERKLE_ROOT = None


class RegTestNet(AbstractNetwork):
    NET = REGTEST
    PUBKEY_HASH = 0x6F
    PRIVATEKEY = 0xEF
    SCRIPTHASH = 0xC4
    XPUBKEY = 0x043587CF
    XPRIVKEY = 0x04358394
    MAGIC = 0xDAB5BFFA
    PORT = 18444
    DNS_SEEDS = ["127.0.0.1"]
    BITCOINX_COIN = BitcoinRegtest
    # i.e. Genesis block
    CHECKPOINT = CheckPoint(
        bytes.fromhex(
            "010000000000000000000000000000000000000000000000000000000000000000000000"
            "3ba3edfd7a7b12b27ac72c3e67768f617fc81bc3888a51323a9fb8aa4b1e5e4adae5494"
            "dffff7f2002000000"
        ),
        height=0,
        prev_work=0,
    )


class NetworkConfig:
    def __init__(self, network_type: str, node_host: Optional[str]=None):
        network: AbstractNetwork = NETWORKS[network_type]
        self.node_host = node_host
        self.NET = network.NET
        self.PUBKEY_HASH = network.PUBKEY_HASH
        self.PRIVATEKEY = network.PRIVATEKEY
        self.SCRIPTHASH = network.SCRIPTHASH
        self.XPUBKEY = network.XPUBKEY
        self.XPRIVKEY = network.XPRIVKEY
        self.MAGIC = network.MAGIC
        self.PORT = network.PORT
        self.DNS_SEEDS = network.DNS_SEEDS
        self.BITCOINX_COIN: Coin = network.BITCOINX_COIN
        self.CHECKPOINT: CheckPoint = network.CHECKPOINT

        self.peers: List[Peer] = []
        self.set_peers(network)

    def get_default_peers(self, network):
        if isinstance(network, RegTestNet):
            self.peers = [Peer("127.0.0.1", 18444)]
            # self.peers = [Peer("host.docker.internal", 18444)]
        if isinstance(network, TestNet):
            self.peers = [
                Peer("127.0.0.1", 18333),
                # Peer("167.99.91.85", 18333),  # random node from WOC
                # Peer("165.22.240.87", 18333),  # austecondevserver.app
                # Peer("176.9.148.163", 18333),  # tsv.usebsv.com
            ]
        if isinstance(network, ScalingTestNet):
            self.peers = [Peer("116.202.171.166", 9333)]
            # self.peers = [Peer("95.217.108.109", 9333)]  #  stn-server.electrumsv.io
        elif isinstance(network, MainNet):
            self.peers = [Peer("127.0.0.1", 8333)]
            # self.peers = [
            #     Peer(host, self.PORT) for host in get_seed_peers(self.DNS_SEEDS)
            # ]

    def set_peers(self, network):
        if self.node_host:
            host = self.node_host.split(":")[0]
            host = cast_to_valid_ipv4(host)  # in docker a container name needs dns resolution
            port = int(self.node_host.split(":")[1])
            self.peers = [Peer(host, port)]

        else:
            self.get_default_peers(network)

NETWORKS = {
    MAINNET: MainNet(),
    TESTNET: TestNet(),
    SCALINGTESTNET: ScalingTestNet(),
    REGTEST: RegTestNet(),
}
