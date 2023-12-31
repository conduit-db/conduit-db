# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.

import logging
from collections import namedtuple

from bitcoinx import Network, Bitcoin, BitcoinTestnet, BitcoinScalingTestnet, BitcoinRegtest

from .constants import MAINNET, TESTNET, SCALINGTESTNET, REGTEST

logger = logging.getLogger("conduit.p2p.networks")

Peer = namedtuple("Peer", ["remote_host", "remote_port"])


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
    BITCOINX_COIN: Network | None = None
    GENESIS_BLOCK_HASH = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
    GENESIS_ACTIVATION_HEIGHT = 0


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
    GENESIS_BLOCK_HASH = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
    GENESIS_ACTIVATION_HEIGHT = 620_538


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
    GENESIS_BLOCK_HASH = "000000000933ea01ad0ee984209779baaec3ced90fa3f408719526f8d77f4943"
    GENESIS_ACTIVATION_HEIGHT = 1_344_302


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
    GENESIS_BLOCK_HASH = "000000000933ea01ad0ee984209779baaec3ced90fa3f408719526f8d77f4943"
    VERIFICATION_BLOCK_MERKLE_ROOT = None
    GENESIS_ACTIVATION_HEIGHT = 100


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
    GENESIS_BLOCK_HASH = "0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206"
    GENESIS_ACTIVATION_HEIGHT = 10_000


class NetworkConfig:
    def __init__(
        self,
        network_type: str,
    ) -> None:
        network: AbstractNetwork = NETWORKS[network_type]
        self.NET = network.NET
        self.PUBKEY_HASH = network.PUBKEY_HASH
        self.PRIVATEKEY = network.PRIVATEKEY
        self.SCRIPTHASH = network.SCRIPTHASH
        self.XPUBKEY = network.XPUBKEY
        self.XPRIVKEY = network.XPRIVKEY
        self.MAGIC = network.MAGIC
        self.PORT = network.PORT
        self.DNS_SEEDS = network.DNS_SEEDS
        self.BITCOINX_COIN: Network = network.BITCOINX_COIN
        self.GENESIS_BLOCK_HASH: str = network.GENESIS_BLOCK_HASH
        self.GENESIS_ACTIVATION_HEIGHT = network.GENESIS_ACTIVATION_HEIGHT


NETWORKS = {
    MAINNET: MainNet(),
    TESTNET: TestNet(),
    SCALINGTESTNET: ScalingTestNet(),
    REGTEST: RegTestNet(),
}
