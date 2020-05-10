from typing import Optional

from bitcoinx import (
    Coin,
    CheckPoint,
    Bitcoin,
    BitcoinTestnet,
    BitcoinScalingTestnet,
    BitcoinRegtest,
)

from peers import get_seed_peers, Peer


class AbstractNetwork:
    PREFIX = ""
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
    PREFIX = "main"
    PUBKEY_HASH = 0x00
    PRIVATEKEY = 0x80
    SCRIPTHASH = 0x05
    XPUBKEY = 0x0488B21E
    XPRIVKEY = 0x0488ADE4
    MAGIC = 0xE3E1F3E8
    PORT = 8333
    DNS_SEEDS = ["seed.bitcoinsv.io"]
    BITCOINX_COIN = Bitcoin
    CHECKPOINT = CheckPoint(
        bytes.fromhex(
            "000000201ff55a51c860352808114e52a9b09ae65dd9156a24e75e01000000000000000033320991"
            "190dcb5a86159e56b9ba2da6bca4e515d9dcbab6878471f8fcdf4039cd98425d7dee0518c5322529"
        ),
        height=593660,
        prev_work=0xE5EEF982B80AF19DBEB052,
    )


class TestNet(AbstractNetwork):
    PREFIX = "test"
    PUBKEY_HASH = 0x6F
    PRIVATEKEY = 0xEF
    SCRIPTHASH = 0xC4
    XPUBKEY = 0x043587CF
    XPRIVKEY = 0x04358394
    MAGIC = 0xF4E5F3F4
    PORT = 18333
    DNS_SEEDS = ["testnet-seed.bitcoinsv.io"]
    BITCOINX_COIN = BitcoinTestnet
    CHECKPOINT = CheckPoint(
        bytes.fromhex(
            "0000002029f1e3df7fda466242b9b56076792ffdb9e5d7ea51610307bc010000000000007ac1fa84"
            "ef5f0998232fb01cd6fea2c0199e34218df2fb33e4e80e79d22b6a746994435d41c4021a208bae0a"
        ),
        height=1314717,
        prev_work=0x62BA4D708756160950,
    )


class ScalingTestNet(AbstractNetwork):
    PREFIX = "stn"
    PUBKEY_HASH = 0x6F
    PRIVATEKEY = 0xEF
    SCRIPTHASH = 0xC4
    XPUBKEY = 0x043587CF
    XPRIVKEY = 0x04358394
    MAGIC = 0xFBCEC4F9
    PORT = 9333
    DNS_SEEDS = ["stn-seed.bitcoinsv.io"]
    BITCOINX_COIN = BitcoinScalingTestnet

    # Todo - probably needs updating
    CHECKPOINT = CheckPoint(
        bytes.fromhex(
            "0100000000000000000000000000000000000000000000000000000000000000000000003ba3edfd"
            "7a7b12b27ac72c3e67768f617fc81bc3888a51323a9fb8aa4b1e5e4adae5494dffff001d1aa4ae18"
        ),
        height=0,
        prev_work=0,
    )
    VERIFICATION_BLOCK_MERKLE_ROOT = None


class RegTestNet(AbstractNetwork):
    PREFIX = "regtest"
    PUBKEY_HASH = 0x6F
    PRIVATEKEY = 0xEF
    SCRIPTHASH = 0xC4
    XPUBKEY = 0x043587CF
    XPRIVKEY = 0x04358394
    MAGIC = 0xDAB5BFFA
    PORT = 18444
    DNS_SEEDS = ["127.0.0.1"]
    BITCOINX_COIN = BitcoinRegtest

    # NOTE: placeholder only - needs to be dynamically created for every RegTest reset
    CHECKPOINT = CheckPoint(
        bytes.fromhex(
            "0000002029f1e3df7fda466242b9b56076792ffdb9e5d7ea51610307bc010000000000007ac1fa84"
            "ef5f0998232fb01cd6fea2c0199e34218df2fb33e4e80e79d22b6a746994435d41c4021a208bae0a"
        ),
        height=123,
        prev_work=123,
    )


class NetworkConfig:
    def __init__(self, network: AbstractNetwork):
        self.PREFIX = network.PREFIX
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

        if isinstance(network, RegTestNet):
            self.peers = [Peer("127.0.0.1", 18444)]
        else:
            self.peers = [
                Peer(host, self.PORT) for host in get_seed_peers(self.DNS_SEEDS)
            ]


NETWORKS = {
    MainNet.PREFIX: MainNet,
    TestNet.PREFIX: TestNet,
    ScalingTestNet.PREFIX: ScalingTestNet,
    RegTestNet.PREFIX: RegTestNet,
}
