from peers import get_seed_peers, Peer


class AbstractNetwork:
    PREFIX = ''
    PUBKEY_HASH = 0x00
    PRIVATEKEY = 0x00
    SCRIPTHASH = 0x00
    XPUBKEY = 0x00000000
    XPRIVKEY = 0x00000000
    MAGIC = 0x00000000
    PORT = 0000
    DNS_SEEDS = [""]


class MainNet(AbstractNetwork):
    PREFIX = 'main'
    PUBKEY_HASH = 0x00
    PRIVATEKEY = 0x80
    SCRIPTHASH = 0x05
    XPUBKEY = 0x0488b21e
    XPRIVKEY = 0x0488ade4
    MAGIC = 0xe3e1f3e8
    PORT = 8333
    DNS_SEEDS = ["seed.bitcoinsv.io"]


class TestNet(AbstractNetwork):
    PREFIX = 'test'
    PUBKEY_HASH = 0x6f
    PRIVATEKEY = 0xef
    SCRIPTHASH = 0xc4
    XPUBKEY = 0x043587cf
    XPRIVKEY = 0x04358394
    MAGIC = 0xf4e5f3f4
    PORT = 18333
    DNS_SEEDS = ["testnet-seed.bitcoinsv.io"]


class ScalingTestNet(AbstractNetwork):
    PREFIX = 'stn'
    PUBKEY_HASH = 0x6f
    PRIVATEKEY = 0xef
    SCRIPTHASH = 0xc4
    XPUBKEY = 0x043587cf
    XPRIVKEY = 0x04358394
    MAGIC = 0xfbcec4f9
    PORT = 9333
    DNS_SEEDS = ["stn-seed.bitcoinsv.io"]


class RegTestNet(AbstractNetwork):
    PREFIX = 'regtest'
    PUBKEY_HASH = 0x6f
    PRIVATEKEY = 0xef
    SCRIPTHASH = 0xc4
    XPUBKEY = 0x043587cf
    XPRIVKEY = 0x04358394
    MAGIC = 0xdab5bffa
    PORT = 18444
    DNS_SEEDS = ["127.0.0.1"]


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
        if isinstance(network, RegTestNet):
            self.peers = [Peer("127.0.0.1", 18444)]
        else:
            self.peers = [Peer(host, self.PORT) for host in get_seed_peers(self.DNS_SEEDS)]
        #self.peers = [Peer(host='178.128.225.128', port=55244)]

NETWORKS = {
    MainNet.PREFIX: MainNet,
    TestNet.PREFIX: TestNet,
    ScalingTestNet.PREFIX: ScalingTestNet,
    RegTestNet.PREFIX: RegTestNet
}
