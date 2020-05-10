from collections import namedtuple
from typing import List
from dns import resolver


Peer = namedtuple("Peer", ['host', 'port'])


def get_seed_peers(dns_seeds) -> List[str]:
    peers = []
    for seed in dns_seeds:
        for result in resolver.query(seed, 'A'):
            peers.append(result.to_text())
    return peers
