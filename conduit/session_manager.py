import asyncio
import logging
from typing import Optional

import bitcoinx
from bitcoinx import Headers

from constants import LOGGING_FORMAT
from networks import (
    NetworkConfig,
    NETWORKS,
)
from peers import Peer
from session import BufferedSession
from store import Storage

logging.basicConfig(
    format=LOGGING_FORMAT, level=logging.DEBUG, datefmt="%Y-%m-%d %H-%M-%S"
)
logger = logging.getLogger("session")


class SessionManager:
    """Coordinates startup and shutdown of all components"""

    def __init__(self, network, host, port):
        self.network: str = network
        self.session: Optional[BufferedSession] = None
        self.transport = None
        self.config = NetworkConfig(NETWORKS[network]())
        self.peers = self.config.peers
        self.host = host
        self.port = port
        self.storage: Optional[Storage] = None

    def get_peer(self) -> Peer:
        return self.peers[0]

    async def connect_session(self):
        loop = asyncio.get_event_loop()
        peer = self.get_peer()
        logger.debug("[connect] connecting to (%s, %s)", peer.host, peer.port)
        protocol_factory = lambda: BufferedSession(
            self.config, peer, self.host, self.port, self.storage
        )
        self.transport, self.session = await loop.create_connection(
            protocol_factory, peer.host, peer.port
        )
        return self.transport, self.session

    async def run(self):
        try:
            self.setup_storage()
            await self.connect_session()
            init_handshake = asyncio.create_task(
                self.session.send_version(
                    self.session.peer.host,
                    self.session.peer.port,
                    self.session.host,
                    self.session.port,
                )
            )
            wait_until_conn_lost = asyncio.create_task(
                self.session.con_lost_event.wait()
            )
            await asyncio.wait([init_handshake, wait_until_conn_lost])
        finally:
            if self.transport:
                self.transport.close()

    def setup_storage(self) -> None:
        # Headers
        headers = bitcoinx.Headers.from_file(
            self.config.BITCOINX_COIN, "headers.mmap", self.config.CHECKPOINT
        )
        # PG
        # -- NotImplemented
        # Memcached
        # -- NotImplemented
        self.storage = Storage(headers, None, None)
