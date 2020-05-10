import asyncio
import logging

from constants import LOGGING_FORMAT
from networks import NetworkConfig, NETWORKS
from peers import Peer
from session import BufferedSession


logging.basicConfig(
    format=LOGGING_FORMAT, level=logging.DEBUG, datefmt="%Y-%m-%d %H-%M-%S"
)
logger = logging.getLogger("session")


class SessionManager:
    def __init__(self, network, host, port):
        self.network = network
        self.session = None
        self.transport = None
        self.config = NetworkConfig(NETWORKS[network]())
        self.peers = self.config.peers
        self.host = host
        self.port = port

    def get_peer(self) -> Peer:
        return self.peers[0]

    async def connect(self):
        loop = asyncio.get_event_loop()
        peer = self.get_peer()
        logger.debug("[connect] connecting to (%s, %s)", peer.host, peer.port)
        protocol_factory = lambda: BufferedSession(
            self.config, peer, self.host, self.port
        )
        self.transport, self.session = await loop.create_connection(
            protocol_factory, peer.host, peer.port
        )
        return self.transport, self.session

    async def run(self):
        try:
            await self.connect()
            init_handshake = asyncio.create_task(
                self.session.send_version(
                    recv_host=self.session.peer.host,
                    recv_port=self.session.peer.port,
                    send_host=self.session.host,
                    send_port=self.session.port,
                )
            )
            wait_untiL_conn_lost = asyncio.create_task(
                self.session.con_lost_event.wait()
            )
            await asyncio.wait([init_handshake, wait_untiL_conn_lost])
        finally:
            if self.transport:
                self.transport.close()
