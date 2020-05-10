import logging
import io

from commands import TX, VERACK, GETDATA, PING
from constants import LOGGING_FORMAT
from deserializer import Deserializer
from networks import NetworkConfig
from serializer import Serializer
from store import Storage

logging.basicConfig(
    format=LOGGING_FORMAT, level=logging.DEBUG, datefmt="%Y-%m-%d %H-%M-%S"
)
logger = logging.getLogger("handlers")


class Handlers:
    def __init__(
        self, session: "BufferedSession", net_config: NetworkConfig, storage: Storage
    ):
        self.net_config = net_config
        self.session = session
        self.serializer = Serializer(self.net_config, storage)
        self.deserializer = Deserializer(self.net_config, storage)
        self.storage = storage

    async def on_version(self, message):
        logger.debug("handling version...")
        verack_message = self.session.serializer.verack()
        await self.session.send_request(VERACK, verack_message)

    async def on_verack(self, message):
        logger.debug("handling verack...")
        logger.debug("handshake complete")

    async def on_protoconf(self, message):
        logger.debug("handling protoconf...")
        protoconf = self.session.deserializer.protoconf(io.BytesIO(message))
        logger.debug(f"protoconf: {protoconf}")
        self.session.handshake_complete_event.set()

        # temporary for testing
        # inv_vects = [{'inv_type': 1, 'inv_hash': '72bb98f5436fe20fe033303e12783841b16e04036c6e0c29581cf17a61d4c12c'}]
        # getdata_msg = self.serializer.getdata(inv_vects)
        # await self.session.send_request(GETDATA, getdata_msg)

    async def on_sendheaders(self, message):
        logger.debug("handling sendheaders...")

    async def on_sendcmpct(self, message):
        logger.debug("handling sendcmpct...")

    async def on_ping(self, message):
        logger.debug("handling ping...")
        pong_message = self.session.serializer.pong()
        await self.session.send_request(PING, pong_message)

    async def on_addr(self, message):
        logger.debug("handling addr...")

    async def on_feefilter(self, message):
        logger.debug("handling feefilter...")

    async def on_inv(self, message):
        """Todo: Optimization
        This could be optimized by staying in bytes. No need to convert the inv_type
        to int to categorise it - rather just match to corresponding byte string.
        payload_len/36 == count and the index in bytearray of each hash is just every 36th byte
        with an offset of 4."""
        logger.debug("handling inv...")
        inv_vects = self.session.deserializer.inv(io.BytesIO(message))
        logger.debug(f"inv: {inv_vects}")

        # temporary for testing - request all relayed txs without checking if we have it already
        getdata_msg = self.serializer.getdata(inv_vects)
        await self.session.send_request(GETDATA, getdata_msg)

    async def on_getdata(self, message):
        logger.debug("handling getdata...")
        # rawtx = "01000000013d4baa6084b3e3ecc4bde56ee0bf617395991124a3d0f61597cd8dd0189d84b1010000006a473044022066ab98bad401393c558b85582e3af35f1f0686bf4dcd5bf5aff06a1d2dc39a870220572aa26b308c9692405c7d55fbbca0be2602586eceda4ca9b5cd29cd2157f0344121036d6b9802775bd330cae7080961b8dccf3589926e8553ff33efae66a6c69a371dffffffff02000000000000000009006a0648656c6c6f0ace850100000000001976a914df149def93f09dbc53ce09b2d2cd3dbccc4d712088ac797f0900"
        # await self.session.send_request(TX, rawtx=rawtx)

    async def on_tx(self, message):
        logger.debug("handling tx...")
        tx = self.session.deserializer.tx(io.BytesIO(message))
        logger.debug(f"tx: {tx}")

    async def on_headers(self, message: bytes):
        logger.debug("handing headers...")
        self.deserializer.headers(io.BytesIO(message))
