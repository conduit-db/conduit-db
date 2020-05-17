import io

import bitcoinx

from .commands import VERACK, GETDATA, PING, SENDCMPCT, PONG
from .deserializer import Deserializer
from .logs import logs
from .networks import NetworkConfig
from .serializer import Serializer
from .store import Storage

logger = logs.get_logger("handlers")


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
        version = self.session.deserializer.version(io.BytesIO(message))
        self.session.set_target_header_height(version["start_height"])
        logger.debug("received version message: %s", version)
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

    async def on_sendheaders(self, message):
        logger.debug("handling sendheaders...")

    async def on_sendcmpct(self, message):
        message = message.tobytes()
        logger.debug("handling sendcmpct...")
        logger.debug("received sendcmpct message: %s", message)
        sendcmpct = self.session.serializer.sendcmpct()
        logger.debug("responding with message: %s", sendcmpct)
        await self.session.send_request(SENDCMPCT, sendcmpct)

    async def on_ping(self, message):
        logger.debug("handling ping...")
        pong_message = self.session.serializer.pong()
        await self.session.send_request(PONG, pong_message)

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
        # logger.debug("handling inv...")
        inv_vects = self.session.deserializer.inv(io.BytesIO(message))
        # logger.debug(f"inv: {inv_vects}")

        for inv in inv_vects:
            if inv["inv_type"] == 2:  # BLOCK
                # logger.info(f"received a block inv: {inv}")
                self.session._pending_blocks_queue.put_nowait(inv)
            else:
                if self.session.is_synchronized():
                    # temporary for testing - request all relayed txs without checking db
                    getdata_msg = self.serializer.getdata(inv_vects)
                    await self.session.send_request(GETDATA, getdata_msg)
                # else ignore mempool activity

    async def on_getdata(self, message):
        logger.debug("handling getdata...")

    async def on_headers(self, message):
        # logger.debug("handling headers...")
        if self.deserializer.connect_headers(io.BytesIO(message)):
            self.session._headers_msg_processed_event.set()

    # ----- Special case messages ----- #

    async def on_tx(self, message):
        # logger.debug("handling tx...")
        tx = self.session.deserializer.tx(io.BytesIO(message))
        # logger.debug(f"tx: {tx}")
        self.session._msg_handled_count += 1

    async def on_block(self, message: memoryview):
        """simplified right down to just parsing a whole block on single core so that
        we can redesign around the 'pre-processor' idea and then do the txs in
        parallel given the co-ordinates of each tx in the block"""
        try:
            raw_header = message[0:80].tobytes()
            header_hash = bitcoinx.double_sha256(raw_header)
            current_block_header, chain = self.storage.headers.lookup(header_hash)
            logger.debug("current_block_header = %s", current_block_header)
            logger.info("block size=%s bytes", len(message))

            # updates local_block_tip - doing this sequentially is not ideal
            # connecting headers should be done at the end of each batch of 500 blocks
            self.deserializer.connect_block_header(raw_header)
            block_subview = message[80:]
            stream: io.BytesIO = io.BytesIO(block_subview)

            txs = []
            count = bitcoinx.read_varint(stream.read)
            for i in range(count):
                tx = bitcoinx.Tx.read(stream.read)
                txn_tuple = (tx.hash(), current_block_header.height, tx.to_bytes())
                txs.append(txn_tuple)

            with self.storage.pg_database.atomic():
                self.storage.insert_many_txs(txs)

            self.session._msg_handled_count += 1
            self.session._block_parsed_event.set()
        except Exception as e:
            logger.exception(e)
