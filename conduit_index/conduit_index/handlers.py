import struct
import typing
from io import BytesIO
from math import ceil

from conduit_lib.constants import CONDUIT_INDEX_SERVICE_NAME
from conduit_p2p import HandlersDefault, MsgType, BitcoinClient

if typing.TYPE_CHECKING:
    from conduit_index.conduit_index.controller import Controller


class IndexerHandlers(HandlersDefault):

    def __init__(self, network_type: str, controller: "Controller"):
        super().__init__(network_type)
        self.controller = controller
        self.headers_threadsafe = self.controller.headers_threadsafe
        self.server_type = self.controller.service_name

    async def on_inv(self, message: bytes, peer: BitcoinClient) -> None:
        inv_vect = peer.deserializer.inv(BytesIO(message))

        tx_inv_vect = []
        for inv in inv_vect:
            # TX
            if inv["inv_type"] == 1 and self.server_type == "ConduitIndex":
                tx_inv_vect.append(inv)

        if tx_inv_vect and self.server_type == "ConduitIndex":
            max_getdata_size = 50_000
            num_getdatas = ceil(len(tx_inv_vect) / max_getdata_size)
            for i in range(num_getdatas):
                getdata_msg = self.serializer.getdata(tx_inv_vect[i: (i + 1) * max_getdata_size])
                await peer.queued_getdata_requests.put(getdata_msg)
                peer.send_message(getdata_msg)

    async def on_tx(self, rawtx: bytes, peer: BitcoinClient) -> None:
        # logger.debug(f"Got mempool tx")
        size_tx = len(rawtx)
        packed_message = struct.pack(f"<II{size_tx}s", MsgType.MSG_TX, size_tx, rawtx)
        if self.controller.service_name == CONDUIT_INDEX_SERVICE_NAME:
            await self.controller.zmq_socket_listeners.socket_mempool_tx.send(packed_message)  # type: ignore
            self.controller.mempool_tx_count += 1  # type: ignore
