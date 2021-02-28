import logging
import sys
import time
from multiprocessing.managers import SyncManager

import bitcoinx


class HeadersStateClient(SyncManager):
    """ConduitIndex should connect first and then "get_tip" to get the current tip
    state of ConduitRaw. The queue will start filling after connecting (some of the queued headers
    may be earlier than the 'get_tip' call returns given timings.

    This assumes there is only ever exactly ONE ConduitIndex instance and ONE ConduitRaw instance
    A language agnostic means of achieving the same would be a REST API for the out-of-band
    'get_tip' request and then ZMQ PUSH/PULL to replace the shared queue
    """

    def __init__(self):
        super().__init__(address=('127.0.0.1', 50000), authkey=b'abracadabra')
        self.logger = logging.getLogger("test-headers-state-client")
        self.register('get_headers_queue')
        self.register('get_tip')  # In case no new blocks come in for a while
        self.register('set_tip')
        self.headers_queue = None

    def connect_or_keep_trying(self):
        """This will block the thread"""
        while True:
            try:
                self.connect()
                break
            except ConnectionRefusedError:
                self.logger.debug(f"Could not connect to HeadersStateServer, retrying...")
                time.sleep(1)
                continue  # Keep trying to connect

    def get_remote_tip(self):
        tip_proxy = self.get_tip()
        tip = tip_proxy._getvalue()
        tip_raw = tip[0]
        tip_hash = tip[1]
        tip_height = tip[2]
        return bitcoinx.Header(*bitcoinx.unpack_header(tip_raw), tip_hash, tip_raw, tip_height)

    def set_remote_tip(self, tip_hash, tip_raw, tip_height):
        self.set_tip(tip_hash, tip_raw, tip_height)

    def get_remote_headers_queue(self):
        """Needs to be connected to return proxy object"""
        self.headers_queue = self.get_headers_queue()
        return self.headers_queue


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    logger = logging.getLogger("test-headers-state-client")
    client = HeadersStateClient()
    headers_queue = client.get_remote_headers_queue()
    tip = client.get_remote_tip()
    logger.debug(f"initial tip={tip}")

    while True:
        try:
            tip_raw, tip_hash, tip_height = headers_queue.get()
            logger.debug(f"got tip height={tip_height}")
        except ConnectionResetError:
            logger.debug(f"forceful disconnection, abort")  # Unable to reconnect - ZMQ may be best
            sys.exit(1)
