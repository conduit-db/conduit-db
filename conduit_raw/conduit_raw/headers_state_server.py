import logging
import threading
from multiprocessing.managers import SyncManager
from queue import Queue


class HeadersStateServer(threading.Thread):

    def __init__(self, headers_manager):
        threading.Thread.__init__(self)
        self.headers_manager = headers_manager
        self.logger = logging.getLogger("headers-state-server")

    def run(self):
        try:
            self.headers_manager.serve()
        except KeyboardInterrupt:
            self.logger.debug("HeadersStateServer stopping...")


class HeadersStateManager(SyncManager):
    """ConduitIndex should block on the queue first and then "get_tip" to get the current tip
    state of ConduitRaw

    This assumes there is only ever exactly ONE ConduitIndex instance and ONE ConduitRaw instance
    A language agnostic means of achieving the same would be a REST API for the out-of-band
    'get_tip' request and then ZMQ PUSH/PULL to replace the shared queue
    """

    def __init__(self, tip_raw, tip_hash, tip_height):
        super().__init__(address=('', 50000), authkey=b'abracadabra')
        self.shared_headers_queue = Queue()
        self.tip_raw = tip_raw
        self.tip_hash = tip_hash
        self.tip_height = tip_height
        self.register('get_headers_queue', callable=self.get_headers_queue)
        self.register('get_tip', callable=self.get_tip)
        self.register('set_tip', callable=self.set_tip)
        self.server = self.get_server()
        self.logger = logging.getLogger("headers-state-server")

        self.tip_lock = threading.Lock()

    def serve(self):
        self.server.serve_forever()

    def stop(self):
        # This is probably not the intended way of shutting down the SyncManager but all of this
        # Code is destined for replacement by Kafka eventually anyway!
        self.server.stop_event.set()

    def get_headers_queue(self):
        return self.shared_headers_queue

    def get_tip(self):
        return self.tip_raw, self.tip_hash, self.tip_height

    def set_tip(self, shared_tip_raw, shared_tip_hash, shared_tip_height):
        self.tip_raw, self.tip_hash, self.tip_height = \
            shared_tip_raw, shared_tip_hash, shared_tip_height


if __name__ == "__main__":

    tip_raw, tip_hash, tip_height = (
        b'\x02\x00\x00\x006\xf8h\xef\xc7\xfeJ\xec\x94\x95\xe0\xc0v;\xc1\x04x\x17\xd7u4(_\xcc'
        b'\x00\x00\x00\x00\x00\x00\x00\x00\xff_\xe7\x02[\x08\xea5\xeci\xc0q|\x92\xf6^c\xff\\\xb2s'
        b'\x87\\m\x8ctV\xb4+\xb3\xad\xed\xf4\xcc\x9bRB\x12\x06\x19\xa4\x93\x00P',
        b'o\xe2\x8c\n\xb6\xf1\xb3r\xc1\xa6\xa2F\xaec\xf7O\x93\x1e\x83e\xe1Z\x08\x9ch\xd6\x19\x00'
        b'\x00\x00\x00\x00',
        272573
    )
    headers_manager = HeadersStateManager(tip_raw, tip_hash, tip_height)
    queue_manager = HeadersStateServer(headers_manager)
    queue_manager.start()
    queue_manager.join()
