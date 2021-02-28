"""NOT ACTUALLY USED BY ANYTHING AND MAY BE REMOVED SOON"""

import logging
import queue
import struct
import threading
from typing import Optional
import zmq


class ZMQHeadersPushNotifications(threading.Thread):
    """Pushes new block tip to ConduitIndexer"""

    def __init__(self, tip_raw: bytes, tip_hash: bytes, tip_height, headers_tip_queue: queue.Queue,
            bind_host: str="127.0.0.1", bind_port: int=65432):
        threading.Thread.__init__(self, daemon=True)
        self.logger = logging.getLogger("zmq-headers-notifications")
        self.bind_host = bind_host
        self.bind_port = bind_port
        self.bind_url = f"tcp://{self.bind_host}:{self.bind_port}"
        self.headers_tip_queue = headers_tip_queue

        self.headers_lock = threading.RLock()
        self.tip_raw = tip_raw
        self.tip_hash = tip_hash
        self.tip_height = tip_height

        # ZMQ API
        # There should only be one peer (ConduitIndexer) otherwise need PUB/SUB model not
        # PUSH/PULL But ZMQ PUB/SUB suffers from some additional complexities (the "slow joiner"
        # problem). Which is easily fixed but i'd rather keep the code simple unless needed.
        self.zmq_sock_lock = threading.RLock()
        self.zmq_peer_lock = threading.RLock()
        self.zmq_peer = None

        self.new_registration_queue = queue.Queue()

    def get_headers_tip_threadsafe(self):
        with self.headers_lock:
            return self.tip_raw, self.tip_hash, self.tip_height

    def set_headers_tip_threadsafe(self, tip_raw, tip_hash, tip_height):
        with self.headers_lock:
            self.tip_raw, self.tip_hash, self.tip_height = tip_raw, tip_hash, tip_height

    def push_message_threadsafe(self, message: bytes):
        with self.zmq_sock_lock:
            self.sock.send(message)

    def run(self):
        t = threading.Thread(target=self.handle_initial_registration_thread,
            daemon=True)
        t.start()

        ctx = zmq.Context()
        self.sock = ctx.socket(zmq.PUB)
        self.sock.bind(self.bind_url)
        self.sock: zmq.Socket

        while True:
            tip_raw, tip_hash, tip_height = self.headers_tip_queue.get()
            # self.logger.debug(f"got from headers_tip_queue: tip_height={tip_height}")

            with self.zmq_peer_lock:
                self.set_headers_tip_threadsafe(tip_raw, tip_hash, tip_height)
                if self.zmq_peer:
                    self.logger.debug(f"sending headers tip update {self.tip_height}")
                    packed_msg = struct.pack('<80s32sI', tip_raw, tip_hash, tip_height)
                    self.push_message_threadsafe(packed_msg)

    def handle_initial_registration_thread(self):
        """Out-of-band REQ/REP for initial exchange of header tip state"""
        sock = Optional[zmq.Socket]
        try:
            host = "127.0.0.1"
            port = 65433
            url = f"tcp://{host}:{port}"
            ctx = zmq.Context()
            sock = ctx.socket(zmq.ROUTER)
            sock.bind(url)
            sock: zmq.Socket

            while True:
                address, empty, packed_req = sock.recv_multipart()
                msg_type, conduit_indexer_uuid = struct.unpack_from("<I32s", packed_req)

                # Register the ConduitIndexer Controller for Push Notifications
                with self.zmq_peer_lock:
                    # Design is only for a single ConduitIndexer Controller
                    # This will overwrite the previous peer in the set
                    self.zmq_peer = conduit_indexer_uuid

                    # Send current headers tip as the initial response
                    tip_raw, tip_hash, tip_height = self.get_headers_tip_threadsafe()
                    self.logger.debug(f"sending initial headers tip to {conduit_indexer_uuid}")
                    packed_msg = struct.pack('<80s32sI', tip_raw, tip_hash, tip_height)
                    sock.send(packed_msg)
        finally:
            if sock:
                sock.close()

