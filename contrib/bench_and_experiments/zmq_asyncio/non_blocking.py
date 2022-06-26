import logging
import threading

import zmq
from typing import List

from conduit_lib.utils import zmq_recv_and_process_batchwise_no_block, zmq_send_no_block

logger = logging.getLogger("zmq-no-block")
logging.basicConfig(level=logging.DEBUG)


def client():
    context6 = zmq.Context[zmq.Socket[bytes]]()
    tx_parse_ack_socket = context6.socket(zmq.PUSH)
    tx_parse_ack_socket.setsockopt(zmq.SNDHWM, 10000)
    tx_parse_ack_socket.connect("tcp://127.0.0.1:33333")

    count = 0
    while True:
        msg = f"hello_{count}".encode('utf-8')
        zmq_send_no_block(tx_parse_ack_socket, msg, on_blocked_msg="Receiver is busy")


def process_work_items(work_items: List[bytes]) -> None:
    logger.debug(f"Got {len(work_items)} work items for processing")


def server():
    context6 = zmq.Context[zmq.Socket[bytes]]()
    tx_parse_ack_socket = context6.socket(zmq.PULL)
    tx_parse_ack_socket.setsockopt(zmq.RCVHWM, 10000)
    tx_parse_ack_socket.bind("tcp://127.0.0.1:33333")
    BATCHING_RATE = 1

    zmq_recv_and_process_batchwise_no_block(tx_parse_ack_socket, process_work_items,
        on_blocked_msg="Receiver blocked", batching_rate=BATCHING_RATE, poll_timeout_ms=100)


threads = [
    threading.Thread(target=server, daemon=True),
    threading.Thread(target=client, daemon=True)
]
for thread in threads:
    thread.start()

for thread in threads:
    thread.join()
