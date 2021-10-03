import socket
import struct

import threading

from conduit_lib.basic_socket_io import send_msg


def send_blocks_thread(sock: socket) -> bool:
    print(f"Raw socket client sending to: {sock_callback_ip}:{sock_port}")
    msg = bytearray(b'x' * 2_000_000_000)
    result: bool = send_msg(sock, msg)
    return result


if __name__ == '__main__':
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock_callback_ip = '127.0.0.1'  # in docker might be conduit-index for example
    sock_port = 7777
    sock.connect((sock_callback_ip, sock_port))

    t = threading.Thread(target=send_blocks_thread, args=(sock,), daemon=True)
    t.start()
    t.join()