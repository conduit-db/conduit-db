import socket
import threading
import time

from conduit_lib.basic_socket_io import recv_msg


def recv_blocks_thread():
    print("Raw socket server listening on port: " + str(sock_port))
    while True:
        conn, sock_addr = sock.accept()
        t0 = time.time()
        print(f"accepted connection from: {sock_addr}")
        while True:
            payload = recv_msg(conn)
            if payload:
                # print(payload)
                print(f"got payload with length: {len(payload)}")
                print(f"transfer took: {time.time() - t0}")
            if not payload:
                break
        conn.close()


if __name__ == '__main__':
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock_callback_ip = '127.0.0.1'  # in docker might be conduit-index for example
    sock.bind((sock_callback_ip, 7777))
    sock.listen()
    t = threading.Thread(target=recv_blocks_thread, daemon=True)
    t.start()
    t.join()


