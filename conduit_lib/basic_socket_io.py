import logging
import struct
from socket import socket

logger = logging.getLogger(f"basic-socket-io")
logger.setLevel(logging.DEBUG)

struct_be_Q = struct.Struct(">Q")


def send_msg(sock: socket, msg: bytes) -> bool:
    # Prefix each message with a 8-byte length (network byte order)
    msg = struct_be_Q.pack(len(msg)) + msg
    sock.sendall(msg)
    return True


def recv_msg(sock: socket) -> bytearray:
    # Read message length and unpack it into an integer
    raw_msglen = recvall(sock, 8)
    if not raw_msglen:
        return None
    msglen = struct_be_Q.unpack(raw_msglen)[0]
    # Read the message data
    return recvall(sock, msglen)


def recvall(sock: socket, n: int) -> bytearray:
    # Helper function to recv n bytes or return None if EOF is hit
    data = bytearray()
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            return None
        data.extend(packet)
    return data
