import logging
import struct
from socket import socket

logger = logging.getLogger(f"basic-socket-io")
logger.setLevel(logging.DEBUG)


def send_msg(sock: socket, msg: bytearray):
    # Prefix each message with a 8-byte length (network byte order)
    msg = struct.pack('>Q', len(msg)) + msg
    sock.sendall(msg)
    return True


def recv_msg(sock: socket):
    # Read message length and unpack it into an integer
    raw_msglen = recvall(sock, 8)
    if not raw_msglen:
        return None
    msglen = struct.unpack('>Q', raw_msglen)[0]
    # Read the message data
    return recvall(sock, msglen)


def recvall(sock: socket, n: int):
    # Helper function to recv n bytes or return None if EOF is hit
    data = bytearray()
    while len(data) < n:
        packet = sock.recv(n - len(data))
        if not packet:
            return None
        data.extend(packet)
    return data
