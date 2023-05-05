import zmq
from zmq.asyncio import Context as AsyncZMQContext

OPTIONS_AFTER_BIND_OR_CONNECT = {
    zmq.SocketOption.SUBSCRIBE,
    zmq.SocketOption.UNSUBSCRIBE,
    zmq.SocketOption.LINGER,
}


def set_options_before(
    zmq_socket: zmq.asyncio.Socket | zmq.Socket[bytes],
    options: list[tuple[zmq.SocketOption, int | bytes]],
) -> None:
    for option_name, option_value in options:
        if option_name not in OPTIONS_AFTER_BIND_OR_CONNECT:
            zmq_socket.setsockopt(option_name, option_value)


def set_options_after(
    zmq_socket: zmq.asyncio.Socket | zmq.Socket[bytes],
    options: list[tuple[zmq.SocketOption, int | bytes]],
) -> None:
    for option_name, option_value in options:
        if option_name in OPTIONS_AFTER_BIND_OR_CONNECT:
            zmq_socket.setsockopt(option_name, option_value)


def bind_async_zmq_socket(
    context: AsyncZMQContext,
    uri: str,
    zmq_socket_type: zmq.SocketType,
    options: list[tuple[zmq.SocketOption, int | bytes]] | None = None,
) -> zmq.asyncio.Socket:
    zmq_socket = context.socket(zmq_socket_type)
    if options:
        set_options_before(zmq_socket, options)
    zmq_socket.bind(uri)
    if options:
        set_options_after(zmq_socket, options)
    return zmq_socket


def connect_async_zmq_socket(
    context: AsyncZMQContext,
    uri: str,
    zmq_socket_type: zmq.SocketType,
    options: list[tuple[zmq.SocketOption, int | bytes]] | None = None,
) -> zmq.asyncio.Socket:
    zmq_socket = context.socket(zmq_socket_type)
    if options:
        set_options_before(zmq_socket, options)
    zmq_socket.connect(uri)
    if options:
        set_options_after(zmq_socket, options)
    return zmq_socket


def bind_non_async_zmq_socket(
    context: zmq.Context[zmq.Socket[bytes]],
    uri: str,
    zmq_socket_type: zmq.SocketType,
    options: list[tuple[zmq.SocketOption, int | bytes]] | None = None,
) -> zmq.Socket[bytes]:
    zmq_socket = context.socket(zmq_socket_type)
    if options:
        set_options_before(zmq_socket, options)
    zmq_socket.bind(uri)
    if options:
        set_options_after(zmq_socket, options)
    return zmq_socket


def connect_non_async_zmq_socket(
    context: zmq.Context[zmq.Socket[bytes]],
    uri: str,
    zmq_socket_type: zmq.SocketType,
    options: list[tuple[zmq.SocketOption, int | bytes]] | None = None,
) -> zmq.Socket[bytes]:
    zmq_socket = context.socket(zmq_socket_type)
    if options:
        set_options_before(zmq_socket, options)
    zmq_socket.connect(uri)
    if options:
        set_options_after(zmq_socket, options)
    return zmq_socket
