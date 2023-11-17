import asyncio
import io
import logging
import os
import socket
import time

import MySQLdb
from cassandra.cluster import NoHostAvailable  # pylint:disable=E0611

from . import DBInterface
from .ipc_sock_client import IPCSocketClient, ServiceUnavailableError
from conduit_lib.deserializer import Deserializer
from conduit_lib.serializer import Serializer


async def wait_for_node(
    node_host: str,
    node_port: int,
    deserializer: Deserializer,
    serializer: Serializer,
) -> None:
    logger = logging.getLogger("wait-for-dependencies")

    # Node
    client = None
    while True:
        is_available = False
        try:
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect((node_host, node_port))

            # Send version
            request = serializer.version(recv_host="127.0.0.1", send_host="127.0.0.1")
            client.send(request)

            response = client.recv(10000)
            if not response:
                client.close()
                break

            stream = io.BytesIO(response)
            message_header = deserializer.deserialize_message_header(stream)
            if message_header["command"] == "version":
                is_available = True
                break
        except ConnectionRefusedError:
            pass
        except socket.gaierror:
            pass
        finally:
            if is_available:
                logger.debug(f"Bitcoin node on: {node_host} is available")
                if client:
                    client.close()
            else:
                logger.debug(f"Bitcoin node on: {node_host} currently unavailable - waiting...")
                await asyncio.sleep(5)


def wait_for_db() -> None:
    logger = logging.getLogger("wait-for-dependencies")

    # Node
    while True:
        is_available = False
        try:
            # Attempt to connect
            db = DBInterface.load_db()
            _result = db.get_tables()
            is_available = True
            break
        except ConnectionRefusedError:
            # logger.exception("unexpected exception")
            pass
        except NoHostAvailable:
            # logger.exception("No ScyllaDB host available")
            pass
        except MySQLdb.OperationalError:
            # logger.exception("unexpected exception")
            pass
        finally:
            if is_available:
                logger.debug(f"Database server is available")
            else:
                logger.debug(f"Database server currently unavailable - waiting...")
                time.sleep(5)


def wait_for_ipc_socket_server() -> None:
    """There are currently two components to this:
    1) The HeadersStateServer - which gives notifications about ConduitRaw's current tip
    2) The LMDB database (which should have an API wrapping it)"""
    logger = logging.getLogger("wait-for-dependencies")
    host: str = os.environ.get("IPC_SOCKET_SERVER_HOST", "127.0.0.1")
    port: int = int(os.environ.get("IPC_SOCKET_SERVER_PORT", "50000"))

    was_waiting = False
    while True:
        try:
            client = IPCSocketClient()
            # This will fail but establishes connectivity & checks to see if the gRPC API
            # can access LMDB without errors
            result = client.ping()
            if result:
                break
        except ServiceUnavailableError:
            was_waiting = True
            logger.debug(
                f"ConduitRawAPI server on: http://{host}:{port} currently " f"unavailable - waiting..."
            )
            time.sleep(5)
        except Exception:
            logger.exception("unexpected exception in 'wait_for_ipc_socket_server'")

    logger.info(f"ConduitRawAPI on:  http://{host}:{port} is available")
    if was_waiting:
        logger.info(f"Allowing ConduitRaw service to complete initial configuration")
        time.sleep(3)
        logger.info(f"ConduitRaw service online")
