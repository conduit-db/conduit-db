import asyncio
import io
import logging
import os
import socket

import MySQLdb

from .conduit_raw_api_client import ConduitRawAPIClient, ServiceUnavailableError
from .utils import is_docker, cast_to_valid_ipv4
from .serializer import Serializer
from .deserializer import Deserializer
from .database.mysql.mysql_database import MySQLDatabase, load_mysql_database


async def wait_for_node(node_host: str, deserializer: Deserializer,
        serializer: Serializer):
    logger = logging.getLogger("wait-for-dependencies")

    # Node
    client = None
    while True:
        is_available = False
        try:
            host = cast_to_valid_ipv4(node_host.split(":")[0])
            port = int(node_host.split(":")[1])
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect((host, port))

            # Send version
            request = serializer.version(recv_host="127.0.0.1", send_host="127.0.0.1")
            client.send(request)

            response = client.recv(10000)
            if not response:
                client.close()
                break

            stream = io.BytesIO(response)
            message_header = deserializer.deserialize_message_header(stream)
            if message_header['command'] == 'version':
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


async def wait_for_mysql(mysql_host: str):
    logger = logging.getLogger("wait-for-dependencies")

    # Node
    client = None
    while True:
        is_available = False
        try:
            host = cast_to_valid_ipv4(mysql_host.split(":")[0])
            port = int(mysql_host.split(":")[1])
            client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            client.connect((host, port))

            # Attempt to connect
            mysql_db: 'MySQLDatabase' = load_mysql_database()
            _result = mysql_db.tables.get_tables()
            is_available = True
            break
        except ConnectionRefusedError:
            # logger.exception("unexpected exception")
            pass
        except MySQLdb.OperationalError:
            # logger.exception("unexpected exception")
            pass
        finally:
            if is_available:
                logger.debug(f"MySQL server on: {mysql_host} is available")
                if client:
                    client.close()
            else:
                logger.debug(f"MySQL server on: {mysql_host} currently unavailable - waiting...")
                await asyncio.sleep(5)


async def wait_for_conduit_raw_api(conduit_raw_api_host):
    """There are currently two components to this:
    1) The HeadersStateServer - which gives notifications about ConduitRaw's current tip
    2) The LMDB database (which should have an API wrapping it)"""
    logger = logging.getLogger("wait-for-dependencies")

    was_waiting = False
    while True:
        try:
            client = ConduitRawAPIClient()
            # This will fail but establishes connectivity & checks to see if the gRPC API
            # can access LMDB without errors
            result = client.ping(1, wait_for_ready=False)
            if result:
                break
        except ServiceUnavailableError:
            was_waiting = True
            logger.debug(f"ConduitRawAPI server on: {conduit_raw_api_host} currently "
                         f"unavailable - waiting...")
            await asyncio.sleep(5)
        except Exception:
            logger.exception("unexpected exception in 'wait_for_conduit_raw_api'")

    logger.info(f"ConduitRawAPI on: {conduit_raw_api_host} is available")
    if was_waiting:
        logger.info(f"Allowing ConduitRaw service to complete initial configuration")
        await asyncio.sleep(3)
        logger.info(f"ConduitRaw service online")
