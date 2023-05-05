"""
These are separated from `utils.py` because loading environment variables from the `.env` file
must occur before importing any other files such as `constants.py` which in turn checks for the
environment variable `NETWORK`. `utils.py` imports many other files including `constants.py`
which relies on environment variables already being set.
"""
import ipaddress
import logging
import os
import socket
import time
from pathlib import Path

logger = logging.getLogger("conduit-lib-startup-utils")
logger.setLevel(logging.DEBUG)


def is_docker() -> bool:
    path = "/proc/self/cgroup"
    return (
        os.path.exists("/.dockerenv") or os.path.isfile(path) and any("docker" in line for line in open(path))
    )


def load_dotenv(dotenv_path: Path) -> None:
    with open(dotenv_path, "r") as f:
        lines = f.readlines()
        for line in lines:
            if line.startswith("#") or line.strip() == "":
                continue

            # Split line on "=" symbol but need to take care of base64 encoded string values.
            split_line = line.strip().split("=")
            key = split_line[0]
            val = split_line[1] + "".join(["=" + part for part in split_line[2:]])
            # Does not override pre-existing environment variables
            if not os.environ.get(key):
                os.environ[key] = val


def cast_to_valid_ipv4(ipv4: str) -> str:
    try:
        ipaddress.ip_address(ipv4)
        return ipv4
    except ValueError:
        # Need to resolve dns name to get ipv4
        try:
            ipv4 = socket.gethostbyname(ipv4)
        except socket.gaierror:
            logger.error(f"Failed to resolve ip address for hostname: {ipv4}")
            time.sleep(0.2)
        return ipv4


def resolve_hosts_and_update_env_vars() -> None:
    for key in os.environ:
        if key in {"MYSQL_HOST", "NODE_HOST", "IPC_SOCKET_SERVER_HOST"}:
            logger.debug(f"Checking environment variable: {key}")
            logger.debug(f"os.environ[key] before: {os.environ[key]}")
            # Resolve the IP address (particularly important for docker container names)
            host = cast_to_valid_ipv4(os.environ[key].split(":")[0])
            os.environ[key] = host
            logger.debug(f"os.environ[key] after: {os.environ[key]}")
