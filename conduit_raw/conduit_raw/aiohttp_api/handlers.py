from __future__ import annotations

import os
import subprocess
from datetime import datetime, timedelta
import logging
from typing import Any, Dict, Optional, TYPE_CHECKING

import aiohttp
from aiohttp import web

from .constants import SERVER_HOST, SERVER_PORT


if TYPE_CHECKING:
    from .server import ApplicationState


logger = logging.getLogger('handlers')


async def ping(request: web.Request) -> web.Response:
    return web.Response(text="true")


async def error(request: web.Request) -> web.Response:
    raise ValueError("This is a test of raising an exception in the handler")

