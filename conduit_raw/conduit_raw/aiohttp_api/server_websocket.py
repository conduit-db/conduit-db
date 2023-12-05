# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.

import logging
from typing import TYPE_CHECKING
import uuid

from aiohttp import web, WSMsgType

if TYPE_CHECKING:
    from .server import ApplicationState


class WSClient(object):
    def __init__(self, websocket_id: str, websocket: web.WebSocketResponse) -> None:
        self.websocket_id = websocket_id
        self.websocket = websocket


class ReferenceServerWebSocket(web.View):
    logger = logging.getLogger("websocket")

    async def get(self) -> web.WebSocketResponse:
        websocket_response = web.WebSocketResponse()
        await websocket_response.prepare(self.request)
        websocket_id = str(uuid.uuid4())

        app_state: ApplicationState = self.request.app["app_state"]
        try:
            client = WSClient(websocket_id, websocket_response)
            app_state.add_ws_client(client)
            self.logger.debug(
                "%s connected, remote_host=%s",
                client.websocket_id,
                self.request.host,
            )

            async for message in client.websocket:
                if message.type == WSMsgType.ERROR:
                    self.logger.error("websocket error", exc_info=message.data)
                else:
                    self.logger.error(
                        "websocket exiting on unwanted incoming message %s",
                        message,
                    )
                    break

            return websocket_response
        finally:
            await websocket_response.close()
            self.logger.debug("removing websocket id: %s", websocket_id)
            app_state.remove_ws_client_by_id(websocket_id)
