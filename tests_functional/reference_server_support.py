from __future__ import annotations

import asyncio
import enum
import json
import logging
import struct
from http import HTTPStatus

import aiohttp
from bitcoinx import PrivateKey, hash_to_hex_str
from datetime import datetime
from typing import TypedDict, cast, NamedTuple, Any

from conduit_lib.database.db_interface.tip_filter_types import output_spend_struct
from tests_functional.data.pushdata_registrations import PUSHDATAS
from tests_functional.data.utxo_spends import UTXO_REGISTRATIONS

logger = logging.getLogger("conftest")

REFERENCE_SERVER_URL = "http://localhost:47124/"
CLIENT_IDENTITY_PRIVATE_KEY_HEX = "d468816bc0f78465d4833426c280166c3810ecc9c0350c5232b0c417687fbde6"
CLIENT_IDENTITY_PRIVATE_KEY = PrivateKey.from_hex(CLIENT_IDENTITY_PRIVATE_KEY_HEX)

NO_BLOCK_HASH = bytes(32)


class BadServerError(Exception):
    pass


class ServerConnectionError(Exception):
    pass


class AccountMessageKind(enum.IntEnum):
    PEER_CHANNEL_MESSAGE = 1
    SPENT_OUTPUT_EVENT = 2


class ChannelNotification(TypedDict):
    id: str
    notification: str


class VerifiableKeyDataDict(TypedDict):
    public_key_hex: str
    signature_hex: str
    message_hex: str


class OutputSpend(NamedTuple):
    out_tx_hash: bytes
    out_index: int
    in_tx_hash: bytes
    in_index: int
    block_hash: bytes | None

    @classmethod
    def from_network(
        cls,
        out_tx_hash: bytes,
        out_index: int,
        in_tx_hash: bytes,
        in_index: int,
        block_hash: bytes | None,
    ) -> OutputSpend:
        """
        Convert the binary representation to the Python representation.
        """
        if block_hash == NO_BLOCK_HASH:
            block_hash = None
        return OutputSpend(out_tx_hash, out_index, in_tx_hash, in_index, block_hash)

    def __repr__(self) -> str:
        return (
            f'OutputSpend(out_tx_hash="{hash_to_hex_str(self.out_tx_hash)}", '
            f'out_index={self.out_index}, in_tx_hash="{hash_to_hex_str(self.in_tx_hash)}", '
            f"in_index={self.in_index}, block_hash="
            + (f'"{hash_to_hex_str(self.block_hash)}"' if self.block_hash else "None")
            + ")"
        )


def _generate_client_key_data() -> VerifiableKeyDataDict:
    iso_date_text = datetime.utcnow().isoformat()
    message_bytes = b"http://server/api/account/metadata" + iso_date_text.encode()
    signature_bytes = CLIENT_IDENTITY_PRIVATE_KEY.sign_message(message_bytes)
    return {
        "public_key_hex": CLIENT_IDENTITY_PRIVATE_KEY.public_key.to_hex(),
        "message_hex": message_bytes.hex(),
        "signature_hex": signature_bytes.hex(),
    }


def get_posix_timestamp() -> int:
    # In theory we can just return `int(time.time())` but this returns the posix timestamp and
    # try reading the documentation for `time.time` and being sure of that.
    return int(datetime.now().timestamp())


class AccountRegisteredDict(TypedDict):
    public_key_hex: str
    api_key: str


async def setup_reference_server_account() -> str:
    obtain_server_key_url = f"{REFERENCE_SERVER_URL}api/v1/account/register"

    timestamp_text = datetime.utcnow().isoformat()
    message_text = f"{obtain_server_key_url} {timestamp_text}"
    identity_private_key = CLIENT_IDENTITY_PRIVATE_KEY
    signature_bytes = identity_private_key.sign_message(message_text.encode())
    key_data: VerifiableKeyDataDict = {
        "public_key_hex": identity_private_key.public_key.to_hex(),
        "signature_hex": signature_bytes.hex(),
        "message_hex": message_text.encode().hex(),
    }

    api_key: str | None = None
    async with aiohttp.ClientSession() as session:
        async with session.post(obtain_server_key_url, json=key_data) as response:
            if response.status != HTTPStatus.OK:
                logger.error(
                    "Unexpected status in payment key endpoint response (vkd) %d (%s)",
                    response.status,
                    response.reason,
                )
                raise aiohttp.ClientError(
                    f"Bad response status code: {response.status}, reason: {response.reason}"
                )

            response_value = await response.json()

        assert isinstance(response_value, dict)
        assert len(response_value) == 2
        assert set(response_value) == {"public_key_hex", "api_key"}

        register_dict = cast(AccountRegisteredDict, response_value)
        assert register_dict["public_key_hex"] == key_data["public_key_hex"]

        logger.debug(f"api_key={api_key}")
        return register_dict["api_key"]


async def create_tip_filter_peer_channel(api_key: str) -> tuple[str, str]:
    create_peer_channel_uri = "http://localhost:47124/api/v1/channel/manage"
    body = {
        "public_read": True,
        "public_write": True,
        "sequenced": True,
        "retention": {"min_age_days": 0, "max_age_days": 0, "auto_prune": True},
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(create_peer_channel_uri, json=body, headers=headers) as response:
            if response.status != HTTPStatus.OK:
                raise aiohttp.ClientError(
                    f"Bad response status code: {response.status}, reason: {response.reason}"
                )
            result = await response.json()
            owner_token: str = result["access_tokens"][0]["token"]

    tipFilterCallbackUrl = f"http://127.0.0.1:47124/api/v1/channel/{result['id']}"
    tipFilterCallbackToken = f"Bearer {owner_token}"
    return tipFilterCallbackUrl, tipFilterCallbackToken


class IndexerServerSettings(TypedDict):
    tipFilterCallbackUrl: str | None
    tipFilterCallbackToken: str | None


async def register_tip_filter_settings(
    api_key: str, indexer_settings: IndexerServerSettings
) -> IndexerServerSettings:
    uri = REFERENCE_SERVER_URL + "api/v1/indexer"
    headers = {"Authorization": f"Bearer {api_key}"}
    async with aiohttp.ClientSession() as session:
        async with session.post(uri, json=indexer_settings, headers=headers) as response:
            if response.status != HTTPStatus.OK:
                raise aiohttp.ClientError()

            return cast(IndexerServerSettings, await response.json())


async def register_for_utxo_notifications(
    api_key: str,
) -> list[tuple[str, int]]:
    uri = REFERENCE_SERVER_URL + "api/v1/output-spend/notifications"
    headers = {"Authorization": f"Bearer {api_key}"}
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.post(uri, json=UTXO_REGISTRATIONS, headers=headers) as response:
                    if response.status != HTTPStatus.OK:
                        logger.error(response.reason)
                        raise aiohttp.ClientError(response.reason)

                    return cast(list[tuple[str, int]], await response.json())
            except aiohttp.ClientError:
                logger.debug(f"Reference server is not ready yet. Retrying in 5 seconds")
                await asyncio.sleep(5)


async def delete_peer_channel_message_async(remote_channel_id: str, access_token: str, sequence: int) -> None:
    """
    Use the reference peer channel implementation API for deleting a message in a peer channel.

    Raises `GeneralAPIError` if a connection was established but the request was unsuccessful.
    Raises `ServerConnectionError` if the remote computer does not accept the connection.
    """
    uri = REFERENCE_SERVER_URL + f"api/v1/channel/{remote_channel_id}/{sequence}"
    headers = {"Authorization": access_token}
    async with aiohttp.ClientSession() as session:
        async with session.delete(uri, headers=headers) as response:
            if response.status != HTTPStatus.OK:
                raise aiohttp.ClientError(
                    f"Bad response status code: {response.status}, reason: {response.reason}"
                )


async def register_for_pushdata_notifications(
    api_key: str,
) -> list[tuple[str, int]]:
    uri = REFERENCE_SERVER_URL + "api/v1/transaction/filter"
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.post(
                    uri,
                    data=json.dumps(PUSHDATAS).encode("utf-8"),
                    headers=headers,
                ) as response:
                    if response.status != HTTPStatus.OK:
                        logger.error(response.reason)
                        raise aiohttp.ClientError(response.reason)

                    result = cast(list[tuple[str, int]], await response.json())
                    logger.debug(f"Pushdata registration result: {result}")
                    return result
            except aiohttp.ClientError:
                logger.debug(f"Reference server is not ready yet. Retrying in 5 seconds")
                await asyncio.sleep(5)


async def setup_reference_server_tip_filtering():
    while True:
        try:
            api_key = await setup_reference_server_account()
            break
        except aiohttp.ClientError:
            logger.debug(f"Reference server is not yet ready. Retrying in 5 seconds")
            await asyncio.sleep(5)
        except aiohttp.ServerDisconnectedError:
            logger.debug(f"Reference server is not yet ready. Retrying in 5 seconds")
            await asyncio.sleep(5)

    (
        tipFilterCallbackUrl,
        tipFilterCallbackToken,
    ) = await create_tip_filter_peer_channel(api_key)
    indexer_settings = IndexerServerSettings(
        tipFilterCallbackUrl=tipFilterCallbackUrl,
        tipFilterCallbackToken=tipFilterCallbackToken,
    )
    indexer_settings = await register_tip_filter_settings(api_key, indexer_settings)

    logger.debug(indexer_settings)
    return indexer_settings, api_key


def process_reference_server_message_bytes(
    message_bytes: bytes,
) -> tuple[AccountMessageKind, ChannelNotification | OutputSpend]:
    try:
        message_kind_value: int = struct.unpack_from(">I", message_bytes, 0)[0]
    except (TypeError, struct.error):
        # `struct.error`: The bytes to be unpacked do not start with valid data for the requested
        #   type.
        raise BadServerError("Received an invalid message type")

    try:
        message_kind = AccountMessageKind(message_kind_value)
    except ValueError:
        # `ValueError`: The value is not a member of the enum.
        raise BadServerError(f"Received an unknown message type ({message_kind_value})")

    if message_kind == AccountMessageKind.PEER_CHANNEL_MESSAGE:
        try:
            message = json.loads(message_bytes[4:].decode("utf-8"))
        except (TypeError, json.decoder.JSONDecodeError):
            raise BadServerError(
                "Received an invalid peer channel message from a "
                "server you are using for peer channels (cannot decode as JSON)"
            )

        # Verify that this at least looks like a valid `ChannelNotification` message.
        if (
            not isinstance(message, dict)
            or len(message) != 2
            or not isinstance(message.get("id", None), str)
            or not isinstance(message.get("notification", None), str)
        ):
            raise BadServerError(
                "Received an invalid peer channel message from a "
                "server you are using (unrecognised structure)"
            )

        return message_kind, cast(ChannelNotification, message)
    elif message_kind == AccountMessageKind.SPENT_OUTPUT_EVENT:
        try:
            spent_output_fields = output_spend_struct.unpack_from(message_bytes, 4)
        except (TypeError, struct.error):
            # `TypeError`: This is raised when the arguments passed are not one bytes object.
            # `struct.error`: This is raised when the bytes object is invalid, whether not long
            #     enough or of incompatible types.
            raise BadServerError(
                "Received an invalid blockchain services message "
                "from a server you are using (unable to decode)"
            )

        return message_kind, OutputSpend.from_network(*spent_output_fields)
    else:
        # If this ever happens it is because the programmer who added a new entry to
        # `AccountMessageKind` did not hook it up here.
        raise NotImplementedError(f"Packing message kind {message_kind} is unsupported")


class GenericPeerChannelMessage(TypedDict):
    sequence: int
    received: str
    content_type: str
    payload: Any


async def list_peer_channel_messages_async(
    url: str, access_token: str, unread_only: bool = True
) -> list[GenericPeerChannelMessage]:
    headers = {"Authorization": access_token, "Accept": "application/json"}
    query_parameters: dict[str, str] = {}
    if unread_only:
        query_parameters["unread"] = "true"
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers, params=query_parameters) as response:
                if response.status != HTTPStatus.OK:
                    raise aiohttp.ClientError(
                        f"Bad response status code: {response.status}, reason: {response.reason}"
                    )
                return cast(list[GenericPeerChannelMessage], await response.json())
    except aiohttp.ClientError:
        # NOTE(exception-details) We log this because we are not sure yet that we do not need
        #     this detail. At a later stage if we are confident that all the exceptions here
        #     are reasonable and expected, we can remove this.
        logger.debug(
            "Wrapped aiohttp exception (do we need to preserve this?)",
            exc_info=True,
        )
        raise ServerConnectionError(f"Unable to establish server connection: {url}")
