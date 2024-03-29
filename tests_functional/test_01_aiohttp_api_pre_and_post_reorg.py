# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.

import asyncio
import base64
import json
import logging
import os
import queue
import sys
import threading
import time
from pathlib import Path
from typing import cast

import aiohttp
import pytest
import requests
from bitcoinx import hash_to_hex_str

from conduit_lib import DBInterface
from conduit_lib.utils import create_task
from conduit_raw.conduit_raw.aiohttp_api.types import (
    TipFilterNotificationMatch,
    TipFilterPushDataMatchesData,
)
from contrib.scripts.import_blocks import import_blocks
from tests_functional import utils
import tests_functional._pre_reorg_data as pre_reorg_test_data
from tests_functional.data.expected_tip_filter_results import (
    PUSHDATA_TO_OUTPOINT_MAP,
)
from tests_functional.data.utxo_spends import UTXO_REGISTRATIONS
from tests_functional.reference_server_support import (
    process_reference_server_message_bytes,
    OutputSpend,
    IndexerServerSettings,
    AccountMessageKind,
    list_peer_channel_messages_async,
    setup_reference_server_tip_filtering,
    register_for_utxo_notifications,
    register_for_pushdata_notifications,
    delete_peer_channel_message_async,
    GenericPeerChannelMessage,
    NotificationJsonData,
)
from tests_functional.utils import GET_TRANSACTION_URL, GET_HEADERS_TIP_URL

MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
REGTEST_TEST_BLOCKCHAIN = MODULE_DIR.parent / "contrib" / "blockchains" / "blockchain_116_7c9cd2"
REGTEST_TEST_BLOCKCHAIN_REORG = MODULE_DIR.parent / "contrib" / "blockchains" / "blockchain_118_0ebc17"

BASE_URL = f"http://127.0.0.1:34525"
PING_URL = BASE_URL + "/"
ERROR_URL = BASE_URL + "/error"

logger = logging.getLogger("test-internal-aiohttp-api")


async def listen_for_notifications_task_async(
    websocket_connected_event,
    output_spend_result_queue: queue.Queue,
    tip_filter_matches_queue: queue.Queue,
    tip_filter_callback_url: str,
    owner_token: str,
    api_key: str,
) -> None:
    websocket_url_template = "http://localhost:47124/" + "api/v1/web-socket?token={access_token}"
    websocket_url = websocket_url_template.format(access_token=api_key)
    headers = {"Accept": "application/octet-stream"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.ws_connect(websocket_url, headers=headers, timeout=5.0) as server_websocket:
                logger.info("Connected to server websocket, url=%s", websocket_url_template)
                websocket_connected_event.set()
                websocket_message: aiohttp.WSMessage
                async for websocket_message in server_websocket:
                    if websocket_message.type == aiohttp.WSMsgType.TEXT:
                        message = json.loads(websocket_message.data)
                        message_type = message["message_type"]
                        assert message_type == "bsvapi.headers.tip"
                        logger.debug(f"Got new headers tip: {message['result']}")

                    elif websocket_message.type == aiohttp.WSMsgType.BINARY:
                        # In processing the message `BadServerError` will be raised if this
                        # server cannot handle the incoming message, or it is malformed.
                        message_bytes = cast(bytes, websocket_message.data)
                        (
                            message_kind,
                            message,
                        ) = process_reference_server_message_bytes(message_bytes)
                        logger.debug(
                            f"Got binary websocket message: message_kind={message_kind}. "
                            f"message: {message}"
                        )

                        if message_kind == AccountMessageKind.PEER_CHANNEL_MESSAGE:
                            channel_message = cast(NotificationJsonData, message)
                            logger.debug(
                                "Queued incoming peer channel message %s",
                                channel_message,
                            )
                            # This is essentially ElectrumSV's
                            # `process_incoming_peer_channel_messages_async` inlined

                            messages = await list_peer_channel_messages_async(
                                tip_filter_callback_url,
                                owner_token,
                                unread_only=False,
                            )
                            if len(messages) == 0:
                                # This may happen legitimately if we had several new message notifications backlogged
                                # for the same channel, but processing a leading notification picks up the messages
                                # for the trailing notification.
                                logger.debug(
                                    "Asked tip filter channel %s for new messages and received none",
                                    tip_filter_callback_url,
                                )
                                continue
                            tip_filter_matches_queue.put_nowait(messages)
                            for message in messages:
                                await delete_peer_channel_message_async(
                                    channel_message["channel_id"],
                                    owner_token,
                                    message["sequence"],
                                )
                        elif message_kind == AccountMessageKind.SPENT_OUTPUT_EVENT:
                            spent_output_message = cast(OutputSpend, message)
                            logger.debug("Queued incoming output spend message")
                            output_spend_result_queue.put_nowait([spent_output_message])
                        else:
                            logger.error(
                                "Unhandled binary server websocket message %r",
                                websocket_message,
                            )
                    elif websocket_message.type in (
                        aiohttp.WSMsgType.CLOSE,
                        aiohttp.WSMsgType.ERROR,
                        aiohttp.WSMsgType.CLOSED,
                        aiohttp.WSMsgType.CLOSING,
                    ):
                        logger.info("Server websocket closed")
                        break
                    else:
                        logger.error(
                            "Unhandled server websocket message type %r",
                            websocket_message,
                        )
    except Exception:
        logger.exception("unexpected exception in `listen_for_notifications_task_async`")


async def spawn_tasks(
    output_spend_result_queue: queue.Queue,
    tip_filter_matches_queue: queue.Queue,
    tip_filter_callback_url: str,
    owner_token: str,
    api_key: str,
    registrations_complete_event: threading.Event,
):
    websocket_connected_event = asyncio.Event()
    task = create_task(
        listen_for_notifications_task_async(
            websocket_connected_event,
            output_spend_result_queue,
            tip_filter_matches_queue,
            tip_filter_callback_url,
            owner_token,
            api_key,
        )
    )
    await websocket_connected_event.wait()
    await register_for_utxo_notifications(api_key)
    await register_for_pushdata_notifications(api_key)
    registrations_complete_event.set()
    await task


def listen_for_notifications_thread(
    loop: asyncio.AbstractEventLoop,
    output_spend_result_queue: queue.Queue,
    tip_filter_matches_queue: queue.Queue,
    tip_filter_callback_url: str,
    owner_token: str,
    api_key: str,
    registrations_complete_event: threading.Event,
) -> None:
    """Launches the ESV-Reference-Server to run in the background but with a test database"""
    try:
        logging.basicConfig(level=logging.DEBUG)
        loop.run_until_complete(
            spawn_tasks(
                output_spend_result_queue,
                tip_filter_matches_queue,
                tip_filter_callback_url,
                owner_token,
                api_key,
                registrations_complete_event,
            )
        )
        sys.exit(0)
    except KeyboardInterrupt:
        logger.debug("Notification listener stopped")
    except Exception:
        logger.exception("Unexpected exception in notification listener thread")


class TestAiohttpRESTAPI:
    logger = logging.getLogger("TestAiohttpRESTAPI")

    def setup_class(self) -> None:
        # Need to set the environment variable DEFAULT_DB_TYPE=MYSQL or DEFAULT_DB_TYPE=SCYLLADB
        # prior to running all of these tests. This goes for the running services too

        os.environ['SCYLLA_KEYSPACE'] = os.getenv('SCYLLA_KEYSPACE', 'condiutdbtest')
        os.environ['DEFAULT_DB_TYPE'] = os.getenv('DEFAULT_DB_TYPE', 'SCYLLADB')

        db = DBInterface.load_db(worker_id="test")
        db.create_permanent_tables()
        db.tip_filter_api.create_tables()

        loop = asyncio.get_event_loop()
        indexer_settings: IndexerServerSettings
        api_key: str
        tip_filter_callback_url, owner_token, api_key = loop.run_until_complete(
            setup_reference_server_tip_filtering()
        )

        self.tip_filter_matches_queue = queue.Queue()
        self.output_spend_result_queue = queue.Queue()
        self.registrations_complete_event = threading.Event()

        thread = threading.Thread(
            target=listen_for_notifications_thread,
            args=[
                loop,
                self.output_spend_result_queue,
                self.tip_filter_matches_queue,
                tip_filter_callback_url,
                owner_token,
                api_key,
                self.registrations_complete_event,
            ],
            daemon=True,
        )
        thread.start()
        self.registrations_complete_event.wait()
        logger.info(f"Registrations done!")

        blockchain_dir = REGTEST_TEST_BLOCKCHAIN
        import_blocks(str(blockchain_dir))
        time.sleep(15)


    def setup_method(self) -> None:
        pass

    def teardown_method(self) -> None:
        pass

    @classmethod
    def teardown_class(klass) -> None:
        pass

    def test_ping(klass) -> None:
        result = requests.get(PING_URL)
        assert result.json() is True

    def test_error(klass) -> None:
        result = requests.get(ERROR_URL)
        assert result.status_code == 400, result.reason
        assert result.reason is not None
        assert isinstance(result.reason, str)

    def test_headers_tip(self) -> None:
        headers = {"Accept": "application/json"}
        result = requests.get(GET_HEADERS_TIP_URL, headers=headers)
        assert result.status_code == 200
        assert len(result.json()) == 1
        assert result.json()[0]['height'] == 116

    @pytest.mark.timeout(20)
    def test_utxo_notifications(self) -> None:
        expected_utxo_spends = set([tuple(utxo) for utxo in UTXO_REGISTRATIONS])
        len_expected_utxo_spends = len(expected_utxo_spends)
        for _ in range(len_expected_utxo_spends):
            message = self.output_spend_result_queue.get()
            logger.debug(f"Got output spend message from queue: {message}")
            output_spend_obj: OutputSpend = message[0]
            utxo = (
                hash_to_hex_str(output_spend_obj.out_tx_hash),
                output_spend_obj.out_index,
            )
            assert utxo in expected_utxo_spends
            expected_utxo_spends.remove(utxo)
        assert len(expected_utxo_spends) == 0

    @pytest.mark.timeout(20)
    def test_pushdata_notifications(self) -> None:
        expected_count = 5
        count = 0
        while True:
            try:
                messages: list[GenericPeerChannelMessage] = self.tip_filter_matches_queue.get_nowait()
                message: GenericPeerChannelMessage
                for message in messages:
                    payload = base64.b64decode(message["payload"])
                    pushdata_notification = cast(TipFilterPushDataMatchesData, json.loads(payload))
                    match: TipFilterNotificationMatch
                    for match in pushdata_notification["matches"]:
                        count += 1
                        pushdata_hash_hex = match["pushDataHashHex"]
                        output_tx_hash = match["transactionId"]
                        output_idx = match["transactionIndex"]
                        outpoint = PUSHDATA_TO_OUTPOINT_MAP[pushdata_hash_hex]
                        assert hash_to_hex_str(outpoint.tx_hash) == output_tx_hash
                        assert outpoint.out_idx == output_idx
                    logger.debug(f"Got pushdata_notification: {pushdata_notification} (total_count={count})")

                if count == expected_count:
                    break
            except queue.Empty:
                logger.debug(f"tip_filter_matches_queue queue empty, waiting for more")
                time.sleep(1)

    def test_get_transaction_json(self) -> None:
        headers = {"Accept": "application/json"}
        for txid, rawtx_hex in pre_reorg_test_data.TRANSACTIONS.items():
            result = requests.get(GET_TRANSACTION_URL.format(txid=txid), headers=headers)
            assert result.status_code == 200, f"reason={result.reason}, txid={txid}"
            assert result.json() == rawtx_hex

    def test_get_transaction_binary(self) -> None:
        headers = {"Accept": "application/octet-stream"}
        for txid, rawtx_hex in pre_reorg_test_data.TRANSACTIONS.items():
            result = requests.get(GET_TRANSACTION_URL.format(txid=txid), headers=headers)
            assert result.status_code == 200, result.reason
            assert result.content == bytes.fromhex(rawtx_hex)

    def test_get_tsc_merkle_proof_json(self) -> None:
        utils._get_tsc_merkle_proof_target_hash_json()
        utils._get_tsc_merkle_proof_target_merkleroot_json()
        utils._get_tsc_merkle_proof_target_header_json()
        utils._get_tsc_merkle_proof_include_rawtx_json()

    def test_pushdata_no_match_json(self) -> None:
        utils._pushdata_no_match_json()

    def test_mining_txs_json(self) -> None:
        utils._mining_txs_json_post_reorg()

    def test_p2pk_json(self) -> None:
        utils._p2pk_json(post_reorg=False)

    def test_p2pkh_json(self) -> None:
        utils._p2pkh_json(post_reorg=False)

    def test_p2sh_json(self) -> None:
        utils._p2sh_json(post_reorg=False)

    def test_p2ms_json(self) -> None:
        utils._p2ms_json(post_reorg=False)

    def test_p2ms2_json(self) -> None:
        utils._p2ms2_json(post_reorg=False)

    def test_submit_reorg_blocks(self) -> None:
        blockchain_dir = REGTEST_TEST_BLOCKCHAIN_REORG
        import_blocks(str(blockchain_dir))
        time.sleep(10)
        assert True

    def test_headers_tip_post_reorg(self) -> None:
        headers = {"Accept": "application/json"}
        result = requests.get(GET_HEADERS_TIP_URL, headers=headers)
        assert result.status_code == 200
        assert len(result.json()) == 2
        tip_116_found = False
        tip_118_found = False
        while True:
            self.logger.debug(f"result.json(): {result.json()}")
            for tip in result.json():
                if tip['height'] == 116:
                    assert tip['header']['hash'] == '7c9cd212920833de9623107c72331c69acacd1964fdd2310f0f608f1e3bee4f4'
                    assert tip['state'] == 'STALE'
                    tip_116_found = True
                if tip['height'] == 118:
                    assert tip['header']['hash'] == '0ebc17feeca04b3f37d4f50b0966ffafe012a0d928b47cf9c2f16431815d6351'
                    assert tip['state'] == 'LONGEST_CHAIN'
                    tip_118_found = True
            if not (tip_116_found and tip_118_found):
                time.sleep(2)
                continue
            else:
                break

    @pytest.mark.timeout(10)
    def test_utxo_notifications_post_reorg(self) -> None:
        # TODO test re-registration and initial status fetch
        expected_utxo_spends = set([tuple(utxo) for utxo in UTXO_REGISTRATIONS])
        len_expected_utxo_spends = len(expected_utxo_spends)
        for _ in range(len_expected_utxo_spends):
            message = self.output_spend_result_queue.get()
            logger.debug(f"Got output spend message from queue: {message}")
            output_spend_obj: OutputSpend = message[0]
            utxo = (
                hash_to_hex_str(output_spend_obj.out_tx_hash),
                output_spend_obj.out_index,
            )
            assert utxo in expected_utxo_spends
            expected_utxo_spends.remove(utxo)
        assert len(expected_utxo_spends) == 0

    @pytest.mark.timeout(20)
    def test_pushdata_notifications_post_reorg(self) -> None:
        expected_count = 5
        count = 0
        while True:
            try:
                messages: list[GenericPeerChannelMessage] = self.tip_filter_matches_queue.get_nowait()
                message: GenericPeerChannelMessage
                for message in messages:
                    payload = base64.b64decode(message["payload"])
                    pushdata_notification = cast(TipFilterPushDataMatchesData, json.loads(payload))
                    logger.debug(f"Got pushdata_notification: {pushdata_notification}")
                    match: TipFilterNotificationMatch
                    for match in pushdata_notification["matches"]:
                        count += 1
                        pushdata_hash_hex = match["pushDataHashHex"]
                        output_tx_hash = match["transactionId"]
                        output_idx = match["transactionIndex"]
                        outpoint = PUSHDATA_TO_OUTPOINT_MAP[pushdata_hash_hex]
                        assert hash_to_hex_str(outpoint.tx_hash) == output_tx_hash
                        assert outpoint.out_idx == output_idx

                if count == expected_count:
                    break
            except queue.Empty:
                logger.debug(f"tip_filter_matches_queue queue empty, waiting for more")
                time.sleep(1)

    def test_get_tsc_merkle_proof_json_post_reorg(self) -> None:
        utils._get_tsc_merkle_proof_target_hash_json(post_reorg=True)
        utils._get_tsc_merkle_proof_target_merkleroot_json(post_reorg=True)
        utils._get_tsc_merkle_proof_target_header_json(post_reorg=True)
        utils._get_tsc_merkle_proof_include_rawtx_json(post_reorg=True)

    def test_pushdata_no_match_json_post_reorg(self) -> None:
        utils._pushdata_no_match_json()

    def test_mining_txs_json_post_reorg(self) -> None:
        utils._mining_txs_json_post_reorg()

    def test_p2pk_json_post_reorg(self) -> None:
        utils._p2pk_json(post_reorg=True)

    def test_p2pkh_json_post_reorg(self) -> None:
        utils._p2pkh_json(post_reorg=True)

    def test_p2sh_json_post_reorg(self) -> None:
        utils._p2sh_json(post_reorg=True)

    def test_p2ms_json_post_reorg(self) -> None:
        utils._p2ms_json(post_reorg=True)

    def test_p2ms2_json_post_reorg(self) -> None:
        utils._p2ms2_json(post_reorg=True)
