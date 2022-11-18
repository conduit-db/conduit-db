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

from conduit_lib.utils import create_task
from conduit_raw.conduit_raw.aiohttp_api.types import TipFilterNotificationMatch, \
    TipFilterPushDataMatchesData
from contrib.scripts.import_blocks import import_blocks
from tests_functional import utils
import tests_functional._pre_reorg_data as pre_reorg_test_data
from tests_functional.data.expected_tip_filter_results import PUSHDATA_TO_OUTPOINT_MAP
from tests_functional.data.utxo_spends import UTXO_REGISTRATIONS
from tests_functional.reference_server_support import process_reference_server_message_bytes, \
    OutputSpend, IndexerServerSettings, AccountMessageKind, ChannelNotification, \
    list_peer_channel_messages_async, setup_reference_server_tip_filtering, \
    register_for_utxo_notifications, register_for_pushdata_notifications, \
    delete_peer_channel_message_async, GenericPeerChannelMessage

BASE_URL = f"http://127.0.0.1:34525"
PING_URL = BASE_URL + "/"
ERROR_URL = BASE_URL + "/error"
GET_TRANSACTION_URL = BASE_URL + "/api/v1/transaction/{txid}"
GET_MERKLE_PROOF_URL = BASE_URL + "/api/v1/merkle-proof/{txid}"
RESTORATION_URL = BASE_URL + "/api/v1/restoration/search"

STREAM_TERMINATION_BYTE = b"\x00"

MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))

logger = logging.getLogger("test-internal-aiohttp-api")


async def listen_for_notifications_task_async(websocket_connected_event,
        output_spend_result_queue: queue.Queue, tip_filter_matches_queue: queue.Queue,
        indexer_settings: IndexerServerSettings,
        api_key: str) -> None:
    access_token = api_key
    websocket_url_template = "http://localhost:47124/" + "api/v1/web-socket?token={access_token}"
    websocket_url = websocket_url_template.format(access_token=access_token)
    headers = {
        "Accept": "application/octet-stream"
    }
    async with aiohttp.ClientSession() as session:
        async with session.ws_connect(websocket_url, headers=headers, timeout=5.0) \
                as server_websocket:
            logger.info('Connected to server websocket, url=%s', websocket_url_template)
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
                    message_kind, message = process_reference_server_message_bytes(message_bytes)

                    if message_kind == AccountMessageKind.PEER_CHANNEL_MESSAGE:
                        channel_message = cast(ChannelNotification, message)
                        logger.debug("Queued incoming peer channel message %s", channel_message)
                        # This is essentially ElectrumSV's
                        # `process_incoming_peer_channel_messages_async` inlined

                        messages = await list_peer_channel_messages_async(
                            indexer_settings['tipFilterCallbackUrl'],
                            indexer_settings['tipFilterCallbackToken'], unread_only=False)
                        if len(messages) == 0:
                            # This may happen legitimately if we had several new message notifications backlogged
                            # for the same channel, but processing a leading notification picks up the messages
                            # for the trailing notification.
                            logger.debug("Asked tip filter channel %s for new messages and received none",
                                indexer_settings['tipFilterCallbackUrl'])
                            continue
                        tip_filter_matches_queue.put_nowait(messages)
                        for message in messages:
                            await delete_peer_channel_message_async(channel_message['id'],
                                indexer_settings['tipFilterCallbackToken'], message['sequence'])
                    elif message_kind == AccountMessageKind.SPENT_OUTPUT_EVENT:
                        spent_output_message = cast(OutputSpend, message)
                        logger.debug("Queued incoming output spend message")
                        output_spend_result_queue.put_nowait([ spent_output_message ])
                    else:
                        logger.error("Unhandled binary server websocket message %r",
                            websocket_message)
                elif websocket_message.type in (aiohttp.WSMsgType.CLOSE,
                        aiohttp.WSMsgType.ERROR, aiohttp.WSMsgType.CLOSED,
                        aiohttp.WSMsgType.CLOSING):
                    logger.info("Server websocket closed")
                    break
                else:
                    logger.error("Unhandled server websocket message type %r",
                        websocket_message)


async def spawn_tasks(output_spend_result_queue: queue.Queue,
        tip_filter_matches_queue: queue.Queue, indexer_settings: IndexerServerSettings,
        api_key: str, registrations_complete_event: threading.Event):
    websocket_connected_event = asyncio.Event()
    task = create_task(listen_for_notifications_task_async(websocket_connected_event,
        output_spend_result_queue, tip_filter_matches_queue, indexer_settings, api_key))
    await websocket_connected_event.wait()
    await register_for_utxo_notifications(api_key)
    await register_for_pushdata_notifications(api_key)
    registrations_complete_event.set()
    await task

def listen_for_notifications_thread(loop: asyncio.AbstractEventLoop,
        output_spend_result_queue: queue.Queue,
        tip_filter_matches_queue: queue.Queue, indexer_settings: IndexerServerSettings,
        api_key: str, registrations_complete_event: threading.Event) -> None:
    """Launches the ESV-Reference-Server to run in the background but with a test database"""
    try:
        logging.basicConfig(level=logging.DEBUG)
        loop.run_until_complete(spawn_tasks(
            output_spend_result_queue, tip_filter_matches_queue, indexer_settings, api_key,
            registrations_complete_event))
        sys.exit(0)
    except KeyboardInterrupt:
        logger.debug("Notification listener stopped")
    except Exception:
        logger.exception("Unexpected exception in notification listener thread")


class TestAiohttpRESTAPI:
    logger = logging.getLogger("TestAiohttpRESTAPI")

    def setup_class(self) -> None:
        loop = asyncio.get_event_loop()
        indexer_settings: IndexerServerSettings
        api_key: str
        indexer_settings, api_key = loop.run_until_complete(setup_reference_server_tip_filtering())

        self.tip_filter_matches_queue = queue.Queue()
        self.output_spend_result_queue = queue.Queue()
        self.registrations_complete_event = threading.Event()

        thread = threading.Thread(
            target=listen_for_notifications_thread, args=[loop,
                self.output_spend_result_queue, self.tip_filter_matches_queue,
                indexer_settings, api_key, self.registrations_complete_event],
            daemon=True)
        thread.start()
        self.registrations_complete_event.wait()
        logger.debug(f"Registrations done!")

        blockchain_dir = MODULE_DIR.parent / "contrib" / "blockchains" / "blockchain_116_7c9cd2"
        import_blocks(str(blockchain_dir))
        time.sleep(15)

    def setup_method(self) -> None:
        pass

    def teardown_method(self) -> None:
        pass

    @classmethod
    def teardown_class(klass) -> None:
        pass

    def test_ping(klass):
        result = requests.get(PING_URL)
        assert result.json() is True

    def test_error(klass):
        result = requests.get(ERROR_URL)
        assert result.status_code == 400, result.reason
        assert result.reason is not None
        assert isinstance(result.reason, str)
    @pytest.mark.timeout(20)
    def test_utxo_notifications(self):
        expected_utxo_spends = set([tuple(utxo) for utxo in UTXO_REGISTRATIONS])
        len_expected_utxo_spends = len(expected_utxo_spends)
        for _ in range(len_expected_utxo_spends):
            message = self.output_spend_result_queue.get()
            logger.debug(f"Got output spend message from queue: {message}")
            output_spend_obj: OutputSpend = message[0]
            utxo = (hash_to_hex_str(output_spend_obj.out_tx_hash), output_spend_obj.out_index)
            assert utxo in expected_utxo_spends
            expected_utxo_spends.remove(utxo)
        assert len(expected_utxo_spends) == 0

    @pytest.mark.timeout(20)
    def test_pushdata_notifications(self):
        expected_count = 5
        count = 0
        while True:
            try:
                messages: list[GenericPeerChannelMessage] = self.tip_filter_matches_queue.get_nowait()
                message: GenericPeerChannelMessage
                for message in messages:
                    payload = base64.b64decode(message['payload'])
                    pushdata_notification = cast(TipFilterPushDataMatchesData, json.loads(payload))
                    logger.debug(f"Got pushdata_notification: {pushdata_notification}")
                    match: TipFilterNotificationMatch
                    for match in pushdata_notification['matches']:
                        count += 1
                        pushdata_hash_hex = match['pushDataHashHex']
                        output_tx_hash = match['transactionId']
                        output_idx = match['transactionIndex']
                        outpoint = PUSHDATA_TO_OUTPOINT_MAP[pushdata_hash_hex]
                        assert hash_to_hex_str(outpoint.tx_hash) == output_tx_hash
                        assert outpoint.out_idx == output_idx

                if count == expected_count:
                    break
            except queue.Empty:
                logger.debug(f"tip_filter_matches_queue queue empty, waiting for more")
                time.sleep(1)

    def test_get_transaction_json(self):
        headers = {'Accept': "application/json"}
        for txid, rawtx_hex in pre_reorg_test_data.TRANSACTIONS.items():
            result = requests.get(GET_TRANSACTION_URL.format(txid=txid), headers=headers)
            assert result.status_code == 200, f"reason={result.reason}, txid={txid}"
            assert result.json() == rawtx_hex

    def test_get_transaction_binary(self):
        headers = {'Accept': "application/octet-stream"}
        for txid, rawtx_hex in pre_reorg_test_data.TRANSACTIONS.items():
            result = requests.get(GET_TRANSACTION_URL.format(txid=txid), headers=headers)
            assert result.status_code == 200, result.reason
            assert result.content == bytes.fromhex(rawtx_hex)

    def test_get_tsc_merkle_proof_json(self):
        utils._get_tsc_merkle_proof_target_hash_json()
        utils._get_tsc_merkle_proof_target_merkleroot_json()
        utils._get_tsc_merkle_proof_target_header_json()
        utils._get_tsc_merkle_proof_include_rawtx_json()

    def test_pushdata_no_match_json(self):
        utils._pushdata_no_match_json()

    def test_mining_txs_json(self):
        utils._mining_txs_json_post_reorg()

    def test_p2pk_json(self):
        utils._p2pk_json(post_reorg=False)

    def test_p2pkh_json(self):
        utils._p2pkh_json(post_reorg=False)

    def test_p2sh_json(self):
        utils._p2sh_json(post_reorg=False)

    def test_p2ms_json(self):
        utils._p2ms_json(post_reorg=False)

    def test_p2ms2_json(self):
        utils._p2ms2_json(post_reorg=False)

    def test_submit_reorg_blocks(self):
        blockchain_dir = MODULE_DIR.parent / "contrib" / "blockchains" / "blockchain_118_0ebc17"
        import_blocks(str(blockchain_dir))
        time.sleep(10)
        assert True

    @pytest.mark.timeout(10)
    def test_utxo_notifications_post_reorg(self):
        # TODO test re-registration and initial status fetch
        expected_utxo_spends = set([tuple(utxo) for utxo in UTXO_REGISTRATIONS])
        len_expected_utxo_spends = len(expected_utxo_spends)
        for _ in range(len_expected_utxo_spends):
            message = self.output_spend_result_queue.get()
            logger.debug(f"Got output spend message from queue: {message}")
            output_spend_obj: OutputSpend = message[0]
            utxo = (hash_to_hex_str(output_spend_obj.out_tx_hash), output_spend_obj.out_index)
            assert utxo in expected_utxo_spends
            expected_utxo_spends.remove(utxo)
        assert len(expected_utxo_spends) == 0

    @pytest.mark.timeout(20)
    def test_pushdata_notifications_post_reorg(self):
        expected_count = 5
        count = 0
        while True:
            try:
                messages: list[GenericPeerChannelMessage] = self.tip_filter_matches_queue.get_nowait()
                message: GenericPeerChannelMessage
                for message in messages:
                    payload = base64.b64decode(message['payload'])
                    pushdata_notification = cast(TipFilterPushDataMatchesData, json.loads(payload))
                    logger.debug(f"Got pushdata_notification: {pushdata_notification}")
                    match: TipFilterNotificationMatch
                    for match in pushdata_notification['matches']:
                        count += 1
                        pushdata_hash_hex = match['pushDataHashHex']
                        output_tx_hash = match['transactionId']
                        output_idx = match['transactionIndex']
                        outpoint = PUSHDATA_TO_OUTPOINT_MAP[pushdata_hash_hex]
                        assert hash_to_hex_str(outpoint.tx_hash) == output_tx_hash
                        assert outpoint.out_idx == output_idx

                if count == expected_count:
                    break
            except queue.Empty:
                logger.debug(f"tip_filter_matches_queue queue empty, waiting for more")
                time.sleep(1)

    def test_get_tsc_merkle_proof_json_post_reorg(self):
        utils._get_tsc_merkle_proof_target_hash_json(post_reorg=True)
        utils._get_tsc_merkle_proof_target_merkleroot_json(post_reorg=True)
        utils._get_tsc_merkle_proof_target_header_json(post_reorg=True)
        utils._get_tsc_merkle_proof_include_rawtx_json(post_reorg=True)

    def test_pushdata_no_match_json_post_reorg(self):
        utils._pushdata_no_match_json()

    def test_mining_txs_json_post_reorg(self):
        utils._mining_txs_json_post_reorg()

    def test_p2pk_json_post_reorg(self):
        utils._p2pk_json(post_reorg=True)

    def test_p2pkh_json_post_reorg(self):
        utils._p2pkh_json(post_reorg=True)

    def test_p2sh_json_post_reorg(self):
        utils._p2sh_json(post_reorg=True)

    def test_p2ms_json_post_reorg(self):
        utils._p2ms_json(post_reorg=True)

    def test_p2ms2_json_post_reorg(self):
        utils._p2ms2_json(post_reorg=True)

