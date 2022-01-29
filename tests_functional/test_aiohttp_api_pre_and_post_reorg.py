import json
import logging
import os
import time
from pathlib import Path

import requests

from contrib.scripts.import_blocks import import_blocks
from tests_functional import utils
import tests_functional._pre_reorg_data as pre_reorg_test_data

BASE_URL = f"http://127.0.0.1:34525"
PING_URL = BASE_URL + "/"
ERROR_URL = BASE_URL + "/error"
GET_TRANSACTION_URL = BASE_URL + "/api/v1/transaction/{txid}"
GET_MERKLE_PROOF_URL = BASE_URL + "/api/v1/merkle-proof/{txid}"
RESTORATION_URL = BASE_URL + "/api/v1/restoration/search"

STREAM_TERMINATION_BYTE = b"\x00"

MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))

logger = logging.getLogger("test-internal-aiohttp-api")


class TestInternalAiohttpRESTAPI:
    logger = logging.getLogger("TestInternalAiohttpRESTAPI")

    @classmethod
    def setup_class(klass) -> None:
        blockchain_dir = MODULE_DIR.parent / "contrib" / "blockchains" / "blockchain_116_7c9cd2"
        import_blocks(str(blockchain_dir))
        time.sleep(10)

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

    def test_get_transaction_json(self):
        headers = {'Accept': "application/json"}
        for txid, rawtx_hex in pre_reorg_test_data.TRANSACTIONS.items():
            result = requests.get(GET_TRANSACTION_URL.format(txid=txid), headers=headers)
            assert result.status_code == 200, result.reason
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

