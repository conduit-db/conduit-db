import json
import logging

import bitcoinx
import requests

from conduit_lib.types import _get_pushdata_match_flag
from conduit_test._test_data import TRANSACTIONS

BASE_URL = f"http://127.0.0.1:34525"
PING_URL = BASE_URL + "/"
ERROR_URL = BASE_URL + "/error"
GET_TRANSACTION_URL = BASE_URL + "/api/v1/transaction/{txid}"
RESTORATION_URL = BASE_URL + "/api/v1/restoration/search"

REF_TYPE_OUTPUT = 0
REF_TYPE_INPUT = 1


class TestInternalAiohttpRESTAPI:
    logger = logging.getLogger("TestInternalAiohttpRESTAPI")

    @classmethod
    def setup_class(klass) -> None:
        pass

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
        assert result.status_code == 400
        assert result.reason is not None
        assert isinstance(result.reason, str)

    def test_get_transaction_json(klass):
        headers = {'Accept': "application/json"}
        for txid, rawtx_hex in TRANSACTIONS.items():
            result = requests.get(GET_TRANSACTION_URL.format(txid=txid), headers=headers)
            assert result.status_code == 200
            assert result.json() == rawtx_hex

    def test_get_transaction_binary(klass):
        headers = {'Accept': "application/octet-stream"}
        for txid, rawtx_hex in TRANSACTIONS.items():
            result = requests.get(GET_TRANSACTION_URL.format(txid=txid), headers=headers)
            assert result.status_code == 200
            assert result.content == bytes.fromhex(rawtx_hex)

    def test_pushdata_no_match_json(klass):
        """
        Find no match for something that is not there.
        """
        headers = {'Accept': "application/json"}
        body = {
            "filterKeys": [
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            ]
        }
        result = requests.get(RESTORATION_URL, json=body, headers=headers)
        assert result.status_code == 404
        assert result.reason is not None
        assert isinstance(result.reason, str)

    def test_mining_txs_json(self):
        """
        Check the coinbase transactions going to the mining wallet. This checks the unspent and spent UTXOs are correct.
        """
        actual_matches = []
        actual_unspent_coinbase_transaction_hashes = []
        actual_spend_transaction_hashes = []

        headers = {'Accept': "application/json"}
        body = {
            "filterKeys": [
                "86c73b803ee5229044621b2fb6fb61b7001a92cbfdab1c7314da27a2fee72948"
            ]
        }
        result = requests.get(RESTORATION_URL, json=body, headers=headers, stream=True)
        assert result.status_code == 200
        assert result.text is not None
        for line in result.iter_lines(delimiter=b"\n"):
            if line == b"\x00":
                break
            match = json.loads(line.decode('utf-8'))

            actual_matches.append(match)
            # collect coinbase transactions
            if match['SpendTransactionId'] == "0000000000000000000000000000000000000000000000000000000000000000":
                actual_unspent_coinbase_transaction_hashes.append(match['TransactionId'])
                assert match['SpendInputIndex'] == 0xFFFFFFFF
            else:
                actual_spend_transaction_hashes.append(match['TransactionId'])

        assert len(actual_matches) == 110
        assert len(actual_unspent_coinbase_transaction_hashes) == 100
        assert len(actual_spend_transaction_hashes) == 10
        actual_spend_transaction_hashes_set = set(actual_spend_transaction_hashes)
        assert len(actual_spend_transaction_hashes_set) == 10

        expected_spend_coinbase_transaction_hashes = [ txid.lower() for txid in [
                "FCD363867BAB384A2CCB4349AEF3EE173D965561CA574B89E9FDB76642DD4D2B",
                "59D06760245723B17BFFD9D587EC01ACDFCA1B7F1ACA9184112EB615D8D50A70",
                "32EFB2AFDC5993AA3D63DBE031196B2AC08BFF196B3FF259DD50F5FB7A4F2CE0",
                "F90E0A8B2667BFC9BB19D2EAC8CF48F78F00F8FA5CA168591E3C1B0346203004",
                "3D052E3F9DF5073A04298AD87B01E6DC186665E4E4D7F965E210723DEE56E2E0",
                "19F2B7FFA0D44E15E6568572590A2D4CBCC80B64859CFCBDCB1260DD5E4383F6",
                "59E863F3BB2F1EC7192529513B95B4A782C80CDC930B028A9D53343866BF5641",
                "1008FD90BB1055AF8AF5272BB60B0E11FE34B777983156714D7DFD2695393513",
                "EE70715C37F23D72803A904A142AE483CE1E776278304DB975846378FFE99437",
                "9E724D5DE860799E909C2B94858741033C2E4201037F860BB583136CBEAB97D8",
            ]
        ]
        assert set(actual_spend_transaction_hashes_set) == set(expected_spend_coinbase_transaction_hashes)

    def test_p2pk_json(self):
        """
        Find the funding of a P2PK UTXO, and the subsequent spend information.
        """
        actual_matches = []
        headers = {'Accept': "application/json"}
        body = {
            "filterKeys": [
                "04bca2ae277997940152716854a95347819c2e07d370d22c093b39708fb9d5eb"
            ]
        }
        result = requests.get(RESTORATION_URL, json=body, headers=headers, stream=True)
        assert result.status_code == 200
        assert result.text is not None
        for line in result.iter_lines(delimiter=b"\n"):
            if line == b"\x00":
                break
            match = json.loads(line.decode('utf-8'))
            actual_matches.append(match)

        assert len(actual_matches) == 1
        match = actual_matches[0]
        assert match["TransactionId"] == "88c92bb09626c7d505ed861ae8fa7e7aaab5b816fc517eac7a8a6c7f28b1b210"
        assert match["Index"] == 0
        assert match["Flags"] == _get_pushdata_match_flag(ref_type=REF_TYPE_OUTPUT)
        assert match["SpendTransactionId"] == "47f3f47a256d70950ff5690ea377c24464310489e3f54d01b817dd0088f0a095"
        assert match["SpendInputIndex"] == 0
        assert match["BlockHeight"] == 112

    def test_p2pkh_json(self):
        """
        Find the funding of a P2PKH UTXO, and the subsequent spend information.
        """
        actual_matches = []
        headers = {'Accept': "application/json"}
        body = {"filterKeys": ["e351e4d2499786e8a3ac5468cbf1444b3416b41e424524b50e2dafc8f6f454db"]}
        result = requests.get(RESTORATION_URL, json=body, headers=headers, stream=True)
        assert result.status_code == 200
        assert result.text is not None
        for line in result.iter_lines(delimiter=b"\n"):
            if line == b"\x00":
                break
            match = json.loads(line.decode('utf-8'))
            actual_matches.append(match)

        assert len(actual_matches) == 1
        match = actual_matches[0]
        assert match["TransactionId"] == "d53a9ebfac748561132e49254c42dbe518080c2a5956822d5d3914d47324e842"
        assert match["Index"] == 0
        assert match["Flags"] == _get_pushdata_match_flag(ref_type=REF_TYPE_OUTPUT)
        assert match["SpendTransactionId"] == "47f3f47a256d70950ff5690ea377c24464310489e3f54d01b817dd0088f0a095"
        assert match["SpendInputIndex"] == 1
        assert match["BlockHeight"] == 111

    def test_p2sh_json(self):
        """
        Find the funding of a P2SH UTXO, and the subsequent spend information.
        """
        actual_matches = []
        headers = {'Accept': "application/json"}
        body = {"filterKeys": ["5e7583878789b03276d2d60a1cf3772a999084e3b12d0d3c1a33a30bd15609db"]}
        result = requests.get(RESTORATION_URL, json=body, headers=headers, stream=True)
        assert result.status_code == 200
        assert result.text is not None
        for line in result.iter_lines(delimiter=b"\n"):
            if line == b"\x00":
                break
            match = json.loads(line.decode('utf-8'))
            actual_matches.append(match)

        assert len(actual_matches) == 1
        match = actual_matches[0]
        assert match["TransactionId"] == "49250a55f59e2bbf1b0615508c2d586c1336d7c0c6d493f02bc82349fabe6609"
        assert match["Index"] == 1
        assert match["Flags"] == _get_pushdata_match_flag(ref_type=REF_TYPE_OUTPUT)
        assert match["SpendTransactionId"] == "1afaa1c87ca193480c9aa176f08af78e457e8b8415c71697eded1297ed953db6"
        assert match["SpendInputIndex"] == 0
        assert match["BlockHeight"] == 115

    def test_p2ms_json(self):
        """
        Find the funding of a P2MS (bare multi-signature) UTXO, and the subsequent spend information (from cosigner 1 perspective).
        """
        actual_matches = []
        headers = {'Accept': "application/json"}
        body = {"filterKeys": ["9ed50dfe0d3a28950ee9a2ee41dce7193dd8666c4ff42c974de1bde60332a701"]}
        result = requests.get(RESTORATION_URL, json=body, headers=headers, stream=True)
        assert result.status_code == 200
        assert result.text is not None
        for line in result.iter_lines(delimiter=b"\n"):
            if line == b"\x00":
                break
            match = json.loads(line.decode('utf-8'))
            actual_matches.append(match)

        assert len(actual_matches) == 1
        match = actual_matches[0]
        assert match["TransactionId"] == "479833ff49d1000cd6f9d23a88924d22eaeae8b9d543e773d7420c2bbfd73fe2"
        assert match["Index"] == 2
        assert match["Flags"] == _get_pushdata_match_flag(ref_type=REF_TYPE_OUTPUT)
        assert match["SpendTransactionId"] == "0120eae6dc11459fe79fbad26f998f4f8c5b75fa6f0fff5b0beca4f35ea7d721"
        assert match["SpendInputIndex"] == 0
        assert match["BlockHeight"] == 113

    def test_p2ms2_json(self):
        """
        Find the funding of a P2MS (bare multi-signature) UTXO, and the subsequent
        spend information (from co-signer 2 perspective).
        """
        actual_matches = []
        headers = {'Accept': "application/json"}
        body = {"filterKeys": ["e6221c70e0f3c686255b548789c63d0e2c6aa795ad87324dfd71d0b53d90d59d"]}
        result = requests.get(RESTORATION_URL, json=body, headers=headers, stream=True)
        assert result.status_code == 200
        assert result.text is not None
        for line in result.iter_lines(delimiter=b"\n"):
            if line == b"\x00":
                break
            match = json.loads(line.decode('utf-8'))
            actual_matches.append(match)

        assert len(actual_matches) == 1
        match = actual_matches[0]
        assert match["TransactionId"] == "479833ff49d1000cd6f9d23a88924d22eaeae8b9d543e773d7420c2bbfd73fe2"
        assert match["Index"] == 2
        assert match["Flags"] == _get_pushdata_match_flag(ref_type=REF_TYPE_OUTPUT)
        assert match["SpendTransactionId"] == "0120eae6dc11459fe79fbad26f998f4f8c5b75fa6f0fff5b0beca4f35ea7d721"
        assert match["SpendInputIndex"] == 0
        assert match["BlockHeight"] == 113
