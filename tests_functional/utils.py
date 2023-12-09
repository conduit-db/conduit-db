# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.


import json
import logging
import requests
import tests_functional._pre_reorg_data as pre_reorg_test_data
import tests_functional._post_reorg_data as post_reorg_test_data
from conduit_lib.types import PushdataMatchFlags

BASE_URL = f"http://127.0.0.1:34525"
GET_TRANSACTION_URL = BASE_URL + "/api/v1/transaction/{txid}"
GET_MERKLE_PROOF_URL = BASE_URL + "/api/v1/merkle-proof/{txid}"
GET_HEADERS_TIP_URL = BASE_URL + "/api/v1/chain/tips"
RESTORATION_URL = BASE_URL + "/api/v1/restoration/search"

STREAM_TERMINATION_SYMBOL = b"{}"

logger = logging.getLogger("test-internal-aiohttp-api")


def _get_tsc_merkle_proof_target_hash_json(post_reorg=False):
    if post_reorg:
        test_data = post_reorg_test_data.MERKLE_PROOFS_TARGET_AS_HASH
    else:
        test_data = pre_reorg_test_data.MERKLE_PROOFS_TARGET_AS_HASH
    headers = {"Accept": "application/json"}
    params = {"targetType": "hash", "includeFullTx": "0"}
    for txid, expected_tsc_proof in test_data.items():
        result = requests.get(
            GET_MERKLE_PROOF_URL.format(txid=txid),
            params=params,
            headers=headers,
        )
        assert result.status_code == 200, result.reason
        assert (
            result.json() == expected_tsc_proof
        ), f"result.json()={result.json()}; expected_tsc_proof={expected_tsc_proof}"


def _get_tsc_merkle_proof_target_header_json(post_reorg=False):
    if post_reorg:
        test_data = post_reorg_test_data.MERKLE_PROOFS_TARGET_AS_HEADER
    else:
        test_data = pre_reorg_test_data.MERKLE_PROOFS_TARGET_AS_HEADER
    headers = {"Accept": "application/json"}
    params = {"targetType": "header", "includeFullTx": "0"}
    for txid, expected_tsc_proof in test_data.items():
        result = requests.get(
            GET_MERKLE_PROOF_URL.format(txid=txid),
            params=params,
            headers=headers,
        )
        assert result.status_code == 200, result.reason
        assert (
            result.json() == expected_tsc_proof
        ), f"result.json()={result.json()}; expected_tsc_proof={expected_tsc_proof}"


def _get_tsc_merkle_proof_target_merkleroot_json(post_reorg=False):
    if post_reorg:
        test_data = post_reorg_test_data.MERKLE_PROOFS_TARGET_AS_MERKLE_ROOT
    else:
        test_data = pre_reorg_test_data.MERKLE_PROOFS_TARGET_AS_MERKLE_ROOT
    headers = {"Accept": "application/json"}
    params = {"targetType": "merkleroot", "includeFullTx": "0"}
    for txid, expected_tsc_proof in test_data.items():
        result = requests.get(
            GET_MERKLE_PROOF_URL.format(txid=txid),
            params=params,
            headers=headers,
        )
        assert result.status_code == 200, result.reason
        assert (
            result.json() == expected_tsc_proof
        ), f"result.json()={result.json()}; expected_tsc_proof={expected_tsc_proof}"


def _get_tsc_merkle_proof_include_rawtx_json(post_reorg=False):
    if post_reorg:
        test_data = post_reorg_test_data.MERKLE_PROOFS_INCLUDE_RAWTX
    else:
        test_data = pre_reorg_test_data.MERKLE_PROOFS_INCLUDE_RAWTX
    headers = {"Accept": "application/json"}
    params = {"targetType": "hash", "includeFullTx": "1"}
    for txid, expected_tsc_proof in test_data.items():
        result = requests.get(
            GET_MERKLE_PROOF_URL.format(txid=txid),
            params=params,
            headers=headers,
        )
        assert result.status_code == 200, result.reason
        json_body = result.json()
        for key, value in json_body.items():
            assert json_body[key] == expected_tsc_proof[key], f"{json_body[key]} != {expected_tsc_proof[key]}"


def _pushdata_no_match_json():
    """
    Find no match for something that is not there.
    """
    headers = {"Accept": "application/json"}
    body = {"filterKeys": ["aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"]}
    result = requests.post(RESTORATION_URL, json=body, headers=headers)
    assert result.status_code == 200, result.reason
    assert result.json() == {}, result.text


def _mining_txs_json_post_reorg():
    """
    Check the coinbase transactions going to the mining wallet. This checks the unspent and
    spent UTXOs are correct.
    """
    actual_matches = []
    actual_unspent_coinbase_transaction_hashes = []
    actual_spend_transaction_hashes = []

    headers = {"Accept": "application/json"}
    body = {"filterKeys": ["86c73b803ee5229044621b2fb6fb61b7001a92cbfdab1c7314da27a2fee72948"]}
    result = requests.post(RESTORATION_URL, json=body, headers=headers, stream=True)
    assert result.status_code == 200, result.reason
    assert result.text is not None
    for line in result.iter_lines(delimiter=b"\n"):
        if not line or line == STREAM_TERMINATION_SYMBOL:
            continue
        match = json.loads(line.decode("utf-8"))
        actual_matches.append(match)
        # collect coinbase transactions
        if match["unlockingTransactionId"] is None:
            actual_unspent_coinbase_transaction_hashes.append(match["lockingTransactionId"])
            assert match["unlockingInputIndex"] == 0xFFFFFFFF
        else:
            actual_spend_transaction_hashes.append(match["lockingTransactionId"])
    assert len(actual_matches) == 112, len(actual_matches)
    assert len(actual_unspent_coinbase_transaction_hashes) == 100, len(
        actual_unspent_coinbase_transaction_hashes
    )
    assert len(actual_spend_transaction_hashes) == 12, len(actual_spend_transaction_hashes)
    # No duplicates
    actual_spend_transaction_hashes_set = set(actual_spend_transaction_hashes)
    assert len(actual_spend_transaction_hashes_set) == 12, len(actual_spend_transaction_hashes_set)

    expected_spend_coinbase_transaction_hashes = [
        txid.lower()
        for txid in [
            "F90E0A8B2667BFC9BB19D2EAC8CF48F78F00F8FA5CA168591E3C1B0346203004",
            "4723501D7EC5488D32E19A59CBDB11EED7B9BB99B681303614A6E8B763BA1EA6",
            "F22CC55F333E4F3027CFCAEC488CB5472B503A1BE8B1049064AE6A4D00E26921",
            "3D052E3F9DF5073A04298AD87B01E6DC186665E4E4D7F965E210723DEE56E2E0",
            "FCD363867BAB384A2CCB4349AEF3EE173D965561CA574B89E9FDB76642DD4D2B",
            "59E863F3BB2F1EC7192529513B95B4A782C80CDC930B028A9D53343866BF5641",
            "9E724D5DE860799E909C2B94858741033C2E4201037F860BB583136CBEAB97D8",
            "1008FD90BB1055AF8AF5272BB60B0E11FE34B777983156714D7DFD2695393513",
            "19F2B7FFA0D44E15E6568572590A2D4CBCC80B64859CFCBDCB1260DD5E4383F6",
            "59D06760245723B17BFFD9D587EC01ACDFCA1B7F1ACA9184112EB615D8D50A70",
            "EE70715C37F23D72803A904A142AE483CE1E776278304DB975846378FFE99437",
            "32EFB2AFDC5993AA3D63DBE031196B2AC08BFF196B3FF259DD50F5FB7A4F2CE0",
        ]
    ]
    assert set(actual_spend_transaction_hashes_set) == set(
        expected_spend_coinbase_transaction_hashes
    ), actual_spend_transaction_hashes_set


def _p2pk_json(post_reorg=False):
    """
    Find the funding of a P2PK UTXO, and the subsequent spend information.
    """
    actual_matches = []
    headers = {"Accept": "application/json"}
    body = {"filterKeys": ["04bca2ae277997940152716854a95347819c2e07d370d22c093b39708fb9d5eb"]}
    result = requests.post(RESTORATION_URL, json=body, headers=headers, stream=True)
    assert result.status_code == 200, result.reason
    assert result.text is not None
    for line in result.iter_lines(delimiter=b"\n"):
        if not line or line == STREAM_TERMINATION_SYMBOL:
            continue
        match = json.loads(line.decode("utf-8"))
        actual_matches.append(match)

    assert len(actual_matches) == 1, len(actual_matches)
    match = actual_matches[0]
    assert match["lockingTransactionId"] == "88c92bb09626c7d505ed861ae8fa7e7aaab5b816fc517eac7a8a6c7f28b1b210"
    assert match["lockingTransactionIndex"] == 0
    assert match["flags"] == PushdataMatchFlags.OUTPUT
    assert (
        match["unlockingTransactionId"] == "47f3f47a256d70950ff5690ea377c24464310489e3f54d01b817dd0088f0a095"
    )
    assert match["unlockingInputIndex"] == 0
    # if post_reorg:
    #     assert match["BlockHeight"] == 116
    # else:
    #     assert match["BlockHeight"] == 112


def _p2pkh_json(post_reorg=False):
    """
    Find the funding of a P2PKH UTXO, and the subsequent spend information.
    """
    actual_matches = []
    headers = {"Accept": "application/json"}
    body = {"filterKeys": ["e351e4d2499786e8a3ac5468cbf1444b3416b41e424524b50e2dafc8f6f454db"]}
    result = requests.post(RESTORATION_URL, json=body, headers=headers, stream=True)
    assert result.status_code == 200, result.reason
    assert result.text is not None
    for line in result.iter_lines(delimiter=b"\n"):
        if not line or line == STREAM_TERMINATION_SYMBOL:
            continue
        match = json.loads(line.decode("utf-8"))
        actual_matches.append(match)

    assert len(actual_matches) == 1, len(actual_matches)
    match = actual_matches[0]
    assert match["lockingTransactionId"] == "d53a9ebfac748561132e49254c42dbe518080c2a5956822d5d3914d47324e842"
    assert match["lockingTransactionIndex"] == 0
    assert match["flags"] == PushdataMatchFlags.OUTPUT
    assert (
        match["unlockingTransactionId"] == "47f3f47a256d70950ff5690ea377c24464310489e3f54d01b817dd0088f0a095"
    )
    assert match["unlockingInputIndex"] == 1
    # if post_reorg:
    #     assert match["BlockHeight"] == 116
    # else:
    #     assert match["BlockHeight"] == 111


def _p2sh_json(post_reorg=False):
    """
    Find the funding of a P2SH UTXO, and the subsequent spend information.
    """
    actual_matches = []
    headers = {"Accept": "application/json"}
    body = {"filterKeys": ["5e7583878789b03276d2d60a1cf3772a999084e3b12d0d3c1a33a30bd15609db"]}
    result = requests.post(RESTORATION_URL, json=body, headers=headers, stream=True)
    assert result.status_code == 200, result.reason
    assert result.text is not None
    for line in result.iter_lines(delimiter=b"\n"):
        if not line or line == STREAM_TERMINATION_SYMBOL:
            continue
        match = json.loads(line.decode("utf-8"))
        actual_matches.append(match)

    assert len(actual_matches) == 1, len(actual_matches)
    match = actual_matches[0]
    assert match["lockingTransactionId"] == "49250a55f59e2bbf1b0615508c2d586c1336d7c0c6d493f02bc82349fabe6609"
    assert match["lockingTransactionIndex"] == 1
    assert match["flags"] == PushdataMatchFlags.OUTPUT
    assert (
        match["unlockingTransactionId"] == "1afaa1c87ca193480c9aa176f08af78e457e8b8415c71697eded1297ed953db6"
    )
    assert match["unlockingInputIndex"] == 0
    # if post_reorg:
    #     assert match["BlockHeight"] == 116
    # else:
    #     assert match["BlockHeight"] == 115


def _p2ms_json(post_reorg=False):
    """
    Find the funding of a P2MS (bare multi-signature) UTXO, and the subsequent spend information (from cosigner 1 perspective).
    """
    actual_matches = []
    headers = {"Accept": "application/json"}
    body = {"filterKeys": ["9ed50dfe0d3a28950ee9a2ee41dce7193dd8666c4ff42c974de1bde60332a701"]}
    result = requests.post(RESTORATION_URL, json=body, headers=headers, stream=True)
    assert result.status_code == 200, result.reason
    assert result.text is not None
    for line in result.iter_lines(delimiter=b"\n"):
        if not line or line == STREAM_TERMINATION_SYMBOL:
            continue
        match = json.loads(line.decode("utf-8"))
        actual_matches.append(match)

    assert len(actual_matches) == 1, len(actual_matches)
    match = actual_matches[0]
    assert match["lockingTransactionId"] == "479833ff49d1000cd6f9d23a88924d22eaeae8b9d543e773d7420c2bbfd73fe2"
    assert match["lockingTransactionIndex"] == 2
    assert match["flags"] == PushdataMatchFlags.OUTPUT
    assert (
        match["unlockingTransactionId"] == "0120eae6dc11459fe79fbad26f998f4f8c5b75fa6f0fff5b0beca4f35ea7d721"
    )
    assert match["unlockingInputIndex"] == 0
    # if post_reorg:
    #     assert match["BlockHeight"] == 116
    # else:
    #     assert match["BlockHeight"] == 113


def _p2ms2_json(post_reorg=False):
    """
    Find the funding of a P2MS (bare multi-signature) UTXO, and the subsequent
    spend information (from co-signer 2 perspective).
    """
    actual_matches = []
    headers = {"Accept": "application/json"}
    body = {"filterKeys": ["e6221c70e0f3c686255b548789c63d0e2c6aa795ad87324dfd71d0b53d90d59d"]}
    result = requests.post(RESTORATION_URL, json=body, headers=headers, stream=True)
    assert result.status_code == 200, result.reason
    assert result.text is not None
    for line in result.iter_lines(delimiter=b"\n"):
        if not line or line == STREAM_TERMINATION_SYMBOL:
            continue
        match = json.loads(line.decode("utf-8"))
        actual_matches.append(match)

    assert len(actual_matches) == 1, len(actual_matches)
    match = actual_matches[0]
    assert match["lockingTransactionId"] == "479833ff49d1000cd6f9d23a88924d22eaeae8b9d543e773d7420c2bbfd73fe2"
    assert match["lockingTransactionIndex"] == 2
    assert match["flags"] == PushdataMatchFlags.OUTPUT
    assert (
        match["unlockingTransactionId"] == "0120eae6dc11459fe79fbad26f998f4f8c5b75fa6f0fff5b0beca4f35ea7d721"
    )
    assert match["unlockingInputIndex"] == 0
    # if post_reorg:
    #     assert match["BlockHeight"] == 116
    # else:
    #     assert match["BlockHeight"] == 113
