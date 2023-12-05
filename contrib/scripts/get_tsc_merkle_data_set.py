# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.

"""
Used for generating the:
 - conduit_test._test_data.MERKLE_PROOFS_TARGET_AS_HASH
 - conduit_test._test_data.MERKLE_PROOFS_TARGET_AS_MERKLE_ROOT
 - conduit_test._test_data.MERKLE_PROOFS_TARGET_AS_HEADERS
"""

from pprint import pprint

from electrumsv_node import electrumsv_node

from tests_functional._pre_reorg_data import TRANSACTIONS


def get_merkle():
    # Get block_hash
    include_full_tx = True
    target_type = "hash"

    merkle_proof_test_data = {}
    for txid in TRANSACTIONS:
        block_hash = electrumsv_node.call_any("getrawtransaction", txid, 1).json()["result"]["blockhash"]
        tsc_merkle_proof = electrumsv_node.call_any(
            "getmerkleproof2", block_hash, txid, include_full_tx, target_type
        ).json()["result"]

        merkle_proof_test_data[txid] = tsc_merkle_proof
    pprint(merkle_proof_test_data)


get_merkle()
