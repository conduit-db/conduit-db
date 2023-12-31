# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.
"""
This script is for pre-loading ConduitRaw with an intentionally corrupted database
before running the functional tests. It should recover from this seamlessly and
continue on without any adverse effects. For now, the corruption will include
a .zst raw block archive file that has one good zstd frame flushed for block 1
and then a partially written block 2 and without flushing the end of the zstd frame.

Under normal circumstances, a .zst archive in this state would give an error and be
unable to be opened. But it is possible to decompress all of the zstd frames prior
to the corrupted one to do a recovery.

To create the corrupted database I first:
- Loaded blocks 0 & 1 into the node
- Sync ConduitRaw against it to flush the zstd frames
- Load block 2 into the node with ConduitRaw still running
- This will flush a second zstd frame
- Shut down all docker containers.
- Intentionally corrupt the last zstd frame in the raw_blocks datadir
- Now before starting all functional tests, it will startup with a corrupted raw_blocks datadir
that needs to be repaired and will require re-indexing block 2.
All tests should continue working the same as they always have.
"""
import os
import shutil
from pathlib import Path
import json
import logging
import requests

from conduit_lib import wait_for_node
from conduit_lib.constants import REGTEST
from conduit_p2p import NetworkConfig, Serializer, Deserializer
from import_blocks import validate_inputs, submit_blocks
from tests_functional.test_01_aiohttp_api_pre_and_post_reorg import REGTEST_TEST_BLOCKCHAIN
from tests_functional.test_02_bitcoin_p2p_client import REGTEST_NODE_HOST, REGTEST_NODE_PORT

MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
CORRUPTED_DATADIRS = MODULE_DIR.parent / "corrupted_datadirs"
FUNCTIONAL_TEST_DATADIRS = MODULE_DIR.parent / "functional_test_datadirs"


logger = logging.getLogger("conduit.p2p.conftest")


def call_any(method_name: str, *args, rpcport: int=18332, rpchost: str="127.0.0.1", rpcuser:
        str="rpcuser", rpcpassword: str="rpcpassword"):
    """Send an RPC request to the specified bitcoin node"""
    result = None
    try:
        if not args:
            params = []
        else:
            params = [*args]
        payload = json.dumps(
            {"jsonrpc": "2.0", "method": f"{method_name}", "params": params, "id": 0})
        result = requests.post(f"http://{rpcuser}:{rpcpassword}@{rpchost}:{rpcport}", data=payload,
                               timeout=10.0)
        result.raise_for_status()
        return result
    except requests.exceptions.HTTPError as e:
        if result is not None:
            logger.error(result.json()['error']['message'])
        raise e


def import_first_3_regtest_blocks(blockchain_dir: str):
    output_dir_path, headers_file_path = validate_inputs(blockchain_dir)
    header_hash_hexs = [
        '0f9188f13cb7b2c71f2a335e3a4fc328bf5beb436012afca590b1a11466e2206',
        '3651292faaaa19ead28299e743aaf4e45b474fbbfa7cc7d3d71564c879a3fd5c',
        '52dee9409f757469fb1e465e24f881a589349b23fa8ffd6816e2048acbdd65a0'
    ]
    submit_blocks(header_hash_hexs, output_dir_path)
    # Take the node out of IBD mode, otherwise it won't respond with headers or block invs
    call_any('generate', 1)


def bind_mount_corrupted_conduit_raw_database():
    os.makedirs(FUNCTIONAL_TEST_DATADIRS, exist_ok=True)
    shutil.rmtree(FUNCTIONAL_TEST_DATADIRS)
    assert CORRUPTED_DATADIRS.exists()
    print(f"Copying corrupted database: {CORRUPTED_DATADIRS} into functional test dir: {FUNCTIONAL_TEST_DATADIRS}")
    shutil.copytree(src=CORRUPTED_DATADIRS, dst=FUNCTIONAL_TEST_DATADIRS)


if __name__ == '__main__':
    net_config = NetworkConfig(REGTEST)
    serializer = Serializer(net_config)
    deserializer = Deserializer(net_config)
    wait_for_node(node_host=REGTEST_NODE_HOST, node_port=REGTEST_NODE_PORT,
        deserializer=deserializer, serializer=serializer)
    import_first_3_regtest_blocks(REGTEST_TEST_BLOCKCHAIN)
    bind_mount_corrupted_conduit_raw_database()
