# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.

import argparse

from conduit_lib.constants import SCALINGTESTNET, REGTEST, MAINNET, TESTNET


def get_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="run conduit_index chain-indexer")
    parser.add_argument(
        "--host",
        dest="remote_host",
        nargs=argparse.OPTIONAL,
        help="specify a remote_host bitcoin node to connect to (to override default)",
    )
    parser.add_argument(
        "--port",
        dest="remote_port",
        action="store",
        type=int,
        help="remote_port for remote daemon; defaults=[mainnet=8333, testnet=18333, "
        "scaling-testnet=9333, regtest=18444]",
    )
    parser.add_argument("--mainnet", action="store_true", dest=MAINNET, help="use mainnet")
    parser.add_argument(
        "--testnet",
        action="store_true",
        dest=TESTNET,
        help="use testnet (default)",
    )
    parser.add_argument(
        "--scaling-testnet",
        action="store_true",
        dest=SCALINGTESTNET,
        help="Use scaling-testnet",
    )
    parser.add_argument(
        "--regtest",
        action="store_true",
        dest=REGTEST,
        help="Use regression testnet",
    )
    parser.add_argument(
        "-v",
        "--verbosity",
        action="store",
        dest="verbosity",
        const="info",
        nargs=argparse.OPTIONAL,
        choices=("debug", "info", "warning", "error", "critical"),
        help="Set logging verbosity",
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        dest="reset",
        help="will wipe block_headers.mmap, headers.mmap and drop the LMDB and postgres database "
        "(for convenience when testing on RegTest)",
    )
    parser.add_argument(
        "--db-host",
        action="store",
        dest="db_host",
        default="127.0.0.1:52525",
        type=str,
        help="e.g. localhost:52525 for use outside of docker or " "mysql:3306 for use within docker",
    )
    parser.add_argument(
        "--node-host",
        action="store",
        dest="node_host",
        default="127.0.0.1:18444",
        type=str,
        help="e.g. localhost:18444 for use outside of docker or "
        "node:18444 for use within docker (this refers to the NODE_P2P_PORT not the RPC remote_port)",
    )
    parser.add_argument(
        "--lmdb-path",
        action="store",
        dest="lmdb_path",
        default=None,
        type=str,
    )
    return parser
