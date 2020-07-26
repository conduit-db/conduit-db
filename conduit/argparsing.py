import argparse

from .constants import SCALINGTESTNET, REGTEST, MAINNET, TESTNET


def get_parser():
    parser = argparse.ArgumentParser(description="run conduit chain-indexer")
    parser.add_argument(
        "--host",
        dest="host",
        nargs=argparse.OPTIONAL,
        help="specify a host bitcoin node to connect to (to override default)",
    )
    parser.add_argument(
        "--port",
        dest="port",
        action="store",
        type=int,
        help="port for remote daemon; defaults=[mainnet=8333, testnet=18333, "
        "scaling-testnet=9333, regtest=18444]",
    )

    parser.add_argument(
        "--mainnet", action="store_true", dest=MAINNET, help="use mainnet"
    )
    parser.add_argument(
        "--testnet", action="store_true", dest=TESTNET, help="use testnet (default)"
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
        "--file-logging", action="store_true", dest="filelogging", help="log to file"
    )
    parser.add_argument(
        "--client-mode",
        action="store_true",
        dest="client_mode",
        help="will not sync any blocks and does not require a "
        "database or redis - for use as a broadcasting "
        "service only (Not implemented)",
    )
    return parser
