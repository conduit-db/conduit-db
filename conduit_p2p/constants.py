# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.

# Object types
from enum import IntEnum

CCODES = {
    "0x01": "REJECT_MALFORMED",
    "0x10": "REJECT_INVALID",
    "0x11": "REJECT_OBSOLETE",
    "0x12": "REJECT_DUPLICATE",
    "0x40": "REJECT_NONSTANDARD",
    "0x41": "REJECT_DUST",
    "0x42": "REJECT_INSUFFICIENTFEE",
    "0x43": "REJECT_CHECKPOINT",
}

BLOCK_HEADER_LENGTH = 80
ZERO_HASH = b"00" * 32

MAINNET = "mainnet"
TESTNET = "testnet"
SCALINGTESTNET = "scaling-testnet"
REGTEST = "regtest"
LOGGING_LEVEL_VARNAME = "logging_level"


class MsgType(IntEnum):
    ERROR = 0
    MSG_TX = 1
    MSG_BLOCK = 2
    MSG_FILTERED_BLOCK = 3
    MSG_CMPCT_BLOCK = 4
