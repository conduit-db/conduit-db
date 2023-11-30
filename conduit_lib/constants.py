# Object types
from enum import IntEnum
import os

MAX_BLOCK_PER_BATCH_COUNT = 1000
MAX_BLOCKS_SYNCRONIZED_AHEAD = 250  # Max number of blocks ConduitRaw can get ahead of ConduitIndex

ERROR = 0
MSG_TX = 1
MSG_BLOCK = 2
MSG_FILTERED_BLOCK = 3
MSG_CMPCT_BLOCK = 4

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

LOGGING_FORMAT = "%(asctime)s %(levelname)s %(message)s"

HashXLength = 11
MAX_UINT32 = 2**32 - 1

HEADER_LENGTH = 24
EXTENDED_HEADER_LENGTH = 24 + 20
BLOCK_HEADER_LENGTH = 80
ZERO_HASH = b"00" * 32
GENESIS_BLOCK = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
NULL_HASH = "0000000000000000000000000000000000000000000000000000000000000000"

MAINNET = "mainnet"
TESTNET = "testnet"
SCALINGTESTNET = "scaling-testnet"
REGTEST = "regtest"
LOGGING_LEVEL_VARNAME = "logging_level"

# For small blocks with less than this number of txs, do not divide into partitions
# It's (probably) more efficient for a single worker process to consume it as a single batch
# The only exception would be if these 1000 txs have an extreme amount of inputs, outputs and
# pushdata matches and are very very large txs.
SMALL_BLOCK_SIZE = 10_000
BULK_LOADING_BATCH_SIZE_ROW_COUNT = 100000

if os.environ.get("NETWORK") == "regtest":
    CHIP_AWAY_BYTE_SIZE_LIMIT = (1024**2) * 100
    MAIN_BATCH_HEADERS_COUNT_LIMIT = 100  # Number of headers to request (long poll) from conduit raw
    TARGET_BYTES_BLOCK_BATCH_REQUEST_SIZE_CONDUIT_RAW = int((1024**2) * 256)
    TARGET_BYTES_BLOCK_BATCH_REQUEST_SIZE_CONDUIT_INDEX = int((1024**2) * 256)
else:
    CHIP_AWAY_BYTE_SIZE_LIMIT = (1024**3) * 4
    MAIN_BATCH_HEADERS_COUNT_LIMIT = 100  # Number of headers to request (long poll) from conduit raw
    TARGET_BYTES_BLOCK_BATCH_REQUEST_SIZE_CONDUIT_RAW = int((1024**3) * 2)
    TARGET_BYTES_BLOCK_BATCH_REQUEST_SIZE_CONDUIT_INDEX = int((1024**3) * 2)


class MsgType(IntEnum):
    ERROR = 0
    MSG_TX = 1
    MSG_BLOCK = 2
    MSG_FILTERED_BLOCK = 3
    MSG_CMPCT_BLOCK = 4


# Log MTreeLevel
PROFILING = 9

CONDUIT_RAW_SERVICE_NAME = "conduit_raw"
CONDUIT_INDEX_SERVICE_NAME = "conduit_index"

SIZE_UINT64_T = 8
