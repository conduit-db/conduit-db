# Object types
import enum

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

HEADER_LENGTH = 24
BLOCK_HEADER_LENGTH = 80
ZERO_HASH = b"00" * 32
GENESIS_BLOCK = "000000000019d6689c085ae165831e934ff763ae46a2a6c172b3f1b60a8ce26f"
NULL_HASH = '0000000000000000000000000000000000000000000000000000000000000000'

BITCOIN_NETWORK_VARNAME = "network"
DATABASE_NAME_VARNAME = "database_name"
DATABASE_USER_VARNAME = "database_username"
DATABASE_HOST_VARNAME = "database_host"
DATABASE_PORT_VARNAME = "database_port"
DATABASE_PASSWORD_VARNAME = "database_password"
LOGGING_LEVEL_VARNAME = "logging_level"
RESET_VARNAME = "reset"

MAINNET = "mainnet"
TESTNET = "testnet"
SCALINGTESTNET = "scaling-testnet"
REGTEST = "regtest"

# Todo - add these as environment variables
WORKER_COUNT_PREPROCESSORS = 1
# WORKER_COUNT_TX_PARSERS = 8  # This is set for an AMD 5850X with 16 cores / 32 threads
WORKER_COUNT_TX_PARSERS = 2  # This is better for testing/developmnent
WORKER_COUNT_MTREE_CALCULATORS = 4
WORKER_COUNT_BLK_WRITER = 1
WORKER_COUNT_LOGGING_SERVERS = 1

# For small blocks with less than this number of txs, do not divide into partitions
# It's (probably) more efficient for a single worker process to consume it as a single batch
# The only exception would be if these 1000 txs have an extreme amount of inputs, outputs and
# pushdata matches and are very very large txs.
SMALL_BLOCK_SIZE = 10_000


# CHIP_AWAY_BYTE_SIZE_LIMIT must be larger than the largest transaction we expect otherwise
# the server would likely crash. It is set artificially low for now to prove that a large block
# can be fully processed without allocating it all into memory at once
CHIP_AWAY_BYTE_SIZE_LIMIT = 1024 ** 2 * 512
MAIN_BATCH_HEADERS_COUNT_LIMIT = 4000  # Number of headers to request (long poll) from conduit raw


class MsgType(enum.IntEnum):
    ERROR = 0
    MSG_TX = 1
    MSG_BLOCK = 2
    MSG_FILTERED_BLOCK = 3
    MSG_CMPCT_BLOCK = 4


# Log Level
PROFILING = 9

CONDUIT_RAW_SERVICE_NAME = 'conduit_raw'
CONDUIT_INDEX_SERVICE_NAME = 'conduit_index'
