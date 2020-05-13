# Object types
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

BITCOIN_NETWORK_VARNAME = 'network'
DATABASE_NAME_VARNAME = 'database_name'
DATABASE_USER_VARNAME = 'database_username'
DATABASE_HOST_VARNAME = 'database_host'
DATABASE_PORT_VARNAME = 'database_port'
DATABASE_PASSWORD_VARNAME = 'database_password'
LOGGING_LEVEL_VARNAME = 'logging_level'
