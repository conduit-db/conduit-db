from conduit_lib.utils import is_docker

if is_docker():
    SERVER_HOST = "0.0.0.0"
else:
    SERVER_HOST = "127.0.0.1"
SERVER_PORT = 34525

MAX_UINT32 = 2**32 - 1

# Converted to a 1 byte char flag for pushdata matches
OUTPUT_MATCH = 1 << 0
INPUT_MATCH = 1 << 1

REFERENCE_SERVER_SCHEME = "http"
REFERENCE_SERVER_HOST = "127.0.0.1"
REFERENCE_SERVER_PORT = 47126

UTXO_REGISTRATION_ACK_TIMEOUT = 5.0  # seconds

UTXO_REGISTRATION_TOPIC = b"utxo_registration"
PUSHDATA_REGISTRATION_TOPIC = b"pushdata_registration"
