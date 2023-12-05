# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.

from typing import TypedDict

from bitcoinx import PrivateKey
from datetime import datetime
import json


class VerifiableKeyDataDict(TypedDict):
    public_key_hex: str
    signature_hex: str
    message_hex: str


CLIENT_IDENTITY_PRIVATE_KEY_HEX = "d468816bc0f78465d4833426c280166c3810ecc9c0350c5232b0c417687fbde6"
CLIENT_IDENTITY_PRIVATE_KEY = PrivateKey.from_hex(CLIENT_IDENTITY_PRIVATE_KEY_HEX)


def _generate_client_key_data() -> VerifiableKeyDataDict:
    iso_date_text = datetime.utcnow().isoformat()
    message_bytes = b"http://server/api/account/metadata" + iso_date_text.encode()
    signature_bytes = CLIENT_IDENTITY_PRIVATE_KEY.sign_message(message_bytes)
    return {
        "public_key_hex": CLIENT_IDENTITY_PRIVATE_KEY.public_key.to_hex(),
        "message_hex": message_bytes.hex(),
        "signature_hex": signature_bytes.hex(),
    }


if __name__ == "__main__":
    print(json.dumps(_generate_client_key_data()))
