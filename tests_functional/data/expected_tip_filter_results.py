# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.

from bitcoinx import hex_str_to_hash

from conduit_lib.types import OutpointType

P2PK = "04bca2ae277997940152716854a95347819c2e07d370d22c093b39708fb9d5eb"
P2PK_RECEIVE_OUTPOINT = OutpointType(
    hex_str_to_hash("88c92bb09626c7d505ed861ae8fa7e7aaab5b816fc517eac7a8a6c7f28b1b210"),
    0,
)
P2PKH = "e351e4d2499786e8a3ac5468cbf1444b3416b41e424524b50e2dafc8f6f454db"
P2PKH_RECEIVE_OUTPOINT = OutpointType(
    hex_str_to_hash("d53a9ebfac748561132e49254c42dbe518080c2a5956822d5d3914d47324e842"),
    0,
)
P2MS_1 = "9ed50dfe0d3a28950ee9a2ee41dce7193dd8666c4ff42c974de1bde60332a701"
P2MS_1_RECEIVE_OUTPOINT = OutpointType(
    hex_str_to_hash("479833ff49d1000cd6f9d23a88924d22eaeae8b9d543e773d7420c2bbfd73fe2"),
    2,
)
P2MS_2 = "e6221c70e0f3c686255b548789c63d0e2c6aa795ad87324dfd71d0b53d90d59d"
P2MS_2_RECEIVE_OUTPOINT = OutpointType(
    hex_str_to_hash("479833ff49d1000cd6f9d23a88924d22eaeae8b9d543e773d7420c2bbfd73fe2"),
    2,
)
P2SH = "5e7583878789b03276d2d60a1cf3772a999084e3b12d0d3c1a33a30bd15609db"
P2SH_RECEIVE_OUTPOINT = OutpointType(
    hex_str_to_hash("49250a55f59e2bbf1b0615508c2d586c1336d7c0c6d493f02bc82349fabe6609"),
    1,
)

PUSHDATA_TO_OUTPOINT_MAP: dict[str, OutpointType] = {
    P2PK: P2PK_RECEIVE_OUTPOINT,
    P2PKH: P2PKH_RECEIVE_OUTPOINT,
    P2MS_1: P2MS_1_RECEIVE_OUTPOINT,
    P2MS_2: P2MS_2_RECEIVE_OUTPOINT,
    P2SH: P2SH_RECEIVE_OUTPOINT,
}
