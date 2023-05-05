import os
from pathlib import Path

import bitsv
from bitsv import FullNode

BITCOIN_DIR = Path(os.getenv("LOCALAPPDATA")) / "ElectrumSV-Node"

LESS_THAN_20_BYTES = bytes.fromhex("aa")
MORE_THAN_20_BYTES = bytes.fromhex("bb" * 21)
JUST_SHY_OF_PUSHDATA1 = bytes.fromhex("cc" * 76)
PUSHDATA1_BYTES = bytes.fromhex("dd" * 0xFF)
PUSHDATA2_BYTES = bytes.fromhex("ee" * 0xFFFF)
PUSHDATA4_BYTES = bytes.fromhex("ff" * 0x010000)

list_of_pushdatas = [
    LESS_THAN_20_BYTES,
    MORE_THAN_20_BYTES,
    JUST_SHY_OF_PUSHDATA1,
    PUSHDATA1_BYTES,
    PUSHDATA2_BYTES,
]
key = bitsv.PrivateKey("cMvUEMJv86oxfgicMHJq518WuSQERRpWHHyYaxc3GJmrwEWhzNtm", network="test")
fullnode = FullNode(
    conf_dir=BITCOIN_DIR,
    rpcuser="rpcuser",
    rpcpassword="rpcpassword",
    network="regtest",
)
fullnode.importprivkey(key.to_wif())
utxos = fullnode.get_unspents(key.address)
rawtx1 = key.create_op_return_tx(list_of_pushdatas, unspents=utxos)
fullnode.sendrawtransaction(rawtx1)

list_of_pushdatas = [PUSHDATA4_BYTES]
utxos = fullnode.get_unspents(key.address)
rawtx2 = key.create_op_return_tx(list_of_pushdatas, unspents=utxos)
fullnode.sendrawtransaction(rawtx2)
