import bitcoinx
from bitcoinx import sha256

# P2SH example: 6a26d2ecb67f27d1fa5524763b49029d7106e91e3cc05743073461a719776192
# P2PK example: dbaf14e1c476e76ea05a8b71921a46d6b06f0a950f17c5f9f1a03b8fae467f10
# P2PKH example: e03a9a4b5c557f6ee3400a29ff1475d1df73e9cddb48c2391abdc391d8c1504a

# There were a stream of back-to-back "Big blocks" >250MB from around height 699024
# Would be good to check the txs towards the end of the block to make sure the rawtxs are
# stored correctly given the latest feature upgrade to processing blocks in smaller chunks.

rawtx = "0100000001f6ea284ec7521f8a7d094a6cf4e6873098b90f90725ffd372b343189d7a4089c0100000026255121029b6d2c97b8b7c718c325d7be3ac30f7c9d67651bce0c929f55ee77ce58efcf8451aeffffffff0130570500000000001976a9145a3acbc7bbcc97c5ff16f5909c9d7d3fadb293a888ac00000000"

tx = bitcoinx.Tx.from_hex(rawtx)
print(f"TxHash: {tx.hex_hash()}")
for idx, out in enumerate(tx.outputs):
    script = bitcoinx.Script(out.script_pubkey)
    pkh = None
    pk = None
    print(f"Output ASM: {script.to_asm(False)}")
    for op in script.ops():
        if isinstance(op, bytes):
            if len(op) == 20:
                pkh = op
            if len(op) in {33, 65}:
                pk = op
    if pkh:
        addr = bitcoinx.P2PKH_Address(pkh, bitcoinx.Bitcoin)
        pushdata_hash = sha256(pkh)
        print(f"Output idx: {idx}")
        print(f"Address: {addr}")
        print(f"Pushdata hash: {pushdata_hash.hex()}")

    if pk:
        addr = bitcoinx.P2PK_Output(pk, bitcoinx.Bitcoin)
        pushdata_hash = sha256(pk)
        print(f"Output idx: {idx}")
        print(f"Pubkey: {pk.hex()}")
        print(f"Pushdata hash: {pushdata_hash.hex()}")
