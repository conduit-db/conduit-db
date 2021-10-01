"""
This is a simple script - there is no utxo splitting algorithm. This is achieved by mining more
blocks. I suggest you mine 3000 blocks (in batches of 1000) then run this script for as long
as you feel is sufficient to load the mempool. This will result in around 3000 transactions
being send per iteration (every ~5 second or so).

The use case is to be able to preload the mempool to a large size and ensure that ConduitIndex
does not choke on the mempool invalidation logic. Later it will be used to ensure there are no
regressions when I migrate to the compact block protocol (namely performance regressions)

The node's rpcthreads need to be set higher than the number of threads used here.

Fixes as a result of this script's usage:
1) With > 20000 txs in mempool the gRPC StatusCode.RESOURCE_EXHAUSTED error occurs
- Lifted default gRPC message size from 4MB -> 50MB
2) Mempool tx deletion when mining a new block was painfully slow with RocksDB engine (3mins25sec
for 40,000 transactions)... With ENGINE=MEMORY and a HASH index it is now basically instantaneous.
"""
import concurrent
from concurrent.futures.thread import ThreadPoolExecutor
from decimal import Decimal
from os import urandom

import electrumsv_node
import bitcoinx
import struct

from bitcoinx import BitcoinRegtest, TxInput, Script, SEQUENCE_FINAL, TxOutput, Tx, SigHash, \
    hex_str_to_hash


SIGHASH_FORKID = 0x40
SIGHASH_ALL = 0x01


def get_random_output():
    # Random key so that it is not tracked by bitcoin daemon wallet
    new_priv_key = bitcoinx.PrivateKey(secret=urandom(32), coin=BitcoinRegtest)
    pubkey: bitcoinx.PublicKey = new_priv_key.public_key
    output = TxOutput(value=10000, script_pubkey=pubkey.P2PKH_script())
    return output


# NOTE: Keeping all utxos tracked by bitcoind as P2PK type to match default from mining.
def get_change_output(pubkey, change_amount):
    output = TxOutput(value=change_amount, script_pubkey=pubkey.P2PK_script())
    return output


def create_txs():
    result = electrumsv_node.call_any('listunspent', 0).json()['result']
    utxos = {}  # address: utxo
    for utxo in result:
        if utxo['spendable']:
            addr = utxo['address']
            if not utxos.get(addr):
                utxos[addr] = []
            utxos[addr].append(utxo)


    rawtxs = []
    for addr in utxos:
        priv_key = electrumsv_node.call_any('dumpprivkey', addr).json()['result']
        key = bitcoinx.PrivateKey.from_WIF(priv_key)
        pubkey: bitcoinx.PublicKey = key.public_key

        # Make 1 tx per utxo
        for utxo in utxos[addr]:
            value = int(Decimal(str(utxo['amount'])) * 100_000_000)
            input = TxInput(prev_hash=hex_str_to_hash(utxo['txid']), prev_idx=int(utxo['vout']),
                script_sig=Script(), sequence=SEQUENCE_FINAL)

            output = get_random_output()
            tx = Tx(inputs=[input], outputs=[output], locktime=0x00000000, version=1)
            size_tx = tx.size_io([input], [output] * 2)
            fee_for_size = size_tx * 1  # 1 sat/byte
            change_amount = value - (output.value + fee_for_size)
            if change_amount < 1000:
                continue  # discard small utxos
            change_output = get_change_output(pubkey, change_amount=change_amount)
            tx.outputs.append(change_output)
            for idx, input in enumerate(tx.inputs):
                sig_hash = tx.signature_hash(input_index=idx, value=value,
                    script_code=pubkey.P2PK_script(),
                    sighash=SigHash(SIGHASH_ALL | SIGHASH_FORKID))
                sig = key.sign(sig_hash, None)
                script_sig = Script().push_many([
                    sig + struct.pack('B', (SIGHASH_ALL | SIGHASH_FORKID)),
                ])
                tx.inputs[0].script_sig = Script(script_sig)
                rawtxs.append(tx.to_hex())
    return rawtxs

def main():
    while True:
        with ThreadPoolExecutor(max_workers=50) as tpe:
            futures = []
            rawtxs = create_txs()
            for rawtx in rawtxs:
                fut = tpe.submit(electrumsv_node.call_any, 'sendrawtransaction', rawtx)
                futures.append(fut)

            for fut in concurrent.futures.as_completed(futures):
                try:
                    fut.result()
                except Exception as e:
                    print(e)

        print(f"Sent {len(rawtxs)} transactions to the node")


if __name__ == '__main__':
    main()
