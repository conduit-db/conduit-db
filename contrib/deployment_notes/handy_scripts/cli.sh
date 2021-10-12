#!/usr/bin/env bash
./bitcoin-sv-1.0.8/bin/bitcoin-cli -conf=/mnt/data/bitcoin_data/bitcoin.conf -datadir=/mnt/data/bitcoin_data -rpcuser=rpcuser -rpcpassword=rpcpassword "$@"
