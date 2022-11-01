#!/bin/sh -e

/usr/local/lib/python3.10/site-packages/electrumsv_node/bin/bitcoin-cli -regtest -datadir=/root/.electrumsv-node -rpcuser=rpcuser -rpcpassword=rpcpassword "$@"
