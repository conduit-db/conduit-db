# Conduit

(In early development - not fully implemented yet)

As the original and unadulterated bitcoin protocol (Bitcoin SV) scales there are new
challenges for keeping applications responsive under load. Scaling-testnet's > 2GB
blocks can take some time to index and leads to clients waiting
unreasonable times for mempool events and confirmations.

Conduit can parallelize the chain indexing process across multiple cores
(16, 32, 64 etc.) so wallets/applications can get timely notifications.

## Getting Started

## Windows (cmd.exe shell)

To run ConduitRaw

    git clone https://github.com/AustEcon/conduit.git
    cd conduit
    set PYTHONPATH=.
    py -m pip install -r .\conduit_raw\requirements.txt
    py .\conduit_raw\conduit_server.py
    
To run ConduitIndex (Depends on ConduitRaw)

    git clone https://github.com/AustEcon/conduit.git
    cd conduit
    set PYTHONPATH=.
    py -m pip install -r .\conduit_index\requirements.txt
    py .\conduit_index\conduit_server.py

## Linux

To run ConduitRaw

    git clone https://github.com/AustEcon/conduit.git
    cd conduit/conduit_raw
    python -m pip install -r requirements.txt
    python .\conduit_server.py
    
To run ConduitIndex (Depends on ConduitRaw)

    git clone https://github.com/AustEcon/conduit.git
    cd conduit/conduit_raw
    python -m pip install -r requirements.txt
    python .\conduit_server.py

## Testnet / Regtest / Scaling-Testnet / Mainnet

    python .\conduit_server.py --testnet

    python .\conduit_server.py --regtest

    python .\conduit_server.py --scaling-testnet
    
    python .\conduit_server.py                      # Mainnet is the default

currently the peers are hardcoded so the host / port cannot be
passed in via commandline (yet).

## Acknowedgments

- This project makes heavy use of the [bitcoinx](https://github.com/kyuupichan/bitcoinX) bitcoin
library created by [kyuupichan](https://github.com/kyuupichan) for tracking of headers and
chain forks in a memory mapped file
- The idea for indexing pushdata hashes came from discussions with
[Roger Taylor](https://github.com/rt121212121) the lead maintainer of [electrumsv](https://github.com/electrumsv/electrumsv)

