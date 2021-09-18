# ConduitDB
As the original and unadulterated bitcoin protocol (Bitcoin SV) scales there are new
challenges for keeping applications responsive under load. Scaling-testnet's > 2GB
blocks can take some time to index and leads to clients waiting
unreasonable times for mempool events and confirmations.

ConduitDB can parallelize the chain indexing process so wallets/applications can
continue to get timely notifications despite exponentially increasing blockchain
transaction throughput.

## Getting Started

## MariaDB (Pre-requisite configuration step)

Please make sure that the file `contrib/mariadb/config/my.cnf` has its file permissions set to
"Read-only" on windows otherwise the Docker image will reject the RocksDB engine configuration.

## Running different combinations of services in Docker

The core services of mysql, kafka, zookeeper and the  Bitcoin SV node can be run using:

    docker-compose -f docker-compose.yml up

This is useful to develop the indexer and raw services on bare metal. However, additional Docker
compose files can be layered to get some or all of the other services running.

To run all services:

    docker-compose -f docker-compose.yml -f docker-compose.indexing.yml -f docker-compose.spvchannels.yml -f docker-compose.api.yml up

To run enough services to debug the REST API service on bare metal:

    docker-compose -f docker-compose.yml -f docker-compose.indexing.yml up

Most volumes are named, with the current exception of the Bitcoin SV node. The expectation is that
doing a docker compose stop or down will reuse existing state. If a user wants to reset everything
they should take everything down and then prune (Docker speak for delete) all volumes.

    docker volume prune

## Running ConduitRaw

Windows cmd.exe:

    git clone https://github.com/AustEcon/conduit.git
    cd conduit
    set PYTHONPATH=.
    py -m pip install -r .\conduit_raw\requirements.txt
    py .\conduit_raw\conduit_server.py          (optional flag: --reset)

Unix:

    git clone https://github.com/AustEcon/conduit.git
    cd conduit
    export PYTHONPATH=.
    python -m pip install -r ./conduit_raw/requirements.txt
    python ./conduit_raw/conduit_server.py      (optional flag: --reset)

## Running ConduitIndex - (depends on ConduitRaw)

Windows cmd.exe:

    git clone https://github.com/AustEcon/conduit.git
    cd conduit
    set PYTHONPATH=.
    py -m pip install -r .\conduit_index\requirements.txt
    py .\conduit_index\conduit_server.py        (optional flag: --reset)

Unix:

    git clone https://github.com/AustEcon/conduit.git
    cd conduit
    set PYTHONPATH=.
    python3 -m pip install -r ./conduit_index/requirements.txt
    python3 ./conduit_index/conduit_server.py   (optional flag: --reset)


## Testnet / Regtest / Scaling-Testnet / Mainnet

    python .\conduit_server.py --regtest            # regtest is the default
    python .\conduit_server.py --testnet
    python .\conduit_server.py --scaling-testnet
    python .\conduit_server.py --mainnet

Currently the peers are hardcoded.

## Warning about new changes

All configuration will soon be moved to exclusively be via environment variables in
`.env` and `.env.bare.metal` for docker vs bare-metal services respectively. This
should apply to ALL services for both python and C# to minimize cognitive strain
during deployments and end-to-end testing / CI/CD!

## Acknowedgments

- This project makes heavy use of the [bitcoinx](https://github.com/kyuupichan/bitcoinX) bitcoin
library created by [kyuupichan](https://github.com/kyuupichan) for tracking of headers and
chain forks in a memory mapped file
- The idea for indexing pushdata hashes came from discussions with
[Roger Taylor](https://github.com/rt121212121) the lead maintainer of [electrumsv](https://github.com/electrumsv/electrumsv)

