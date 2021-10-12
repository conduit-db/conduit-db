# ConduitDB
As the original and unadulterated bitcoin protocol (Bitcoin SV) scales there are new
challenges for keeping applications responsive under load. Scaling-testnet's > 2GB
blocks can take some time to index and leads to clients waiting
unreasonable times for mempool events and confirmations.

ConduitDB can parallelize the chain indexing process so wallets/applications can
continue to get timely notifications despite exponentially increasing blockchain
transaction throughput.

## Getting Started

## Running different combinations of services in Docker

The core services of mysql and the  Bitcoin SV node can be run using:

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

## Production recommendations
It is recommended to run all services including MySQL and the Node bare metal 
via systemd for the best performance on a linux server with ideally 8-16 CPU 
cores and >=32GB RAM.  

NVMe for the MySQL database is advised. The node and LMDB database can be split
across SATA SSD for their relatively small indexing footprint and HDD for 
raw block storage given the predominantly sequential access patterns for this 
data. 

## Running ConduitRaw

Windows cmd.exe:

    git clone https://github.com/AustEcon/conduit.git
    cd conduit-db
    set PYTHONPATH=.
    py -m pip install -r .\conduit_raw\requirements.txt
    py .\conduit_raw\conduit_server.py          (optional flag: --reset)

Unix:

    git clone https://github.com/AustEcon/conduit.git
    cd conduit-db
    export PYTHONPATH=.
    python3 -m pip install -r ./conduit_raw/requirements.txt
    python3 -m pip install -r ./conduit_index/requirements-linux-extras.txt
    python3 ./conduit_raw/conduit_server.py      (optional flag: --reset)

## Running ConduitIndex - (depends on ConduitRaw)

Windows cmd.exe:

    git clone https://github.com/AustEcon/conduit.git
    cd conduit-db
    set PYTHONPATH=.
    py -m pip install -r .\conduit_index\requirements.txt
    py .\conduit_index\conduit_server.py        (optional flag: --reset)

Unix:
    
    sudo apt-get install python3-dev default-libmysqlclient-dev build-essential
    
    git clone https://github.com/AustEcon/conduit.git
    cd conduit-db
    export PYTHONPATH=.
    python3 -m pip install -r ./conduit_index/requirements.txt
    python3 -m pip install -r ./conduit_index/requirements-linux-extras.txt
    python3 ./conduit_index/conduit_server.py   (optional flag: --reset)


## Testnet / Regtest / Scaling-Testnet / Mainnet

    python .\conduit_server.py --regtest            # regtest is the default
    python .\conduit_server.py --testnet
    python .\conduit_server.py --scaling-testnet
    python .\conduit_server.py --mainnet

Currently the peers are hardcoded.


## Cython extensions
To compile cython extension modules:

    py -3.9-64 setup.py build_ext --inplace

As of 11/10/2021 (on i5-8600k CPU):

    preprocessor (see bench/cy_preprocessor)
    ----------------------------------------
    pure python:        362 MB/sec
    cythonized:         6000 MB/sec

    tx parser (see bench/cy_txparser)
    ---------------------------------
    pure python:        24 MB/sec 
    cythonized:         39 MB/sec


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

