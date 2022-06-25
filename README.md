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

To run the node and mysql:

    docker-compose -f docker-compose.yml up node mysql

To run all services:

    docker-compose -f docker-compose.yml up node mysql conduit-raw conduit-index

Most volumes are named, with the current exception of the Bitcoin SV node. The expectation is that
doing a docker compose stop or down will reuse existing state. If a user wants to reset everything
they should take everything down and then prune (Docker speak for delete) all volumes.

    docker volume prune

See `contrib/rebuild-and-run.bat` or `contrib/rebuild-and-run.sh`
for scripts to do a fresh rebuild of all components. This should be
run before running the functional tests

## Python Version
Currently `python3.10` is required

## Windows - Running ConduitRaw & ConduitIndex

Windows cmd.exe:

    git clone https://github.com/conduit-db/conduit.git
    cd conduit-db
    set PYTHONPATH=.

This is optional if .env using H: drive letter for the data storage paths, this only needs to be
done once to setup.

    mkdir localdata
    subst H: .\localdata

Now install packages and run ConduitRaw (in one terminal)

    py -m pip install -r .\conduit_raw\requirements.txt
    py .\conduit_raw\run_conduit_raw.py

And ConduitIndex (in another terminal)

    py -m pip install -r .\conduit_index\requirements.txt
    py .\conduit_index\run_conduit_index.py

## Unix - Running ConduitRaw & ConduitIndex

Bash terminal:

    git clone https://github.com/conduit-db/conduit.git
    cd conduit-db
    export PYTHONPATH=.

Now install packages and run ConduitRaw (in one terminal)

    python3 -m pip install -r ./conduit_raw/requirements.txt
    python3 ./conduit_raw/conduit_server.py

And ConduitIndex (in another terminal)

    python3 -m pip install -r ./conduit_index/requirements.txt
    python3 ./conduit_index/conduit_server.py

## Configuration
All configuration is done via the `.env` file in the top level directory.
The settings should all be fairly self-explanatory.

## Acknowedgments

- This project makes heavy use of the [bitcoinx](https://github.com/kyuupichan/bitcoinX) bitcoin
library created by [kyuupichan](https://github.com/kyuupichan) for tracking of headers and
chain forks in a memory mapped file
- The idea for indexing pushdata hashes came from discussions with
[Roger Taylor](https://github.com/rt121212121) the lead maintainer of [electrumsv](https://github.com/electrumsv/electrumsv)

