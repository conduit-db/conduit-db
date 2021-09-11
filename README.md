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

## Running MariaDB / Bitcoin Node / Kafka
These services are required for ConduitRaw and ConduitIndex to begin the chain indexing
process.

From the top-level directory please run:

    docker-compose -f docker-compose.dev.yml down
    docker-compose -f docker-compose.dev.yml up
    
This will start these services ready to develop against.

To run all services (including ConduitRaw and ConduitIndex) there is a 
docker-compose.yml.

To persist data between runs - mount a volume to the host file system. For example
the bitcoin node data directory can be mounted to a windows directory to persist chain
state between runs.

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

