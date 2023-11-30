# ConduitDB
ConduitDB is a horizontally scalable blockchain parser and
indexer. It uses ScyllaDB with a tight database schema optimized
for write throughput and makes limited use of Redis for managing 
some of the state between worker processes.

ConduitDB is designed to be a flexible, value-added service that is capable of doing all of the 
indexing and data storage work entirely on its own (without running a node at all). 
This can include storing and indexing the full 10TB+ blockchain with:

- Raw transaction lookups
- Merkle proof lookups
- UTXO lookups
- Historic address-based / or pushdata-based lookups from genesis to chain tip. This is mainly to support seed-based restoration of wallets but could also be used for _unwriter protocol namespace lookups and querying
- Tip filter notification API gives notifications when pushdatas are seen of 20, 33 or 65 bytes in length i.e. pubkey hash (20 bytes) addresses for P2PKH, public keys for P2PK (33 or 65 for compressed vs uncompressed). Other use cases in the token and diverse namespace protocols are also likely.
- Output spend notifications API gives notifications when a particular utxo is spent. This is useful to know that a broadcast transaction has indeed propagated to the mempool and subsequently been included in a block OR if it has been affected by a subsequent reorg. The SPV wallet can react accordingly for each of these events and fetch the new merkle proof - updating their local wallet database.

It is the opinion of the author that these APIs cover all of the basic requirements for an SPV wallet or application.

You can also turn off unnecessary features if these APIs can already be covered by other services such as a local 
bitcoin node, or if you want to fetch raw transactions from a 3rd party API such as conduitdb.com / whatsonchain.com / 
TAAL. This flexibility avoids storing or indexing the 10TB+ blockchain twice. 

ConduitDB therefore fits into your existing stack to add value in a way that you only pay for what you use. 
It can either be run in a lightweight mode for the bare minimum required support for SPV wallets / applications 
like ElectrumSV or it can be configured to store and index absolutely everything or anything in-between - 
perhaps only scanning for a token sub-protocol or bitcom namespace of interest to you and your application.

An instance that indexes and stores everything is running at [conduit-db.com](http://conduit-db.com) and is free for 
public use but may need to be rate-limited depending on demand.

## Getting Started
ConduitDB deployment is docker-based only. ScyllaDB only loses
[3% performance in docker when properly configured](https://www.scylladb.com/2018/08/09/cost-containerization-scylla/)

ConduitDB connects to the p2p Bitcoin network so doesn't technically
require you to run your own full node. However, ideally you will have a 
bare metal, localhost bitcoind instance. You can get the latest version 
from here (https://www.bsvblockchain.org/svnode). See 
[notes](##-notes-on-using-a-non-localhost-node) below on using a [non-localhost node](##-notes-on-using-a-non-localhost-node).

These settings in the bitcoin.conf file are recommended:

    whitelist=127.0.0.1
    whitelist=172.17.0.1
    p2phandshaketimeout=300

Once you have a node to connect to, update the `.env.docker.production`
config file where it says:

    NODE_HOST=127.0.0.1
    NODE_PORT=8333


Set the other configuration options in `.env.docker.production` such as:
    
    SCYLLA_DATA_DIR=./scylla/data
    CONDUIT_RAW_DATA_HDD=./conduit_raw_data_hdd
    CONDUIT_RAW_DATA_SSD=./conduit_raw_data_ssd
    CONDUIT_INDEX_DATA_HDD=./conduit_index_data_hdd
    CONDUIT_INDEX_DATA_SSD=./conduit_index_data_ssd
    REFERENCE_SERVER_DIR=./reference_server

ConduitDB makes deliberate use of fast (HDD) vs slow (SSD/NVME) storage volumes for the data directories.
Raw blocks and long arrays of transaction hashes are written sequentially to HDD to economise on disc usage.
SSD/NVME is used for memory mapped files and of course ScyllaDB.

By default all directories will be bind mounded (by Docker Compose) into the above locations at the root directory 
of this cloned repository. If you're running in prune mode (deleting raw block data after parsing), then the defaults
will work well for you on NVME storage. This is the recommended configuration.

Running the production configuration is only supported on linux.
If you really want to test out the production configuration on windows you could use WSL.

    ./run_production.sh

To tail the docker container logs:

    ./tail_production_logs.sh


# Development
## Python Version
Currently `python3.10` is required

## Build all of the images

    docker build -f ./contrib/python_base/Dockerfile . -t python_base
    docker-compose -f docker-compose.yml build --parallel --no-cache

## Run static analysis checks

    ./run_static_checks.bat

Or on Unix:

    ./run_static_checks.sh

## Run all functional tests and unittests locally

    ./run_all_tests_fresh.bat

Or on Unix:

    ./run_all_tests_fresh.sh


## 2) Running ConduitRaw & ConduitIndex
### Windows cmd.exe:

    git clone https://github.com/conduit-db/conduit.git
    cd conduit-db
    set PYTHONPATH=.

Now install packages and run ConduitRaw (in one terminal)

    py -m pip install -r .\contrib\requirements.txt
    py .\conduit_raw\run_conduit_raw.py

And ConduitIndex (in another terminal)

    py -m pip install -r .\contrib\requirements.txt
    py .\conduit_index\run_conduit_index.py

## Unix Bash terminal

    git clone https://github.com/conduit-db/conduit.git
    cd conduit-db
    export PYTHONPATH=.

Now install packages and run ConduitRaw (in one terminal)

    python3 -m pip install -r ./contrib/requirements.txt
    python3 ./conduit_raw/conduit_server.py

And ConduitIndex (in another terminal)

    python3 -m pip install -r ./contrib/requirements.txt
    python3 ./conduit_index/conduit_server.py


## Configuration
All configuration is done via the `.env` files in the top level directory.

- `.env` is for bare metal instances of ConduitDB services when iterating in active development
- `.env.docker.development` is for the `docker-compose.yml` which is used for automated testing and the CI/CD pipeline
- `.env.docker.production` is for the `docker-compose.production.yml` which is
used for production deployments.


<h2 id="notes-on-using-a-non-localhost-node">
  Notes on using a non-localhost node
</h2>
Indexing from a non-localhost node is an experimental feature at the
present moment but I hope to improve upon this at a later date.

If you still want to go ahead with connecting to a remote node, 
ideally your IP address should be a whitelisted on this node and it should be a
low-latency connection (i.e. ideally in the same data center or at least in the 
same geographical region). Remote nodes that have not whitelisted you will likely throttle your initial 
block download.

Even with a local bitcoin node, if it's main raw block storage is on a 
magnetic harddrive, it will max out the sequential read capacity of the 
harddrive at around 200MB/sec. This becomes the speed limit for everything.


## Acknowedgments

- This project makes heavy use of the [bitcoinx](https://github.com/kyuupichan/bitcoinX) bitcoin
library created by [kyuupichan](https://github.com/kyuupichan) for tracking of headers and
chain forks in a memory mapped file
- The idea for indexing pushdata hashes came from discussions with
[Roger Taylor](https://github.com/rt121212121) the lead maintainer of [electrumsv](https://github.com/electrumsv/electrumsv)
