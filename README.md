# Conduit

(In early development - not fully implemented yet)

As the original and unadulterated bitcoin protocol (Bitcoin SV) scales there are new
challenges for keeping applications responsive under load. Scaling-testnet's > 2GB
blocks can take some time to index and leads to clients waiting
unreasonable times for mempool events and confirmations.

Conduit can parallelize the chain indexing process across multiple cores
(16, 32, 64 etc.) so wallets/applications can get timely notifications.

There are two approaches one can take:
1. 1 core -> 1 block and do many blocks in parallel (geared like a formula1 race car)
2. many cores -> 1 block and chug through 1 (partitioned) block at a time (geared like
a diesel truck)

Approach 1 is great for whipping through thousands of tiny <1MB blocks but it has
**no impact on responsiveness to indexing the most recent big block!** The other cores
cannot help and so it will take a very long time (maybe several minutes) before the
pub-sub notifications go out. So you get nice looking IBD stats to tell your friends about
but **zero impact where it actually matters**!

So in my view, approach #1 offers little value.

Option 2 on the other hand will be ideal for handling very large blocks where it can
partition the block and deploy all available resources to deal with it as quickly as
possible. This is the approach that conduit takes.

## Project Status

Conduit is still in early development. So far it can:

    1. Do an initial sync of all headers and can stop/start and saves progress
    2. Do an initial sync of all blocks and can stop/start and saves progress
    3. Insert the transactions into a basic postgres "Transaction" table
    4. Connect to mainnet, testnet, regtest, scalingtestnet nodes for the above purposes.
    5. Currently does this on a single core but does so via a pure function that can
    be outsourced to any number of worker processes (not implemented yet).
    The parsing algorithm is also not optimized at all as it converts to and from
    python objects rather than staying in binary (and in theory could also be
    'cythonized'). In saying that it does about 50,000tx/sec for blocks > 1MB.

Currently it will not continue syncing to the latest new blocks that come in - only does
the initial sync and then stops. Also does not calculate the merkle tree or have a database
table for it (yet). And it goes without saying that a redis instance (for an LRU cache of
the merkle tree and other things) is not in the picture yet nor is some kind of external API
that might wrap all of this (perhaps best left for a separate repo?).

See `BLUEPRINT.md` for the overall design plans.

## Getting Started

To run conduit (Windows)

    git clone https://github.com/AustEcon/conduit.git
    cd conduit
    py -m pip install -r .\requirements.txt
    py .\conduit_server.py

To run conduit (Linux)

    git clone https://github.com/AustEcon/conduit.git
    cd conduit
    python -m pip install -r requirements.txt
    python .\conduit_server.py

That's basically it.

## Testnet / Regtest / Scaling-Testet

    python .\conduit_server.py --testnet

    python .\conduit_server.py --regtest

    python .\conduit_server.py --scaling-testnet

currently the peers are hardcoded so the host / port cannot be
passed in via commandline (yet).

## Acknowedgments

- This project makes heavy use of the [bitcoinx](https://github.com/kyuupichan/bitcoinX) bitcoin
library created by [kyuupichan](https://github.com/kyuupichan) for tracking of headers and
chain forks in a memory mapped file, parsing transactions and generally simplifying binary packing / unpacking etc.
- Many of the ideas/insights for this project, the requirements and the idea for greatly
simplifying the indexing process to only pubkeys and pubkey hashes came from
[Roger Taylor](https://github.com/rt121212121) the lead maintainer of [electrumsv](https://github.com/electrumsv/electrumsv)
- Other aspects of the codebase are also inspired by or borrowed from [electrumsv](https://github.com/electrumsv/electrumsv).
