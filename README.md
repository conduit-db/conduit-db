# Conduit

(In early development - not fully implemented yet)

As the original and unadulterated bitcoin protocol (Bitcoin SV) scales there are new
challenges for keeping applications responsive under load. Scaling-testnet's > 2GB
blocks can take some time to index and leads to clients waiting
unreasonable times for mempool events and confirmations.

Conduit can parallelize the chain indexing process across multiple cores
(16, 32, 64 etc.) so wallets/applications can get timely notifications.

## Project Status

Conduit is still in early development. So far it can:

    1. Use the new python3.8 BufferedProtocol to connect to a localhost node with 
    transfer speeds of 500-600MB/sec.
    2. Do an initial sync of all headers and can stop/start and saves progress
    3. Do an initial sync of all blocks and can stop/start and saves progress
    4. Connect to mainnet, testnet, regtest, scalingtestnet nodes for the above purposes.
    5. Skeleton for 4 dedicated worker processes is in place to make use of python3.8's
    multiprocessing.shared_memory + BufferedProtocol such that it can read multiple p2p protocol
    messages into one contiguous, large buffer (e.g. >2GB) of shared_memory. 
    - The synchronizing of buffer resets with worker processes was the hardest part but that's
    completed now.
    6. Cythonized pre-processor prototype is complete - see 'bench' folder which finds all of the
    start byte positions of txs in a block at around 3200 MB/sec on a single core.
    7. Cythonized tx parser prototype mostly complete - see 'bench' folder which parses txs at these
    byte positions in shared memory to pull out the tx_hash and pk/pkh associated with the input or
    output script at around 80-90MB/sec per core.
    8. Merkle Tree calculation not implemented yet but tx offsets are being fed to this worker via a
    queue. Just needs an algorithm for hashing out the tx_hashes and storing it in LMDB.
    9. Design decision made on LMDB for synchronous writes of raw blocks (in append-only mode) which should
    in theory match the write speed of the underlying storage media. Read performance is better than any other
    key-value store that I've seen by quite a margin. Currently the worker gets the block start/stop 
    byte position offsets for the raw blocks in shared_memory - just a matter of performing the write 
    operation.
    10. Design decision made on postgres for storing stripped tx metadata (such as tx_hash, height, 
    pk and pkhs, byte offset in the raw block) but *not* the full rawtx (which will be retrieved 
    from LMDB raw blocks via the byte offset for the tx).
    11. Some kind of external api (in this repo or a different repo)

So mostly just need to connect it all up to LMDB and postgres to actually persist the data but 
should not be much work left. 
I think it can probably stay under 4000 lines of code (excluding an external API for it and unittesting) 
- currently only at around 2000 LOC

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
