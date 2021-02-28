## Architecture (In Flux)

Generally speaking there is a linear pipeline from the bitcoin node, through to 
ConduitRaw and then on to ConduitIndex and finally ConduitAPI (not built yet) to
serve client requests.

### Persistent Data Stores

#### LMDB
Read-only from the perspective of ConduitIndex
Writes only performed by ConduitRaw
- Raw Blocks
- Merkle Tree
- Full TxId set
Block Hashes are the keys for all tables
LMDB does not touch mempool transaction data whatsoever

#### MySQL / MyRocks / rocksdb engine
Read-only from the perspective of ConduitRaw (which MAY use the mempool
table for implementing the compact block protocol - but then again it may
keep it's own independent record of mempool txs).

Writes only performed by ConduitIndex

#### Headers (two sets)
Writes only by ConduitRaw. 

ConduitIndex does not directly access these, it must ask ConduitRaw (via ZMQ)
what its chain tip is and go via the API abstraction to ensure it only
issues requests that can be served.

### P2P Network Connection
ConduitRaw receives:    Headers, Blocks
ConduitIndex receives:  Mempool Transactions

To be continued...
