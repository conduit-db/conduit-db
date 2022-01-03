## ConduitIndex

Horizontally scalable and shardable transaction parser
(depends on ConduitRaw's preprocessed blockchain data)

Maintains an index of:
- Confirmed Transactions
- Mempool Transactions
- Inputs
- Outputs
- Pushdata hashes
- Headers and block metadata

MyRocks SQL database for indexed transaction data
(Which is structured in a highly shardable way so as to allow
for migration to a distributed SQL or similar db backend in future).

The full raw block, transaction data and merkle proof data is stored
with ConduitRaw and can be requested over a lightweight TCP socket server.
