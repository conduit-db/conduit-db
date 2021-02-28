## ConduitIndex

Horizontally scalable and shardable transaction parser (depends on ConduitRaw)

Writes to mmap header files and MyRocks SQL database for indexed transaction data
(Which is structured in a highly shardable way so as to allow for migration to
a distributed SQL or similar db backend in future).

Only reads from ConduitRaw's LMDB database tables (later this will require a 
network API abstraction to move beyond a single machine). But for now can benefit 
from low latency, direct access.
