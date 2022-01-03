## ConduitRaw
Very fast preprocessor for raw block data in the ConduitDB pipeline
- RawBlocks
- MerkleTree
- Tx Offsets (byte offset in the raw block)
- Block metadata

Uses a combination of LMDB (for mappings) and flat files (Where the
vast majority of data is stored).

Access patterns are designed for storing the flat file data on
spinning disc for cost savings.

## AiohttpAPI
This service also hosts a REST API
