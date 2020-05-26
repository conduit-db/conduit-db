# Chain indexer

## Transactions table
The fastest way to get all txids affected by a reorg is by paging through all 
txids in a block - see MerkleTree table - just get the deepest level
of the merkle tree for all txids in a block. This could be parallelised if need-be.

    primary_key:        txid
    other columns:      height
                        tx_offset (LMDB)

## History table
(Doesn't need to change on a reorg) 

pushdata field ignores multiple occurences of pushdatas for a given txid - only indicates
whether it is included in either an input or an output of the tx

    combined idx:   pushdata (pubkey length bytes or p2pkh length=20 bytes)
                    txid

The History table (below) would be optimized for 

	pubkey_id -> history of all txids

queries such as:

	SELECT * FROM TxHistory WHERE pushdata=<identity pubkey>
	SELECT * FROM TxHistory WHERE pushdata=<identity pubkey> AND txid="aad56646ad56a7..."
	SELECT COUNT(*) FROM TxHistory WHERE pushdata=<identity pubkey>
	
NOTE: Each row of the history table is immutable - which has performance advantages.
	
## MerkleTree table (LMDB vs postgres)
Merkle Tree key-value store and db structure:

    {header_id + depth + position: binary hashes}
    
    where header_id + depth + position is a concatenation of uint_32 integers
    to form the key

    at depth = 0 have 2^0 32-byte hashes - so keys are (0,0)
    at depth = 1 have 2^1 32-byte hashes etc. - so keys are (0,0) and (0,1)
    at depth = 2 have 2^2 32-byte hashes concatenated side by  ... lots of keys
    
    when everything is in binary it offers the opportunity later to "cythonize" all of the CPU intensive parts 
    and achieve C-like performance.
    
    The postgres DB tables would be very basic - just storing the key: value pairs as raw binary.
    
If you needed to retrieve all of the txids in a block in sequential order (e.g. for handling a reorg and updating the 
Transactions table) you would for example... let's say there are 2048 txs in the block -> log2(2048) = 11 levels to the
full merkle tree. So we use the header_id + depth + position where depth = 11 and position is the range 0 to 2047. 
The postgres db would be indexed on this key and so would be a sequential readout of rows in bulk.

NOTE: The most recent 20 blocks would be stored in the redis key-value store anyway so a big db query to postgres
would rarely if ever occur - making reorg handling very fast. This postgres table would only be read from 
for serving up old (> 20 blocks old) merkle proofs for individual txs (client queries).


## Headers
This would be based on the bitcoinx Headers object and how ElectrumX does this to track chain forks and the current 
chain tip etc.

NOTE: header_id would be a foreign key of the MerkleTree table in order to save space (rather than
having the blockhash be part of the merkle tree key for millions of keys... 
that's 32MB for 1 million x 32-byte blockhashes repeated over and over needlessly 
Instead we use a 4 byte integer unique `header_id`).

## Blocks
The block headers are tracked the same as above headers but lag behind (and track the 'tip' of sync'd blocks) so 
that the first, complete set of headers acts as 'training wheels' for the IBD process.

The raw blocks and rawtx data will be dumped into an LMDB database using append only mode which is very performant 
for bulk writes. Txs can the be retrieved with the offset (stored via the Transaction table)

## DB table design tradeoffs

The Transactions table would be used as follows for a reorg event:

`UPDATE Transactions SET height=<new height> WHERE txid=<txid>`

Each txid lookup is O(1) so not bad.

The payoff is that you get **low overhead PARSING AND WRITE** performance to keep up with the crazy STN volumes...

## Reorg Handling
On reorg -> lookup all relevant blockhashes (from merkle tree db - the deepest row with all txids in order)
-> all affected txids and update the Transactions table (History table doesn't need to change)


# Architecture

Dream stack for performance of a python chain indexer would be:

    1. uvloop
    2. BufferedProtocols - 600MB/s - (for zero-copy streaming of block data)
    3. cython optimized parsers etc. that operate on shared memory views (see 6) of each transaction
    4. asyncpg -> postgres (blindingly fast asyncio/uvloop based driver for postgres)
    5. multicore parsing of block / mempool data and committing to db
    6. new python3.8 shared memory feature for multicore workers to parse blocks with zero-cpy
    
    Overall "dumber indexer" design (only pulling out what is needed - no utxo tracking).

#### Merkle Tree calculation

the merkle tree must be calculated in full before signalling block completion (2 above) and then all or part of it stored to db (there's a trade off to
what depth it should be stored, pre-calculated)... for every level you do not store (i.e. the very bottom layer)
you halve the storage space required... so probably a good trade-off is to at least knock off the last 3 levels
so that a merkle proof calculation requires fetching of only 8 transaction hashes to re-calculate up to the 
pre-calculated level. - the MVP would probably just store the entire merkle tree for simplicity of 
implementation.
- merkle tree calculation is easily parallelisable - but may not be necessary.
        
see MerkleTree db above
    
## Parallelising block / tx / merkle tree calculation
- see conduit.workers.py
