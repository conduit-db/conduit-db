# Chain indexer

## Transactions table
    primary_key:     tx_num (autoincrementing int) - used for table joins
    other columns:   tx_hash (indexed separately)
                     height
                     position
                     tx_offset (LMDB)

## Inputs table

    clustered idx:   prevout_hash
                     out_idx
                     pushdata_id
    other columns    in_tx_num
                     in_idx

## Outputs table

    clustered idx:   out_tx_num
                     out_idx
                     pushdata_id
    other columns    value

## Pubkeys table

    primary_key:     pushdata_id
                     pushdata (pk or pkh for example)

## Query plan
Tx parser should extract these rows directly from a block without any need to query the database:
- tx_row:       (tx_num, tx_hash, height, pos, offset)
- in_row:       (prevout_hash, out_idx, pushdata_id, in_tx_num, in_idx)
- out_row:      (out_tx_num, out_idx, pushdata_id, value)

notice that the in_row uses `prevout_hash` and not `out_tx_num` - this is because it would require a db query at tx parsing
time to fetch it which is unacceptable for performance reasons. Therefore we directly store what is readily
available during the tx parsing process. There is no disadvantage when it comes to table joins for querying key history
because a table join with the transaction table is needed anyway.

Query for key history becomes (maybe something like this - but better optimized):
    
    Then query becomes:
    
    WITH filtered_ins AS (
        SELECT * 
        FROM inputs 
        WHERE pushdata_id in {12345}
    )
    WITH filtered_outs AS (
        SELECT * 
        FROM outputs 
        WHERE pushdata_id in {12345}
    )
    WITH outs_with_tx_hash AS (
        SELECT *
        FROM outputs
        JOIN transactions 
        ON transactions.tx_num = outputs.tx_num
    )
    SELECT *
    FROM outs_with_tx_hash
    -- should get rows where pk or pkh is in input scriptsig but not in corresponding output 
    --     which is basically unheard of but theoretically possible I guess...)
    -- should get rows where pk or pkh is in output scriptpubkey but not in corresponding input (common)
    FULL OUTER JOIN filtered_ins
    ON outs_with_tx_hash.tx_hash = filtered_ins.tx_hash AND outs_with_tx_hash.out_idx = 
        filtered_ins.in_idx


Also need to then join with Transaction table to convert `in_tx_num` and `out_tx_num`
to `in_tx_hash` and `out_tx_hash` respectively + `height` column. Client receives an array of:

    [(in_tx_hash, in_indx, out_tx_hash, out_idx, pushdata_id, value, height),
     (in_tx_hash, in_indx, out_tx_hash, out_idx, pushdata_id, value, height),
     (in_tx_hash, in_indx, out_tx_hash, out_idx, pushdata_id, value, height),
     ...                                                                     ]

If `in_tx_hash` is null then it is a utxo (not yet spent).

## MerkleTree table (LMDB vs postgres)
Merkle Tree key-value store and db structure:

    {header_id + depth + position: binary hashes}
    
    where header_id + depth + position is a concatenation of uint_32 integers
    to form the key

    at depth = 0 have 2^0 32-byte hashes - so keys are (0,0)
    at depth = 1 have 2^1 32-byte hashes etc. - so keys are (0,0) and (0,1)
    at depth = 2 have 2^2 32-byte hashes concatenated side by side...

Store only to mid-level only to avoid excessive disc usage.

## Headers
Use bitcoinx Headers object for tracking chain forks and the current chain tip etc.

NOTE: header_id would be a foreign key of the MerkleTree table in order to save space (rather than
millions of blockhashes...)

## Blocks
The block headers are tracked the same as above headers but lag behind (and track the 'tip' of sync'd blocks) so 
that the first, complete set of headers acts as 'training wheels' for the IBD process.

The raw blocks and rawtx data will be dumped into an LMDB database using append only mode which is very performant 
for bulk writes. Txs can the be retrieved with the offset (stored via the Transaction table)

## Reorg handling...

When there is a reorg... the affected raw block(s) would be fetched and a list of all tx_hashes
compiled via double_sha256 at each tx offset.

Then the affected txs in Transactions table either have their `height`, `position` and `offset`
reset to (0, null and null) or they get a new height, position and offset (if included in the new block(s))

The database update (and therefore affecting client side queries) should ideally be done as one atomic event.
However, the concern would be a deep reorg leading to OOM...
may need to lock any client queries until the db is once again in a consistent state... if the full reorg
handling can be completed in <10 seconds maybe that could be acceptable (seeing as though it's quite rare... ).

Clients should be using a 30 second timeout so might be okay to just have a delayed response until reorg is
fully dealt with. I am not 100% sure what's best at this stage.


# Architecture

Dream stack for performance of a python chain indexer would be:

    1. uvloop
    2. BufferedProtocols - 600MB/s - (for zero-copy streaming of block data)
    3. cython optimized parsers etc. that operate on shared memory views (see 6) of each transaction
    4. asyncpg -> postgres (blindingly fast asyncio/uvloop based driver for postgres)
    5. multicore parsing of block / mempool data and committing to db
    6. new python3.8 shared memory feature for multicore workers to parse blocks with zero-cpy
    
    Overall "dumber indexer" design (only pulling out what is needed - no utxo tracking).

