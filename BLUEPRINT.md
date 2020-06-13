# Chain indexer

# Confirmed Block Data

## Transactions table
    primary_key:     tx_num (NOT autoincrementing - calculated in py/cython algo)
    other columns:   tx_hash (indexed separately)
                     height
                     position
                     tx_offset (LMDB)

## IO table (inputs and outputs)

    tx_num (aka out_tx_hash + in_prevout_hash joined)
    out_idx
    out_value
    in_tx_num
    in_idx
    
## Pushdata_hashes
    
    pd_id (generated for uniqueness constraint) as PK
    tx_num  (indexed)
    idx (out_idx or in_idx)
    pushdata_hash  (indexed)
    ref_type (bit = 0 for output, bit = 1 for input)
    
- multi-idx on tx_num, idx, pushdata_hash - which also allows for situations where there is
no pushdata indexed (20, 33, 65 bytes length) from either the input or the output 
(if we have decided to not index it).
- normalizing out the pushdata_hashes is also essential for the many-to-many relationship of
pushdatas to inputs or outputs. To denormalize this would cause insane disc usage by duplicating
all of the input and output rows! 
- retrieving the relationship of pushdata <-> io is only 1 table join away and 
- utxos could be cached later if we wanted to without much trouble (to provide that as 
a frequently desired service)
    
# Mempool data
In short - use redis to store the same information as the above permanent postgres tables: 

    transactions, inputs, outputs and pushdata rows in whatever way is most useful.

# Pipeline
## TxParser requirement (will be heavily cythonised)

This design is entirely centered around how to most efficiently
store this into postgres in bulk with minimal table joins 
(whilst minimizing db bloat and denormalization in the process). This design is also very friendly
to the concept of postgres-XL with sharding on tx_hash for (approximately) linearly scaling
**write** throughput.

####Steps:
Track a global tx_num and increment it with the tx_count of the block (or block_partition) inside of
a multiprocessing.Lock'd code segment. 

Then allocate this sequence of tx_nums to 'tx_rows'

    (tx_num, tx_hash, height, position, offset)
    
Calculate 'input_rows' (that will -> a SQL UPDATE of the IO table)

    (prevout_hash, out_idx, tx_num, in_idx)

Append input to 'in_pushdata_rows'

    (tx_num, idx, pushdata_hash, ref_type=1)  # NOTE pushdata_hash may be null
    
Calculate 'output_rows' (that will be bulk copied directly to IO table)

    (tx_num, idx, value)
    
Append output to 'out_pushdata_rows'

    (tx_num, idx, pushdata_hash, ref_type=0)

so in the end there are:

    tx_rows =       [(tx_num, tx_hash, height, position, offset)...]
    in_rows =       [(prevout_hash, out_idx, tx_num, in_idx)...)...]
    out_rows =      [(tx_num, idx, value)...)]
    pd_rows =       [(tx_num, idx, pushdata_hash, ref_type=0 or 1)...]

## Bulk postgres insert algorithm

1) Bulk copy directly to the transaction table
2) Bulk copy directly to the io table (for the **outputs** - i.e. input columns blank)
    - sidenote: if we wanted utxos cached now would be the time to update redis too...
3) Inputs are trickier
    - Bulk copy to temporary table
    - INNER JOIN on transaction table to convert prevout_hash to out_tx_num
    - Bulk UPDATE of input columns based on the tx_num + idx matching
4) Bulk copy directly to the pushdata table (**outputs and inputs together**)

NOTE: step 4 is only made possible if an edge case is ruled out!:
- there's an edge case where the input/output could have the same (tx_num AND idx
AND pushdata_hash AND same ref_type) --- but it's probably very rare... 
so options are to either rule out input or output row duplicates in the cython parser (**much preferred 
and much faster**) or avoid bulk copy and instead do bulk upsert with an ON CONFLICT DO NOTHING; 
to account for this possibility)


## MerkleTree table (LMDB vs postgres)
Merkle Tree key-value store and db structure stored only to mid-level only to avoid 
excessive disc usage.:

    {header_id + depth + position: binary hashes}
    
    where header_id + depth + position is a concatenation of uint_32 integers
    to form the key

    at depth = 0 have 2^0 32-byte hashes - so keys are (0,0)
    at depth = 1 have 2^1 32-byte hashes etc. - so keys are (0,0) and (0,1)
    at depth = 2 have 2^2 32-byte hashes concatenated side by side...


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


## Tech Stack

Dream stack for performance of a python chain indexer would be:

    1. uvloop
    2. BufferedProtocols - 600MB/s - (for zero-copy streaming of block data)
    3. cython optimized parsers etc. that operate on shared memory views (see 6) of each transaction
    4. asyncpg -> postgres (blindingly fast asyncio/uvloop based driver for postgres)
    5. multicore parsing of block / mempool data and committing to db
    6. new python3.8 shared memory feature for multicore workers to parse blocks with zero-cpy
    
    Overall "dumber indexer" design (only pulling out what is needed - no utxo tracking).
