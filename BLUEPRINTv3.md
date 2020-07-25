# Chain indexer (Schema v3)

## Principles for write optimization

1) Minimize number of random IO ops
2) Minimize size of database and the size of the index - to optimize DB:RAM ratio

Regardless of what is done, there is no avoiding at least 
1 x random IO for each output, input, tx and pushdata hash 
(at least 4-5 x random IO ops for each tx).
you can hide it behind a deferred random READ (in the form of 
table joins to acquire the correct tx_num before the UPDATE 
but you cannot actually avoid it - READ IO ops are just as bad 
as WRITE IO ops).
    
So instead of kidding ourselves that we can make this go away by using sequentially 
incrementing tx_nums and marvelling at how great the sequential write thoughput is for tx outputs...
We should accept that we must eat all of this random IO at some point and take it head on.

If we do this, we can drop the waste-of-space tx_nums from all tables and replace it with an aggressively
shortened tx_hash instead (an 8 byte signed int64_t - to match Postgres BigInt).

The following schema will no doubt improve things considerably with Postgres but the reality is that
B-tree indexes do not scale well for random writes for a DB:RAM ratio > around 4:1.

Therefore the ultimate plan is to swap out the backend with MyRocks (MySQL variant) - which will use 
RocksDB - an LSM based engine for much better sustained random write throughput when DB size is 
significantly > RAM. It also has the needed tools for future horizontal scaling if that is required.

# Confirmed Block Data

    NOTE: shash = "short hash"

## Confirmed Transactions table
    primary_key:     tx_shash (primary key with unique constraint)
                     tx_hash (NOT INDEXED)
                     height
                     position
                     tx_offset_start
                     tx_offset_end
                     has_collided  (boolean 0 or 1) - referring to 'short hash' (WITHIN CONFIRMED TX TABLE ONLY)
                     
    (65 bytes per row)

## IO table (inputs and outputs)

    out_tx_shash (aka out_tx_hash + in_prevout_hash joined) - indexed
    out_idx
    out_value
    out_has_collided  (boolean 0 or 1)
    in_tx_shash
    in_idx
    in_has_collided  (boolean 0 or 1) 
    
    NOTE: 'in_has_collided' is needed BUT AN INDEX ON in_tx_shash is NOT NEEDED.
    Inputs have prev_out_hash so can get from colliding tx to the input via the out_tx_shash
    
    (32 bytes per row)
    
## Pushdata_hashes
    
    pd_shash  (indexed)
    tx_shash (not indexed)
    idx (out_idx or in_idx)
    ref_type (bit = 0 for output, bit = 1 for input)
    pd_has_collided (boolean 0 or 1)

    (22 bytes per row)
    
So roughly speaking 540 million transactions on mainnet should fit in under 54GB 
(without factoring in compaction of RocksDB engine). Which gives Postgres a fighting 
chance but will definitely shine with MyRocks.

- retrieving the relationship of pushdata <-> io is only 1 table join away 
- utxos could be cached later if we wanted to without much trouble (to provide that as 
a frequently desired service)

## Mempool Transactions table

    primary_key:    mp_tx_shash bigint PRIMARY KEY, (with unique constraint)
                    mp_tx_hash bytea, (NOT INDEXED)
                    mp_tx_timestamp timestamptz, (INSTEAD OF HEIGHT IN CONFIRMED TX TABLE)
                    mp_tx_has_collided boolean,  (boolean 0 or 1) - referring to 'short hash' (WITHIN MEMPOOL TX TABLE ONLY)
                    mp_rawtx bytea
    
When IBD is complete, need to request the full mempool and begin accepting relayed mempool txs.
These 'tx' messages can be fed to the TxParser and the pushdata_rows, input_rows, output_rows
**can be treated exactly the same as for confirmed transactions (these records are immutable!)**
BUT the **tx_rows** need to go to a **Mempool transaction table** rather than the confirmed transaction table. 

The mempool transaction table has timestamps in lieu of block heights.

Queries for e.g. pushdata will do an inner join with the confirmed transaction table for confirmed history
and should exclude height > api_chain_tip (see: api state table below).

Insertions for tx_rows will go in this order for new blocks:
    
    1) insert pushdata, inputs, output rows THAT ARE MISSING (i.e. compatible with compact block paradigm)
    2) insert ALL tx_rows in raw block to the confirmed table (but do not invalidate mempool txs yet!)
    3) ATOMICALLY 
        a) invalidate all confirmed tx_hashes from the mempool tx table + 
        b) update the api_chain_tip
        - this has the effect of atomically causing the API to stop returning the unconfirmed txs
        whilst adding these txs to the return value for *confirmed* history.

and an inner join with the mempool transactions table for unconfirmed history.


## API State

    api_chain_tip_height    (only incremented when the postgres tables are fully ready to accept queries for this height)
    api_chain_tip_hash

The api_chain_tip_height modifies the queries for pushdata_hash history in that the inner join for confirmed history
with the confirmed transactions table will exclude txs above the api_chain_tip_height because the presence of these
txs indicates that the block is not fully committed to the database yet (+ mempool txs invalidated) - if the block
WAS fully committed then the api_chain_tip_height would be +=1.

# Collision tables (tx, input, output)

Need to track all colliding tx_hashes.
Basically the procedure is:
- Do bulk insert of 10,000 transaction rows... catch the exception if there is a duplicate entry and 
rely on the database to roll it back (it's a rare event).
- Only if the batch fails do we then find out which tx_shash collided. This probably involves a 
table join of the batch -> temporary table with the permanent transaction table to get the matching
tx_shash... 
- then need to update permanent transaction table row with "hash_collided=True"
- we cannot insert duplicate primary keys for this table so would need to then record both of
these transactions in the specially provisioned "tx_hash_collisions table"...
- now when doing reads we merely need to check if the "hash_collided" flag is set to "True".
in the rare event that it is True need to take extra measures to check the "tx_hash_collisions table"
and request the transaction row by its full 32 byte tx_hash instead of the shortened 8 byte tx_shash.
- to get full tx_hash can avoid wasted RAM consumption for such a rare event and just lookup the rawtx
in LMDB raw block and re-hash it.
- Finally, this collision event needs to be propagated to every SQL table for ****all affected
input and output rows**** BUT WAIT... This means that the tx_shash for inputs will need an index!
But maybe its not needed because inputs by definition have the out_tx_shash right so can lookup 
based on that...
- The handling of colliding hashes should not be exposed in the external API - it should be
entirely handled internally such that the client always receives the correct metadata for the correct
transaction without necessarily appreciating how it happened under the hood.

Pushdata short hashes are 16 bytes so the chances of collisions are negligible. 
No such collision table will exist for these.


# Pipeline
## TxParser requirement (will be heavily cythonised)

This design is entirely centered around maximizing bulk writes with minimal table joins
for horizontal scaling in future (i.e. sharding on tx_hash).

#### Steps:
The main reference will be the 'short tx_hash' i.e. "tx_shash".

Then allocate this sequence of tx_shash to 'tx_rows'

    (tx_shash, height, position, offset_start, offset_end, has_collided==0)
    
Calculate 'input_rows' (that will -> a SQL UPDATE of the IO table)

    (out_shash, out_idx, in_tx_shash, in_idx)

Append input to 'pd_rows'

    (pushdata_shash, in_tx_shash, idx, ref_type=1)  # NOTE pushdata_hash may be null
    
Calculate 'output_rows' (that will be bulk copied directly to IO table)

    (out_tx_shash, idx, value)
    
Append output to 'pd_rows'

    (pushdata_hash, out_tx_shash, idx, ref_type=0)

so in the end there are:

    tx_rows =       [(tx_shash, tx_hash, height, position, offset_start, offset_end, has_collided_flags)...]
    in_rows =       [(prevout_hash, out_idx, tx_num, in_idx, has_collided_flags)...)...]
    out_rows =      [(out_tx_shash, idx, value, out_has_collided, None, None)...)]
    pd_rows =       [(pushdata_shash, tx_shash, idx, ref_type=0 or 1, has_collided_flags)...]
    
NOTE: 'has_collided' should probably by a "bit string" (postgres) or a "tinyint" (MySQL) and
use flags like this:

    tx_has_collided =   1 << 0 
    in_has_collided =   1 << 1
    out_has_collided =  1 << 2
    pd_hash_collided =  1 << 3

The flag refers to the specific row of the table that the flag is set on...
So for the IO table there could be cases where the out_has_collided flag is set but
the in_has_collided flag is NOT set because the tx_hash for the input has not collided.

## Bulk postgres insert algorithm

1) Bulk copy directly to the transaction table 
    - if no unique constraint violation continue (overwhelmingly most likely path)
    - if constraint violation then batch fails and need to handle this special case.
        - table join batched tx_rows (via temp table) to transaction permanent table to get 
        the colliding row(s).
        - now update the permanent table flags for **tx, inputs and outputs**
        - extract the relevant input and output rows from their respective pending in_rows
        and out_rows batches (should only be doing max of 10,000 rows in a batch)...
        - now insert both colliding transaction entries to the collision tables (which
        DO NOT have a unique constraint for tx_hash).
2) Bulk copy directly to the io table (for the **outputs** - i.e. input columns blank)
    - sidenote: if we wanted utxos cached now would be the time to update redis too...
3) Inputs are trickier
    - **check collisions table(s) for collisions on the in_tx_shash then proceed**
    - no need to check collisions table for the out_tx_shash because step 1 handles this
    possibility.
    - Bulk copy to temporary table
    - Bulk UPDATE of input columns where out_tx_shash + idx matches
4) Bulk copy directly to the pushdata table (**outputs and inputs together**)
    - if constraint violation occurs, follow similar steps as for (1). 
        - i.e. update collision flags in permanent tables
        - insert both colliding pushdata_hash rows to the collision table.

NOTE: step 4 is only made possible if an edge case is ruled out!:
- there's an edge case where the input/output could have the same (tx_num AND idx
AND pushdata_hash AND same ref_type) --- but it's probably very rare... 
so options are to either rule out input or output row duplicates in the cython parser 
(**much preferred and much faster**) or avoid bulk copy and instead do bulk upsert 
with an ON CONFLICT DO NOTHING; to account for this possibility)


## MerkleTree table (LMDB vs postgres)
Merkle Tree key-value store and db structure stored only to mid-level only to avoid 
excessive disc usage.:

    {header_id + depth: hash array}
    
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

There will be two main LMDB tables for blocks:

#### Blocks table

    block_num (key) - STRICTLY sequential
    raw_block (val)

#### Block numbers table
The sole reason for this table to exist is so that the Blocks table has sequential keys 
and can turn on append-only mode - which is the only reason that using LMDB is in any way acceptable for
heavy write throughput.

    block_hash (key) - 32 byte block hash
    block_num (val) - uint32_t

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
