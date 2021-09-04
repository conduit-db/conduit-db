# Chain indexer Schema (In Flux)

## Principles for write optimization

In a nutshell: "Minimize random disc IO ops".

LSM databases are by far superior to B-tree databases for handling
heavy random writes. It turns out that tx_nums for the keys is not worth it
when considering the pipeline end-to-end. It is far better to just accept that 
the random writes need to be "eaten" one way or another (with all of those tx 
hashes and pushdata hashes). If you try to avoid it, you end up seeking
around on the disc to unpick the relationships between things and this is far
less efficient than doing large bulk sequential writes of SST tables into an LSM
database. That's what it's designed to do!

I considered short hashing to 8 bytes instead of 32 bytes but in the end, the 
added complexity of maintaining collision tables starts to cost you on the read 
side and the complexities explode when you consider a distributed, horizontally
scalable system for all parts of the pipeline. Ruling: "Not worth the hassle"

MyRocks (MySQL variant) - which uses RocksDB as the LSM-based engine gives
far superior performance for these workloads than any B-tree database and 
you can basically be confident that whatever throughput you're getting with
testing will be mostly sustainable for larger database sizes. Very much NOT
the case for B-tree databases...

# Confirmed Block Data

## Confirmed Transactions table

    tx_hash BINARY(32) PRIMARY KEY,
    tx_height INT UNSIGNED,
    tx_position BIGINT UNSIGNED,
    tx_offset_start BIGINT UNSIGNED,
    tx_offset_end BIGINT UNSIGNED

Consider whether tx_height should be changed to tx_block_hash or tx_block_num
so that on a reorg, the Transaction table does not need to be modified at all.

However it would lead to duplicate tx_hashes in the Transaction table...
Which is a PRIMARY KEY. Would need to relax the uniqueness constraint
and then pick the tx with block hash that is on the longest-chain.

Using tx_height leads to the API doing a "stop-the-world" repair whilst the
client would need to wait a few seconds... (a much easier implementation 
though)

An argument can be made that in an ideal world SPV clients should be able
to request merkle branches for both sides of a fork immediately (rather than
having to wait until a winner emerges) because if they download
the wrong one and a long time passes by, their backed up merkle proof will
be invalidated (and possibly pruned!!)

## Inputs

    out_tx_hash BINARY(32),
    out_idx INT UNSIGNED,
    in_tx_hash BINARY(32),
    in_idx INT UNSIGNED,
    in_offset_start INT UNSIGNED,
    in_offset_end INT UNSIGNED

## Outputs

    out_tx_hash BINARY(32),
    out_idx INT UNSIGNED,
    out_value BIGINT UNSIGNED,
    out_offset_start INT UNSIGNED,
    out_offset_end INT UNSIGNED

## Pushdata_hashes

    pushdata_hash BINARY(32),
    tx_hash BINARY (32),
    idx INT,
    ref_type SMALLINT
    
## Mempool Transactions table

    mp_tx_hash BINARY(32) PRIMARY KEY,
    mp_tx_timestamp TIMESTAMP,
    mp_rawtx LONGBLOB

## API State table

    id INT PRIMARY KEY,
    api_tip_height INT,
    api_tip_hash BINARY(32)

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

#### Further considerations:
When a new block is mined, a check needs to be done to see which transactions 
have already been processed.

The api_chain_tip_height modifies the queries for pushdata_hash history in that the inner join for confirmed history
with the confirmed transactions table will exclude txs above the api_chain_tip_height because the presence of these
txs indicates that the block is not fully committed to the database yet (+ mempool txs invalidated) - if the block
WAS fully committed then the api_chain_tip_height would be +=1.


## Headers
Use bitcoinx Headers object for tracking chain forks and the current chain tip etc.
See Reorg Handling section for details.

## Blocks
The block headers are tracked the same as above headers but lag behind (and track the 'tip' of sync'd blocks) so 
that the first, complete set of headers acts as 'training wheels' for the IBD process.

The raw blocks and rawtx data will be dumped into an LMDB database using append only mode which is very performant 
for bulk writes. Txs can the be retrieved with the offset (stored via the Transaction table)

There will be two main LMDB tables for blocks:

#### Blocks table

    block_num (key) - STRICTLY sequential to achieve append-only writes
    raw_block (val)

#### Block numbers table
The sole reason for this table to exist is so that the Blocks table has sequential keys 
and can turn on append-only mode - which is the only reason that using LMDB is in any way acceptable for
heavy write throughput.

    block_hash (key) - 32 byte block hash
    block_num (val) - uint32_t
    
## Tx Offsets Set
Stored in LMDB key is block_hash and value is tx_offsets array

## Reorg Handling
1. Delete all transactions with a height == to a reorged block.
2. Request the two or more reorging blocks from the node (avoid compact block 
protocol complexities for now and just request the entire raw block again)
3. Sync to tip as usual (and invalidate mempool txs via the usual procedure
as they are added to the confirmed transaction table)
4. Now scan the entire remaining mempool for double spends on the basis of
referencing the same outpoint as a confirmed block (one of the two reorg blocks).
5. Possibly other safety checks for malleated transactions (i.e. the tx may
have been orphaned by the reorg and not references a UTXO that doesn't exist)
6. Flush everything to disc, update the API chain tip to make the new
changes visible and unlock the API to begin serving requests once again.

Clients should be using a 30 second timeout so the aim would be to complete this
full procedure in <10 seconds or so. But if it 
was a really big reorg, the clients would probably timeout and have to reconnect 
when the service has completed the reorg handling procedure. 

It's not perfect, but it should work for an initial proof of concept.

## Architecture

See ARCHITECTURE.md

