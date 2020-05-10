# Chain indexer

## Transactions table
The fastest way to get all txids affected by a reorg is by paging through all 
txids in a block - see MerkleTree table - just get the deepest level
of the merkle tree for all txids in a block. This could be parallelised if need-be.

    primary_key:        txid
    other columns:      height
                        transaction bytes
                        32/33 byte unique pushdatas(pubkeys)
                        20 byte unique pushdatas (pkh)

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
	
## MerkleTree table
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

NOTE: The most recent 20 blocks would be stored in the memcached key-value store anyway so a big db query to postgres
would rarely if ever occur - making reorg handling very fast. This postgres table would only be read from 
for serving up old (> 20 blocks old) merkle proofs for individual txs (client queries).


## Headers table
This would be based on the bitcoinx Headers object and how ElectrumX does this to track chain forks and the current 
chain tip etc.

NOTE: header_id would be a foreign key of the MerkleTree table in order to save space (rather than
having the blockhash be part of the merkle tree key for millions of keys... 
that's 32MB for 1 million x 32-byte blockhashes repeated over and over needlessly 
Instead we use a 4 byte integer unique `header_id`).

## DB table design tradeoffs

The Transactions table would be used as follows for a reorg event:

`UPDATE Transactions SET height=<new height> WHERE txid=<txid>`

Each txid lookup is O(1) so not bad.

Would be sub-optimal for:

    txid -> occurrence of a given pubkey_id 

But this is an unlikely / unuseful query in the first place and would still be pretty good anyway
with lookup by txid + a regex matching on the pubkey data.

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

## 1) 'AllocatorProc' - process

The allocator has a p2p protocol client and reads into pre-allocated (shared) memory (see: https://docs.python.org/3/library/multiprocessing.shared_memory.html)
via BufferedProtocol at roughly 600MB/sec... It would receive a continuous stream of "inv" messages and would request
all missing data.

For discussion sake imagine we have a shared memory buffer of exactly 1GB.

Tracks a number of basic primitives:

    1. byte position of buffer (for BufferedProfocol to read into)
    2. count of txs in the full block and how many txs of this total have been read so far.
    3. a cache that tracks the start byte position and length of each transaction in the buffer. This forms a 
    (start_pos, length) tuples (actually two concatenated uint_32s) for each tx. These tuples are then blasted out in 
    batches (via IPC messaging queues) to a number of "WorkerProc" processes that can be spun up to however many 
    CPU cores you want to make use of.
    4. when the buffer is full, it does three important things before resetting the buffer position (see 1) back to 
    zero to recommence streaming of the next (up to) 1GB of data.
    
        1. caches the last, incomplete tx (or message of any kind)
        2. calculates full merkle tree -> persist this as pre-calculated
        3. signal block completion after waiting for all of the WorkerProcs to "Ack" for all of the allocated work + 
        merkle tree calculation to be completed. 

#### Merkle Tree calculation

the merkle tree must be calculated in full before signalling block completion (2 above) and then all or part of it stored to db (there's a trade off to
what depth it should be stored, pre-calculated)... for every level you do not store (i.e. the very bottom layer)
you halve the storage space required... so probably a good trade-off is to at least knock off the last 3 levels
so that a merkle proof calculation requires fetching of only 8 transaction hashes to re-calculate up to the 
pre-calculated level. - the MVP would probably just store the entire merkle tree for simplicity of 
implementation.
- merkle tree calculation is easily parallelisable - just divide the block across an even number of processors
e.g. into quarters and have them each calculate their quarter and update the memcached LRU cache + DB.
- the way it is stored is as a dictionary as follows:
        
see MerkleTree db above
    
#### Block completion signal

The allocator pops the (binary) tuples from it's own "todo list" until all the work for the current block fragment 
is done -> signals a synchronisation event to update the chain tip / start a reorg handling sequence (whichever the case 
may be)
    
    
## 2) 'WorkerProc' processes
    
Can handle a variety of workload types
 
    - parsing bitcoin transactions from a block or the mempool
    - calculating the full merkle tree (would be assigned a fraction of it)
    
    
 (later could sub-specialise them if there was too much overhead parsing a 
variety of bitcoin messages).


## 3) Memcached LRU cache + in-memory merkle tree cache for last 20 blocks

This would serve two main functions.

    1) Enabling parallel calculation of the merkle tree (as a shared key-value store between processes)
    2) Faster response times for client queries to the service and reduced read loads on the db
    
#### Facilitating parallel calculation of merkle tree?

The WorkerProcs are designed to process whatever transactions they are allocated as fast as possible (probably in 
batches of 100 contiguous transactions or so). But when all txs in a block are parsed and they need to 'switch modes'
to merkle proof calculation, they will need rapid access to all of transactions for their designated partition (which
would have been parsed by a different process). The memcached in-memory cache (not LRU for this bit) is the place they 
will first go looking for all of these transaction hashes that they need as well as the place they will store the result

The in-memory cache would be set to keep the last 20 blocks worth of merkle trees (as an arbitrary limit on memory use).
But also the fact the most bang-for-buck performance gains for client queries would be for wallets that need to fetch
the most recent merkle proofs rather than old ones they already have and never need again.

The merkle trees for each block are written to the database in big chunks (i.e. when each WorkerProc completes the 
entire partition - e.g. one quarter of the merkle tree if there are 4 x WorkerProcs)

NOTE: The other advantage of memchached cache is that on a reorg, the WorkerProcs would always do a single "getmany"
query for their newly allocated work to see if the transaction has already been parsed in a prior block (or from 
incoming mempool transactions). This would reduce the overhead of processing reorgs as only the new 
transactions (if any) would need to be actually parsed.

#### Faster response times for queries

Fairly self explanatory - LRU cache for recent requests because often times people using their ESV wallet / daemon will
shut it down and load it backup several times in the ensuing 10-30 minutes... 


## 4) Server

The 3rd process type not including a memcached instance would be for responding to client requests. Would be based on
kyuupichan's aiorpcx JSON-RPC protocol to work with ElectrumSV.

It would first attempt to hit the cache and fallback to a db query. One of these Servers should be enough but more could
easily be added with a load balancer in-between. 

NOTE: clients requesting or subscribing would only be notified of the new chain tip / reorg / merkle proofs and new
headers when the full block is processed along with merkle tree and is persisted to DB. This ensures consistency of 
state.

Mempool transactions OTOH could be continuously streamed through to the cache and db (in batches) and available to client
queries immediately. But the height would need to be updated when they are confirmed in a block.
If a mempool event never came for a tx that is now in a block (e.g. the sender had a contract with miners for cheap tx
fees)... then there'd just be a brief delay until the full block is processed before the pub-sub notification would
go out.

## Miscellaneous

How will block headers be stored? It would use the Headers object from kyuupichan's bitcoinx to determine the chain tip
and if a reorg has occured etc. 

Writes would be in batches for IBD and then switch to a single write-through (cache + db) as soon as the full block has
been processed and the signal / event has gone out from the "Allocator)... it will also update the chain tip etc.


## Final notes on the upper limits of scaling

#### Multiple Allocators 

In theory there could be multiple "AllocatorProcs" to speed up the IDB and they would basically "own" their WorkerProcs
such that you'd have a 1:2 ratio of Allocators to Worker Procs. I think this would be overkill 
at present. I imagine that actually implementing a basic MVP without this would be challenging 
enough.

Furthermore, a faster IBD time - although it sounds great for advertising - actually doesn't have any effect on the 
responsiveness for clients for mempool events and new blocks once the full sync is done. 
And it is this **responsiveness to mempool events and the most recent, new block that determine the final user experience
more than anything else!** And this is why all resources should be dedicated to the processing of **a single block** 
(the most recent block is all that matters) or mempool events as fast as possible.
    
#### Cythonizing the Parsers

By sticking with basic primitives such as integers and binary as much as possible, we should be aiming at making simple
functions that are easily "cythonized" to achieve C-like performance for the parsers. 

The other components such as uvloop and asyncpg are also implemented in cython to achieve their notable performance 
improvements.

NOTE: Should make use of the python array.array type for handling binary / C types as a built-in feature of python for this
very purpose.
