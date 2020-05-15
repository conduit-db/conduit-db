## Re-design (Brain dump)

### Change BufferedProtocol framer design 
    - use a 1GB pre-allocated buffer + 50MB extra for overflows. 
    - The HWM (high-watermark) level at exactly the 1GB mark.
    - only when it hits the HWM level does it "pause_reading()" and wait for the msg queue
    to be emptied completely + 'go' signal to start again. 
    - then create a new 1GB buffer including:
        
        self.buffer[self._last_msg_pos: ] + bytearray(BUFFER_SIZE - len(self._last_msg_pos: ]))

    - for each message the header is deserialized + memory view captured and 
    last byte position recorded.
    - this is stored in an incoming_msg_queue as tuples i.e. a queue of:
     
     (<deserialized_header>, <memory_view_payload>) tuples -> pumped to workers

    if self.pos  - self._last_msg_end_pos is >= header_len + payload_len:
        # we have the next full msg
        cur_msg_end_pos = self._last_msg_pos + header_len + payload_len
        self.add_msg_to_queue(self._cur_header, self.buffer_view[self._last_msg_end_pos: cur_msg_end_pos])


This decouples the **reading** of messages and (potentially many tiny little) blocks from
the **parsing** and we end up with a simple producer-consumer arrangement whereby messages
can be taken from the queue (which only contains memory views) and get fed into any number
of worker processes that can parse them in parallel.

### The special case for block parsing...

**Before** sending out a getblocks request we can lookup (in headers cache) all of the next 500
header hashes and put them into a **blocks_batch_set**

Now:

    - send 'getblocks' request and receive the inv with 500 hashes which is...
    - automatically handled with corresponding 500 x 'getdata' msgs (even if there are extra newly mined blocks in there) 
    (sidenote: later can check the block_headers cache to see if we already have each block before exerting any effort)

Then:
 
    - when 'block' msg is read into the 'incoming_msg_queue' it can be...
    - parsed (either by 1 core (if < 1MB - i.e. 1 partition only) or by multiple cores (if >1MB)) + merkle tree calc etc.
    - when complete, the hash is removed from set of 500 hashes and the len(blocks_batch_set) is checked. 
    - if len == 0 (for last worker) trigger an event that will cause... 
    - connecting each block header in order (for all 500 in the batch) for the block_headers.mmp and update the new chain tip.
    - When fully sync'd this process does not change much - the batch sizes just drop to a size of 1.

NOTE: for the simplest implementation each 'Worker' process should be relatively smart in that they
can just pull directly from the one, unified queue that contains a variety of msg types.

NOTE2: when blocks get really big on STN it would be ill-advised to keep a batch size of 500 so this can be
reduced without really changing any part of the design (just add in a stop hash to give the desired
batch sizes). Mainnet should be fine to do batches of 500 the whole way on a 16GB ram system.

NOTE3: this rapid-fire parallel parsing of 500 block batches can only be done with a 1 block: 1 cpu ratio.
This is because to find the "binding location" for a parser in the middle of a block requires a 100% reliable
lookup of the prev_out hash on the first input... this 100% garauntee is lost when there are preceeding blocks
missing from our dataset... But it doesn't matter. We can have our cake and eat it too so to speak...

- We get fast IBD times and for batch sizes of 1 after initial sync can switch gears to parallel block parsing via 
the partitioning algorithm (where the 'block_part' msg is not always a 'first part' but also has the more
tricky-to-deal-with middle and end chunks...)
- basically if we can find the prev_hash (of the current block this time - not the tx input) in block_headers then we can partition safely if not
then the entire block needs to go in a single chunk to one worker.

For small blocks the "first partition" is just treated the exact same as a whole block. The Worker
doesn't need to know any different.

The merkle tree calculation requires coordination between workers to count how many txs they 
parsed along with the total number of txs -> then can divide up the merkle tree into a factor of 2.
for the next round... So this gets done at a higher level (The 'manager' perhaps)

Therefore merkle tree calculations should be a separate 'type' of task that could go into
 back to the same worker - they use 
the redis cache to get all the tx hashes they need for their part. 
(or maybe shared memory dict??)

OTOH it could be a single 'tx' message

So you get 3 main msg types pulled from a single internal queue

    - 'tx'
    - 'block_part' (1st part may == whole block)
    - 'merkle_tree_part (1st part may == whole tree if small)
    
With this API to the 'smart' workers... msgs can just be pumped at e.g. 32 workers in parallel.
with the only co-ordination steps involving:
 
    1. dividing the merkle tree and generating 'merkle_tree_part' msgs for the workers
    2. connecting block_headers in batches of up to 500  at the end of each batch
    3. send 'go' signal to reader when done using shared block of memory.


The ideal situation would be to have a BlockchainReader class that is **totally decoupled** from 
a BlockchainParser class... the external API to the BlockchainReader would only be the
'incoming_msg_queue' and a "go" signal. The BlockchainReader would automatically 

    1. pause_reading when above HWM
    2. resume_reading when it gets the 'go' signal
    - which doesn't quite happen when queue hits empty (i.e. await incoming_msg_queue.join() )
    - also need to wait until all workers are done utilising the shared memory... so needs an
    explicit trigger. 
    
The BlockchainParser can:

    1. consume from this queue completely independently as fast as it can
    

BlockchainWriter would just be a fancy wrapper for the send_request function in its current form

Then conduit has these three main classes to get its work done.
- Reader/Writer
- Parser x how many cores do you have?  (all expected bottlenecks are here)
    - parse pubkeys out of txs
    - calculate merkle tree
    - save to db
    - notify external API + send 'go' signal to Reader
- External API server (maybe aiorpcx-based) for electrumsv (and maybe other wallets/apps) to 
interact with (open-sourced)
