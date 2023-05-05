"""LMDB bench - of bulk binary inserts and gets
- In general writes faster than 300MB/sec in on an 860 Samsung EVO 500GB SSD
- Worst case write perf was 54MB/sec with small 2kb entries (random keys).
- Reads will never be a bottleneck - very fast and supports many concurrent readers
- Requires 64 bit python for a db > 1GB
- overall size of db can be worked out by (leaf_pages + overflow_pages) * 4096
- overflow pages happen when inserting items larger than the page size (minus header
size). So it gets wasteful of disc space if you're down in the 2kb - 16kb
range of entries (the 2kb and 4kb tests used 300MB for 200MB of data)
"""
import random
import lmdb
import math
import struct
import time

env = lmdb.open("test_lmdb")
env.set_mapsize(1024 * 1024 * 1024)  # 1GB
with env.begin(write=True) as txn:
    db = env.open_db()
    txn.drop(db)

val_size = 2  # MB
val_size_bytes = math.floor(1024 * 1024 * val_size)
val = b"x" * val_size_bytes
niters = 100
total = val_size_bytes * niters

t0 = time.time()
with env.begin(write=True) as txn:
    keys = []
    for i in range(0, niters):
        randint = random.randint(0, 4_000_000)
        keys.append(randint)
        txn.put(struct.pack("<i", randint), val)

        # keys.append(i)
        # txn.put(struct.pack('<i', i), val)
t1 = time.time() - t0
print(f"time for {niters} inserts x {val_size} MB each for tot={total} bytes: {t1} " f"seconds")

t0 = time.time()
with env.begin(write=True, buffers=True) as txn:
    results = []
    for i in keys:
        result = txn.get(struct.pack("<i", i))
        results.append(len(result))
t1 = time.time() - t0
print(f"time for {niters} gets x {val_size} MB each for tot={total} bytes: {t1} " f"seconds")
print(env.info())
print(env.stat())
print(4096 * (env.stat()["leaf_pages"] + env.stat()["overflow_pages"]))
"""
sequential keys
---------------
# 200 MB
time for 100 inserts x 2 MB each for tot=209715200 bytes: 0.6008994579315186 seconds
time for 100 gets x 2 MB each for tot=209715200 bytes: 0.0887613296508789 seconds
{'map_addr': 0, 'map_size': 1073741824, 'last_pgno': 153708, 'last_txnid': 12, 'max_readers': 126, 'num_readers': 1}
{'psize': 4096, 'depth': 1, 'branch_pages': 0, 'leaf_pages': 1, 'overflow_pages': 51300, 'entries': 100}
210128896


random keys
-----------
# 4 GB
time for 1 inserts x 4000 MB each for tot=4194304000 bytes: 12.15371584892273 seconds
{'map_addr': 0, 'map_size': 10737418240, 'last_pgno': 1843318, 'last_txnid': 17, 'max_readers': 126, 'num_readers': 1}
{'psize': 4096, 'depth': 1, 'branch_pages': 0, 'leaf_pages': 1, 'overflow_pages': 1024001, 'entries': 1}
4194312192

# 2 GB
time for 1 inserts x 2000 MB each for tot=2097152000 bytes: 5.596102714538574 seconds
time for 1 gets x 2000 MB each for tot=2097152000 bytes: 0.591930627822876 seconds
{'map_addr': 0, 'map_size': 4294967296, 'last_pgno': 819317, 'last_txnid': 6, 'max_readers': 126, 'num_readers': 1}
{'psize': 4096, 'depth': 1, 'branch_pages': 0, 'leaf_pages': 1, 'overflow_pages': 512001, 'entries': 1}
2097160192

# 200 MB
time for 1 inserts x 200 MB each for tot=209715200 bytes: 0.6143567562103271 seconds
time for 1 gets x 200 MB each for tot=209715200 bytes: 0.05585074424743652 seconds
{'map_addr': 0, 'map_size': 1073741824, 'last_pgno': 231516, 'last_txnid': 100, 'max_readers': 126, 'num_readers': 1}
{'psize': 4096, 'depth': 1, 'branch_pages': 0, 'leaf_pages': 1, 'overflow_pages': 51201, 'entries': 1}
209723392

# 200 MB
time for 10 inserts x 20 MB each for tot=209715200 bytes: 0.5929222106933594 seconds
time for 10 gets x 20 MB each for tot=209715200 bytes: 0.10072755813598633 seconds
{'map_addr': 0, 'map_size': 1073741824, 'last_pgno': 231516, 'last_txnid': 102, 'max_readers': 126, 'num_readers': 1}
{'psize': 4096, 'depth': 1, 'branch_pages': 0, 'leaf_pages': 1, 'overflow_pages': 51210, 'entries': 10}
209760256

time for 100 inserts x 2 MB each for tot=209715200 bytes: 0.5764734745025635 seconds
time for 100 gets x 2 MB each for tot=209715200 bytes: 0.0768120288848877 seconds
{'map_addr': 0, 'map_size': 1073741824, 'last_pgno': 153708, 'last_txnid': 14, 'max_readers': 126, 'num_readers': 1}
{'psize': 4096, 'depth': 1, 'branch_pages': 0, 'leaf_pages': 1, 'overflow_pages': 51300, 'entries': 100}
210128896

# 200 MB
time for 250000 inserts x 0.0008 MB each for tot=209500000 bytes: 2.2235724925994873 seconds
time for 250000 gets x 0.0008 MB each for tot=209500000 bytes: 0.38547706604003906 seconds
{'map_addr': 0, 'map_size': 1073741824, 'last_pgno': 231516, 'last_txnid': 95, 'max_readers': 126, 'num_readers': 1}
{'psize': 4096, 'depth': 4, 'branch_pages': 484, 'leaf_pages': 83066, 'overflow_pages': 0, 'entries': 242423}
340238336

# 200 MB
time for 500000 inserts x 0.0004 MB each for tot=209500000 bytes: 2.5866353511810303 seconds
time for 500000 gets x 0.0004 MB each for tot=209500000 bytes: 0.6772012710571289 seconds
{'map_addr': 0, 'map_size': 1073741824, 'last_pgno': 231516, 'last_txnid': 91, 'max_readers': 126, 'num_readers': 1}
{'psize': 4096, 'depth': 4, 'branch_pages': 436, 'leaf_pages': 77603, 'overflow_pages': 0, 'entries': 470274}
317861888

# 200 MB
time for 1000000 inserts x 0.0002 MB each for tot=209000000 bytes: 3.7176215648651123 seconds
time for 1000000 gets x 0.0002 MB each for tot=209000000 bytes: 1.214759349822998 seconds
{'map_addr': 0, 'map_size': 1073741824, 'last_pgno': 231516, 'last_txnid': 93, 'max_readers': 126, 'num_readers': 1}
{'psize': 4096, 'depth': 4, 'branch_pages': 321, 'leaf_pages': 71970, 'overflow_pages': 0, 'entries': 884813}
294789120
"""
