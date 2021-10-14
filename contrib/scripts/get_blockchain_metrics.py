import logging
import sys
import bitcoinx

from conduit_lib.database.lmdb.lmdb_database import LMDB_Database
from conduit_lib.networks import NetworkConfig
from conduit_lib.store import setup_headers_store

if len(sys.argv) != 5:
    print(f"{sys.argv[0]} <headers.mmap> <network> <node_host> <lmdb_dir>")
    sys.exit(1)

headers_path = sys.argv[1]  # e.g. absolute/path/to/conduit_raw/headers.mmap
network = sys.argv[2]  # e.g. 'mainnet' / 'testnet' / 'regtest'
node_host = sys.argv[3]  # e.g. 127.0.0.1:18444
lmdb_dir = sys.argv[4]  # e.g. /mnt/data/lmdb_data


net_config = NetworkConfig(network, node_host)
headers = setup_headers_store(net_config, headers_path)


# Get ordered list of block hashes by height
ordered_block_hashes = []

chain: bitcoinx.Chain = headers.longest_chain()
for height in range(1, chain.tip.height - 1):
    header: bitcoinx.Header = headers.header_at_height(chain, height)
    print(f"Height: {height}; Hash: {bitcoinx.hash_to_hex_str(header.hash)}")
    ordered_block_hashes.append((height, header.hash))

logger = logging.getLogger("debug-lmdb")
logging.basicConfig(level=logging.DEBUG)
lmdb_db = LMDB_Database(storage_path=lmdb_dir)
cumulative_blockchain_size = 0
cumulative_tx_count = 0
lines = []

headers = ['Height', 'Cumulative Tx Count', 'Blockchain Total Size (MB)', 'Tx Count', 'Block Size (MB)', 'Block Hash']
with open('metrics.csv', 'w') as f:
    f.write(",".join(headers) + "\n")
    f.flush()

    for height, block_hash in ordered_block_hashes:
        try:
            blk_num = lmdb_db.get_block_num(block_hash)
            # this is wastefull... should store tx_count in blockchain metadata db
            tx_offsets = lmdb_db.get_tx_offsets(block_hash)
            tx_count = len(tx_offsets)
            cumulative_tx_count += tx_count

            blk_size = lmdb_db.get_block_metadata(block_hash) / 1024**2  # MB
            cumulative_blockchain_size += blk_size
            print(f'Height: {height}, '
                  f'Cumulative Tx Count: {cumulative_tx_count},'
                  f'Blockchain Total Size: {cumulative_blockchain_size} MB, '
                  f'Tx Count: {tx_count}, '
                  f'Block Size: {blk_size} MiB, '
                  f'Block Hash: {bitcoinx.hash_to_hex_str(block_hash)}')
            line = [height, cumulative_tx_count, cumulative_blockchain_size, tx_count, blk_size, bitcoinx.hash_to_hex_str(block_hash)]
            f.write(",".join([str(x) for x in line]) + "\n")
            f.flush()

        except Exception as e:
            print(e)

