export PYTHONPATH=/home/conduitdb/conduit-db
export LMDB_DATABASE_PATH=/mnt/data/lmdb_data
export RAW_BLOCKS_DIR=/mnt/seagate/raw_blocks

py-spy record --subprocesses -o provile.svg --  python3 /home/conduitdb/conduit-db/conduit_raw/conduit_server.py --mainnet --node-host=127.0.0.1:8333
