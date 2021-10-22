export PYTHONPATH=/home/conduitdb/conduit-db
export CONDUIT_RAW_API_HOST=127.0.0.1:50000
export MYSQL_PORT=3306
export MYSQL_USER=conduitadmin

py-spy record --subprocesses -o provile.svg --  python3 /home/conduitdb/conduit-db/conduit_index/conduit_server.py --mainnet --node-host=127.0.0.1:8333 --mysql-host=127.0.0.1:3306 --reset
