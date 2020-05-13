from typing import Sequence, Tuple

from bitcoinx import Headers

import database
from logs import logs


class Storage:
    def __init__(
        self, headers: Headers, block_headers: Headers, pg_db=None, memcached=None,
    ):
        self.logger = logs.get_logger("storage")
        self.headers: Headers = headers
        self.block_headers: Headers = block_headers
        self.pg_database = database.db
        self._txs = database.Transaction

        # test inserting single tx
        self.insert_tx(tx_hash=b"1", height=1, rawtx=b"deadbeef")
        self.insert_tx(tx_hash=b"2", height=2, rawtx=b"deadbeef")
        self.insert_tx(tx_hash=b"3", height=3, rawtx=b"deadbeef")

        query = self.get_many_txs(tx_hashes=[b"1", b"2", b"3"])
        for row in query:
            print(row.tx_hash, row.height, row.rawtx)

        self.memcached = memcached  # NotImplemented

        # test inserting many txs
        txs = [(b"4", 4, b"deadbeef"), (b"5", 5, b"deadbeef"), (b"6", 6, b"deadbeef")]
        self.insert_many_txs(txs)

        query = self.get_many_txs(tx_hashes=[str(i).encode('utf-8') for i in range(1,
            7)])
        for row in query:
            print(row.tx_hash, row.height, row.rawtx)

    # API
    def insert_tx(self, tx_hash: bytes, height: int, rawtx: bytes):
        # with self.database.atomic():
        self._txs.insert(tx_hash=tx_hash, height=height, rawtx=rawtx).execute()

    def insert_many_txs(self, txs: Sequence[Tuple[bytes, int, bytes]]):
        try:
            with self.pg_database.atomic():
                self._txs.insert_many(
                    txs, fields=[self._txs.tx_hash, self._txs.height, self._txs.rawtx]
                ).execute()
        except Exception as e:
            self.logger.exception(e)

    def get_many_txs(self, tx_hashes: Sequence):
        query = self._txs.select().where(self._txs.tx_hash << tx_hashes).execute()
        return query
