import logging
from typing import Sequence, Tuple, List

from bitcoinx import Headers

import database
from logs import logs


class Storage:
    def __init__(
        self, headers: Headers, block_headers: Headers, pg_db=None, redis=None,
    ):
        logging.getLogger("peewee").setLevel(logging.WARNING)
        self.logger = logs.get_logger("storage")
        self.headers: Headers = headers
        self.block_headers: Headers = block_headers
        self.pg_database = database.db
        self._txs = database.Transaction
        self.redis = redis  # NotImplemented

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

    def get_many_txs_mvs(
        self, tx_hashes: Sequence
    ) -> List[Tuple[memoryview, int, memoryview]]:

        query = self.get_many_txs(tx_hashes)
        results_memory_views = []
        for row in query:
            results_memory_views.append((row.tx_hash, row.height, row.rawtx))
        return results_memory_views

    def get_many_txs_data(self, tx_hashes: Sequence) -> List[Tuple[bytes, int, bytes]]:

        query = self.get_many_txs(tx_hashes)
        results_data = []
        for row in query:
            results_data.append(
                (row.tx_hash.tobytes(), row.height, row.rawtx.tobytes())
            )
        return results_data
