import logging.handlers
import logging
import multiprocessing
import queue
import threading
from datetime import datetime
from multiprocessing import shared_memory
from typing import Tuple, List, Sequence, Optional

from MySQLdb import _mysql

from ..constants import MsgType
from ..database.mysql.mysql_database import MySQLDatabase, mysql_connect
from ..logging_client import setup_tcp_logging
from .algorithms import calc_mtree_base_level, parse_txs


class TxParser(multiprocessing.Process):
    """
    in: blk_hash, blk_height, blk_start_pos, blk_end_pos, tx_positions_div
    out: N/A - directly updates posgres database
    ack_confirmed: blk_hash, count_txs_done
    ack_mempool: tx_counts
    """

    def __init__(
        self,
        worker_id,
        shm_name,
        worker_in_queue_tx_parse,
        worker_ack_queue_tx_parse_confirmed,
        initial_block_download_event_mp,
        worker_ack_queue_mined_tx_hashes,
    ):
        super(TxParser, self).__init__()
        self.worker_id = worker_id
        self.shm = shared_memory.SharedMemory(shm_name, create=False)
        self.worker_in_queue_tx_parse = worker_in_queue_tx_parse
        self.worker_ack_queue_tx_parse_confirmed = worker_ack_queue_tx_parse_confirmed
        # self.worker_ack_queue_tx_parse_mempool = worker_ack_queue_tx_parse_mempool
        self.initial_block_download_event_mp = initial_block_download_event_mp
        self.worker_ack_queue_mined_tx_hashes = worker_ack_queue_mined_tx_hashes

        self.confirmed_tx_flush_queue = None
        self.mempool_tx_flush_queue = None
        self.processed_vs_unprocessed_queue = None

        # self.mysql_db = None
        self.mysql = None
        self.loop = None

        # batched confirmed rows
        self.confirmed_batched_tx_rows = []
        self.confirmed_batched_in_rows = []
        self.confirmed_batched_out_rows = []
        self.confirmed_batched_set_pd_rows = []
        self.confirmed_batched_block_acks = []

        # batched rows
        self.mempool_batched_tx_rows = []
        self.mempool_batched_in_rows = []
        self.mempool_batched_out_rows = []
        self.mempool_batched_set_pd_rows = []
        self.mempool_batched_mempool_tx_acks = []

        # accumulate x seconds worth of txs or MAX_TX_BATCH_SIZE (whichever comes first)
        self.CONFIRMED_MAX_TX_BATCH_SIZE = 50000
        self.CONFIRMED_BATCHED_MAX_WAIT_TIME = 0.3
        self.MEMPOOL_MAX_TX_BATCH_SIZE = 2000
        self.MEMPOOL_BATCHED_MAX_WAIT_TIME = 0.1

    def run(self):
        self.flush_lock = threading.Lock()  # the connection to MySQL is not thread safe
        setup_tcp_logging()
        self.logger = logging.getLogger(f"tx-parser-{self.worker_id}")
        self.logger.setLevel(logging.DEBUG)
        self.logger.debug(f"starting {self.__class__.__name__}...")

        self.confirmed_tx_flush_queue = queue.Queue()
        self.mempool_tx_flush_queue = queue.Queue()
        self.confirmed_tx_flush_ack_queue = queue.Queue()
        self.mempool_tx_flush_ack_queue = queue.Queue()
        self.processed_vs_unprocessed_queue = queue.Queue()
        try:
            main_thread = threading.Thread(target=self.main_thread)
            main_thread.start()

            self.start_flush_threads()
            self.logger.info(f"{self.__class__.__name__} exiting")
        except Exception as e:
            self.logger.exception(e)
            raise

    def start_flush_threads(self):
        threads = [
            threading.Thread(target=self.mysql_insert_confirmed_tx_rows_thread),
            threading.Thread(target=self.mysql_insert_mempool_tx_rows_thread)
        ]
        for t in threads:
            t.setDaemon(True)
            t.start()
        for t in threads:
            t.join()

    # ----- CONFIRMED TXS ----- #

    def reset_batched_rows_confirmed(self):
        self.confirmed_batched_tx_rows = []
        self.confirmed_batched_in_rows = []
        self.confirmed_batched_out_rows = []
        self.confirmed_batched_set_pd_rows = []
        self.confirmed_batched_block_acks = []

    def extend_batched_rows_confirmed(self, blk_rows):
        tx_rows, in_rows, out_rows, set_pd_rows = blk_rows
        self.confirmed_batched_tx_rows.extend(tx_rows)
        self.confirmed_batched_in_rows.extend(in_rows)
        self.confirmed_batched_out_rows.extend(out_rows)
        self.confirmed_batched_set_pd_rows.extend(set_pd_rows)

    def append_acks_confirmed_txs(self, blk_acks):
        self.confirmed_batched_block_acks.append(blk_acks)

    # ----- MEMPOOL TXS ----- #

    def reset_batched_rows_mempool(self):
        self.mempool_batched_tx_rows = []
        self.mempool_batched_in_rows = []
        self.mempool_batched_out_rows = []
        self.mempool_batched_set_pd_rows = []
        self.confirmed_batched_block_acks = []

    def extend_batched_rows_mempool(self, mempool_rows):
        tx_rows, in_rows, out_rows, set_pd_rows = mempool_rows
        self.mempool_batched_tx_rows.extend(tx_rows)
        self.mempool_batched_in_rows.extend(in_rows)
        self.mempool_batched_out_rows.extend(out_rows)
        self.mempool_batched_set_pd_rows.extend(set_pd_rows)

    def append_acks_mempool_txs(self, mempool_tx_acks):
        self.mempool_batched_mempool_tx_acks.append(mempool_tx_acks)

    # ------------------------------------------------- #

    def mysql_flush_ins_outs_and_pushdata_rows(self, in_rows, out_rows, set_pd_rows):
        self.mysql_db.mysql_bulk_load_output_rows(out_rows)
        self.mysql_db.mysql_bulk_load_input_rows(in_rows)
        self.mysql_db.mysql_bulk_load_pushdata_rows(set_pd_rows)

    def mysql_flush_rows(self, tx_rows: Sequence, in_rows: Sequence, out_rows: Sequence,
            set_pd_rows: Sequence, acks: Optional[Sequence], confirmed: bool, ):
        with self.flush_lock:
            try:
                if confirmed:
                    self.mysql_db.mysql_bulk_load_confirmed_tx_rows(tx_rows)
                    self.mysql_flush_ins_outs_and_pushdata_rows(in_rows, out_rows, set_pd_rows)

                    # Ack for all flushed blocks
                    for blk_hash, tx_count in acks:
                        # print(f"updated block hash={blk_hash}, with tx_count={tx_count}")
                        self.worker_ack_queue_tx_parse_confirmed.put((blk_hash, tx_count))

                    # After each block that is fully ACK'd -> atomically..
                    # 1) invalidate mempool rows
                    # 2) update api tip height
                    # This is done via a global cache of mined txshashes to join with mempool db table

                    self.reset_batched_rows_confirmed()

                else:
                    self.mysql_db.mysql_bulk_load_mempool_tx_rows(tx_rows)
                    self.mysql_flush_ins_outs_and_pushdata_rows(in_rows, out_rows, set_pd_rows)
                    # for tx_count in acks:
                    # self.worker_ack_queue_tx_parse_mempool.put((tx_count))

                    self.reset_batched_rows_mempool()

            except _mysql.IntegrityError as e:
                self.logger.exception(f"IntegrityError: {e}")
                raise

    def mysql_insert_confirmed_tx_rows_thread(self):
        self.mysql_db: MySQLDatabase = mysql_connect()
        try:
            while True:
                try:
                    confirmed_rows = self.confirmed_tx_flush_queue.get(
                        timeout=self.CONFIRMED_BATCHED_MAX_WAIT_TIME)
                    block_acks = self.confirmed_tx_flush_ack_queue.get()
                    self.extend_batched_rows_confirmed(confirmed_rows)
                    self.append_acks_confirmed_txs(block_acks)

                except queue.Empty:
                    # print("timed out...")
                    if len(self.confirmed_batched_tx_rows) != 0:
                        self.mysql_flush_rows(
                            self.confirmed_batched_tx_rows,
                            self.confirmed_batched_in_rows,
                            self.confirmed_batched_out_rows,
                            self.confirmed_batched_set_pd_rows,
                            self.confirmed_batched_block_acks,
                            confirmed=True,
                        )
                    continue

                # This is not in the "try block" because I wasn't sure if it would be atomic
                # i.e. what if it times out half-way through committing rows to the db?
                if len(self.confirmed_batched_tx_rows) > self.CONFIRMED_MAX_TX_BATCH_SIZE:
                    # print("hit max tx batch size...")
                    self.mysql_flush_rows(
                        self.confirmed_batched_tx_rows,
                        self.confirmed_batched_in_rows,
                        self.confirmed_batched_out_rows,
                        self.confirmed_batched_set_pd_rows,
                        self.confirmed_batched_block_acks,
                        confirmed=True,
                    )
        except Exception as e:
            self.logger.exception(e)
            raise e
        finally:
            self.mysql_db.close()

    def mysql_insert_mempool_tx_rows_thread(self):
        try:
            self.mysql_db: MySQLDatabase = mysql_connect()
            while True:
                try:
                    mempool_rows = self.mempool_tx_flush_queue.get(
                        timeout=self.MEMPOOL_BATCHED_MAX_WAIT_TIME)
                    mempool_tx_acks = self.mempool_tx_flush_ack_queue.get()
                    self.extend_batched_rows_mempool(mempool_rows)
                    self.append_acks_mempool_txs(mempool_tx_acks)
                except queue.Empty:
                    # self.logger.debug("mempool batch timer triggered")
                    if len(self.mempool_batched_tx_rows) != 0:
                        self.mysql_flush_rows(
                            self.mempool_batched_tx_rows,
                            self.mempool_batched_in_rows,
                            self.mempool_batched_out_rows,
                            self.mempool_batched_set_pd_rows,
                            self.mempool_batched_mempool_tx_acks,
                            confirmed=False,
                        )
                    continue

                # This is not in the "try block" because I wasn't sure if it would be atomic
                # i.e. what if it times out half-way through committing rows to the db?
                if len(self.mempool_batched_tx_rows) > self.MEMPOOL_MAX_TX_BATCH_SIZE - 1:
                    self.logger.debug(
                        f"hit max mempool batch size ({len(self.mempool_batched_tx_rows)})"
                    )
                    self.mysql_flush_rows(
                        self.mempool_batched_tx_rows,
                        self.mempool_batched_in_rows,
                        self.mempool_batched_out_rows,
                        self.mempool_batched_set_pd_rows,
                        self.mempool_batched_mempool_tx_acks,
                        confirmed=False,
                    )

        except Exception as e:
            self.logger.exception(e)
            raise e
        finally:
            self.mysql_db.close()

    def get_block_partition_tx_hashes(self, raw_block, tx_offsets) -> Tuple[List[bytes],
            List[Tuple]]:
        """Returns both a list of tx hashes and list of tuples containing tx hashes (the same
        data) ready for database insertion"""
        partition_tx_hashes = calc_mtree_base_level(0, len(tx_offsets), {}, raw_block,
            tx_offsets)[0]
        tx_hash_rows = []
        for full_tx_hash in partition_tx_hashes:
            tx_hash_rows.append((full_tx_hash.hex(),))
        return partition_tx_hashes, tx_hash_rows

    def get_processed_vs_unprocessed_tx_offsets(self, raw_block, tx_offsets, blk_height):
        """input rows, output rows and pushdata rows must not be inserted again if this has
        already occurred for the mempool transaction"""
        try:
            partition_tx_hashes, partition_tx_hash_rows = self.get_block_partition_tx_hashes(
                raw_block, tx_offsets)
            if self.initial_block_download_event_mp.is_set():
                self.worker_ack_queue_mined_tx_hashes.put({blk_height: partition_tx_hashes})

            offsets_map = dict(zip(partition_tx_hashes, tx_offsets))
            unprocessed_tx_hashes = self.mysql_db.mysql_get_unprocessed_txs(
                partition_tx_hash_rows)
            # self.logger.debug(f'unprocessed_tx_shashes: {unprocessed_tx_shashes}')

            relevant_offsets = []
            for row in unprocessed_tx_hashes:
                relevant_offsets.append(offsets_map.pop(row[0]))
            self.processed_vs_unprocessed_queue.put_nowait((relevant_offsets, offsets_map))
        except Exception:
            self.logger.exception("unexpected exception in get_processed_vs_unprocessed_tx_offsets")
            raise

    def main_thread(self):
        while True:
            try:
                msg_type, item = self.worker_in_queue_tx_parse.get()
                if not item:
                    return  # poison pill stop command

                if msg_type == MsgType.MSG_TX:
                    tx_offsets = item

                    # Todo only does 1 mempool tx at a time at present
                    for tx_start_pos, tx_end_pos in tx_offsets:
                        buffer = bytes(self.shm.buf[tx_start_pos:tx_end_pos])

                    dt = datetime.utcnow()
                    timestamp = dt.strftime("%Y-%m-%d %H:%M:%S")
                    result = parse_txs(buffer, [0], timestamp, False)
                    tx_rows, in_rows, out_rows, set_pd_rows = result

                    # todo - batch up mempool txs before feeding them into the queue.
                    self.mempool_tx_flush_queue.put( (tx_rows, in_rows, out_rows, set_pd_rows) )
                    self.mempool_tx_flush_ack_queue.put( len(tx_offsets) )

                if msg_type == MsgType.MSG_BLOCK:
                    blk_hash, blk_height, blk_start_pos, blk_end_pos, tx_offsets_div, \
                        first_tx_pos_batch = item
                    buffer = bytes(self.shm.buf[blk_start_pos:blk_end_pos])

                    # if in IBD mode, mempool txs are strictly rejected. So there is no point
                    # in sorting out which txs are already in the mempool tx table.
                    # There is also no point performing mempool tx invalidation.
                    if self.initial_block_download_event_mp.is_set():
                        self.get_processed_vs_unprocessed_tx_offsets(buffer, tx_offsets_div,
                                blk_height)
                        item = self.processed_vs_unprocessed_queue.get()
                        unprocessed_tx_offsets, processed_tx_offsets = item
                    else:
                        unprocessed_tx_offsets = tx_offsets_div

                    result = parse_txs(buffer, unprocessed_tx_offsets, blk_height, True,
                        first_tx_pos_batch)
                    tx_rows, in_rows, out_rows, set_pd_rows = result

                    self.confirmed_tx_flush_queue.put( (tx_rows, in_rows, out_rows, set_pd_rows) )
                    self.confirmed_tx_flush_ack_queue.put( (blk_hash, len(tx_offsets_div)) )

                # print(f"parsed rows: len(tx_rows)={len(tx_rows)}, len(in_rows)={len(in_rows)}, "
                #       f"len(out_rows)={len(out_rows)}")
            except Exception as e:
                self.logger.exception(e)
                raise
