import array
import logging.handlers
import logging
import multiprocessing
import os
import queue
import struct
import sys
import threading
import time
from datetime import datetime

from typing import Tuple, List, Sequence, Optional, Dict, Set

import zmq
from MySQLdb import _mysql
from bitcoinx import hash_to_hex_str
from confluent_kafka import Consumer

from conduit_lib.conduit_raw_api_client import ConduitRawAPIClient
from conduit_lib.database.mysql.mysql_database import MySQLDatabase, mysql_connect
from conduit_lib.logging_client import setup_tcp_logging

try:
    from conduit_lib._algorithms import calc_mtree_base_level, parse_txs
except ImportError:
    from conduit_lib.algorithms import calc_mtree_base_level, parse_txs


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
        worker_ack_queue_tx_parse_confirmed,
        worker_ack_queue_mined_tx_hashes,
    ):
        super(TxParser, self).__init__()
        self.worker_id = worker_id
        self.worker_ack_queue_tx_parse_confirmed = worker_ack_queue_tx_parse_confirmed
        # self.worker_ack_queue_tx_parse_mempool = worker_ack_queue_tx_parse_mempool
        self.worker_ack_queue_mined_tx_hashes = worker_ack_queue_mined_tx_hashes

        self.confirmed_tx_flush_queue = None
        self.mempool_tx_flush_queue = None
        self.processed_vs_unprocessed_queue = None

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
        # TODO - during initial block download - should NOT rely on a timeout at all
        #  Should just keep on pumping the entire batch of blocks as fast as possible to
        #  Max out CPU.
        #  This ideally requires reliable PUB/SUB to do properly
        self.CONFIRMED_MAX_TX_BATCH_SIZE = 200_000
        self.CONFIRMED_BATCHED_MAX_WAIT_TIME = 0.3
        self.MEMPOOL_MAX_TX_BATCH_SIZE = 2000
        self.MEMPOOL_BATCHED_MAX_WAIT_TIME = 0.1

        self.total_tx_parse_time = 0
        self.last_time = 0

    def run(self):
        # IPC from Controller to TxParser
        context1 = zmq.Context()
        self.mined_tx_socket = context1.socket(zmq.PULL)
        self.mined_tx_socket.connect("tcp://127.0.0.1:55555")

        # PUB-SUB from Controller to worker to kill the worker
        context3 = zmq.Context()
        self.kill_worker_socket = context3.socket(zmq.SUB)
        self.kill_worker_socket.connect("tcp://127.0.0.1:63241")
        self.kill_worker_socket.setsockopt(zmq.SUBSCRIBE, b"stop_signal")

        self.flush_lock = threading.Lock()  # the connection to MySQL is not thread safe
        if sys.platform == 'win32':
            setup_tcp_logging(port=65421)
        self.logger = logging.getLogger(f"tx-parser-{self.worker_id}")
        self.logger.setLevel(logging.DEBUG)
        self.logger.info(f"Starting {self.__class__.__name__}...")

        self.confirmed_tx_flush_queue = queue.Queue()
        self.mempool_tx_flush_queue = queue.Queue()
        self.confirmed_tx_flush_ack_queue = queue.Queue()
        self.mempool_tx_flush_ack_queue = queue.Queue()
        self.processed_vs_unprocessed_queue = queue.Queue()
        try:
            main_thread = threading.Thread(target=self.main_thread, daemon=True)
            main_thread.start()

            self.start_flush_threads()
            self.logger.info(f"{self.__class__.__name__} exiting")
        except Exception as e:
            self.logger.exception(e)
            raise

    def start_flush_threads(self):
        threads = [
            threading.Thread(target=self.mysql_insert_confirmed_tx_rows_thread, daemon=True),
            threading.Thread(target=self.mysql_insert_mempool_tx_rows_thread, daemon=True)
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

    # Todo - A late-arriving mempool tx could theoretically cause duplicate
    #  input / output / pushdata rows for a given tx... This is an extreme edge case reliant on
    #  a timing window of milliseconds due to an (only theoretical) out-of-order arrival of a block
    #  tx before a mempool tx leading to commit of a confirmed tx before the mempool tx.
    #  To check for this would be ridiculously wasteful compared to having the External API merely
    #  de-duplicate as necessary (if it is even deemed as necessary).
    def mysql_flush_ins_outs_and_pushdata_rows(self, in_rows, out_rows, set_pd_rows, mysql_db):
        mysql_db.mysql_bulk_load_output_rows(out_rows)
        mysql_db.mysql_bulk_load_input_rows(in_rows)
        mysql_db.mysql_bulk_load_pushdata_rows(set_pd_rows)

    def mysql_flush_rows(self, tx_rows: Sequence, in_rows: Sequence, out_rows: Sequence,
            set_pd_rows: Sequence, acks: Optional[Sequence], confirmed: bool, mysql_db):
        with self.flush_lock:
            try:
                if confirmed:
                    # for blk_hash, tx_count in acks:
                    #     self.logger.debug(f"pre-flush block hash={hash_to_hex_str(blk_hash)}, "
                    #                       f"with tx_count={tx_count}")
                    mysql_db.mysql_bulk_load_confirmed_tx_rows(tx_rows)
                    self.mysql_flush_ins_outs_and_pushdata_rows(in_rows, out_rows, set_pd_rows,
                        mysql_db)

                    # Ack for all flushed blocks
                    for blk_hash, tx_count in acks:
                        # self.logger.debug(f"updated block hash={hash_to_hex_str(blk_hash)}, "
                        #                   f"with tx_count={tx_count}")
                        self.worker_ack_queue_tx_parse_confirmed.put((self.worker_id, blk_hash,
                            tx_count))

                    # After each block that is fully ACK'd -> atomically..
                    # 1) invalidate mempool rows
                    # 2) update api tip height
                    # This is done via a global cache of mined txshashes to join with mempool db table

                    self.reset_batched_rows_confirmed()

                else:
                    mysql_db.mysql_bulk_load_mempool_tx_rows(tx_rows)
                    self.mysql_flush_ins_outs_and_pushdata_rows(in_rows, out_rows, set_pd_rows,
                        mysql_db)
                    # Todo - drain - self.mempool_batched_mempool_tx_acks when done and send to
                    #  controller
                    # for tx_count in acks:
                    # self.worker_ack_queue_tx_parse_mempool.put((tx_count))

                    self.reset_batched_rows_mempool()

            except _mysql.IntegrityError as e:
                self.logger.exception(f"IntegrityError: {e}")
                raise

    def mysql_insert_confirmed_tx_rows_thread(self):
        mysql_db: MySQLDatabase = mysql_connect(worker_id=self.worker_id)
        try:
            while True:
                try:
                    # Pre-IBD do large batched flushes
                    confirmed_rows = self.confirmed_tx_flush_queue.get(
                        timeout=self.CONFIRMED_BATCHED_MAX_WAIT_TIME)
                    if not confirmed_rows:  # poison pill
                        break
                    block_acks = self.confirmed_tx_flush_ack_queue.get()
                    self.extend_batched_rows_confirmed(confirmed_rows)
                    self.append_acks_confirmed_txs(block_acks)

                    if len(self.confirmed_batched_tx_rows) > self.CONFIRMED_MAX_TX_BATCH_SIZE:
                        # self.logger.debug("hit max tx batch size...")
                        self.mysql_flush_rows(
                            self.confirmed_batched_tx_rows,
                            self.confirmed_batched_in_rows,
                            self.confirmed_batched_out_rows,
                            self.confirmed_batched_set_pd_rows,
                            self.confirmed_batched_block_acks,
                            confirmed=True,
                            mysql_db=mysql_db
                        )

                # Post-IBD
                except queue.Empty:
                    # self.logger.debug("timed out...")
                    if len(self.confirmed_batched_tx_rows) != 0:
                        self.mysql_flush_rows(
                            self.confirmed_batched_tx_rows,
                            self.confirmed_batched_in_rows,
                            self.confirmed_batched_out_rows,
                            self.confirmed_batched_set_pd_rows,
                            self.confirmed_batched_block_acks,
                            confirmed=True,
                            mysql_db=mysql_db
                        )
                    continue

        except Exception as e:
            self.logger.exception(e)
            raise e
        finally:
            mysql_db.close()

    def mysql_insert_mempool_tx_rows_thread(self):
        try:
            mysql_db: MySQLDatabase = mysql_connect(worker_id=self.worker_id)
            while True:
                try:
                    mempool_rows = self.mempool_tx_flush_queue.get(
                        timeout=self.MEMPOOL_BATCHED_MAX_WAIT_TIME)
                    if not mempool_rows:  # poison pill
                        break
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
                            mysql_db=mysql_db
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
                        mysql_db=mysql_db
                    )
        except Exception as e:
            self.logger.exception(e)
            raise e
        finally:
            mysql_db.close()

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

    def get_processed_vs_unprocessed_tx_offsets(self, raw_block: bytes, tx_offsets: List[int],
            blk_height: int, mysql_db: MySQLDatabase) -> \
            Tuple[List[int], List[int]]:
        """input rows, output rows and pushdata rows must not be inserted again if this has
        already occurred for the mempool transaction"""
        try:
            partition_tx_hashes, partition_tx_hash_rows = self.get_block_partition_tx_hashes(
                raw_block, tx_offsets)
            self.worker_ack_queue_mined_tx_hashes.put({blk_height: partition_tx_hashes})

            offsets_map: Dict[bytes, int] = dict(zip(partition_tx_hashes, tx_offsets))
            unprocessed_tx_hashes = mysql_db.mysql_get_unprocessed_txs(
                partition_tx_hash_rows)

            not_previously_in_mempool_offsets = []
            for row in unprocessed_tx_hashes:
                not_previously_in_mempool_offsets.append(offsets_map.pop(row[0]))

            not_previously_in_mempool_offsets.sort()
            previously_in_mempool_offsets = list(offsets_map.values())
            previously_in_mempool_offsets.sort()
            return not_previously_in_mempool_offsets, previously_in_mempool_offsets
        except Exception:
            self.logger.exception("unexpected exception in get_processed_vs_unprocessed_tx_offsets")
            raise

    def mempool_thread(self):
        group = 'mempool'
        mempool_tx_consumer: Consumer = Consumer({
            'bootstrap.servers': os.environ.get('KAFKA_HOST', "127.0.0.1:26638"),
            'group.id': group,
            'auto.offset.reset': 'earliest'
        })
        mempool_tx_consumer.subscribe(['mempool-txs'])

        def iterate():
            try:
                # Todo: 1 msg/iteration is going to be very inefficient but leaving it for now
                msgs = mempool_tx_consumer.consume(num_messages=1)
                if not msgs:
                    return  # poison pill stop command

                for msg in msgs:
                    message = msg.value()
                    self.logger.debug(f"Got mempool tx: {message}")
                    msg_type, size_tx = struct.unpack_from(f"<II", message)
                    msg_type, size_tx, rawtx = struct.unpack(f"<II{size_tx}s", message)

                    # Todo only does 1 mempool tx at a time at present
                    dt = datetime.utcnow()
                    tx_offsets = array.array("Q", [0])
                    timestamp = dt.strftime("%Y-%m-%d %H:%M:%S")
                    result = parse_txs(rawtx, tx_offsets, timestamp, False)
                    tx_rows, in_rows, out_rows, set_pd_rows = result

                    # todo - batch up mempool txs before feeding them into the queue.
                    num_mempool_txs_processed = 1
                    self.mempool_tx_flush_queue.put((tx_rows, in_rows, out_rows, set_pd_rows))
                    self.mempool_tx_flush_ack_queue.put(num_mempool_txs_processed)
            except Exception as e:
                self.logger.exception(e)
                raise

        while True:
            if iterate() == "stop":
                break

    def mined_blocks_thread(self):

        def iterate(self):
            packed_msg = self.mined_tx_socket.recv()
            if not packed_msg:
                return  # poison pill stop command

            # self.logger.debug(f"len(packed_msg)={len(packed_msg)}")

            msg_type, len_arr = struct.unpack_from("<II", packed_msg)  # get size_array
            msg_type, len_arr, blk_hash, blk_height, first_tx_pos_batch, part_end_offset, \
                packed_array = struct.unpack(f"<II32sIIQ{len_arr}s", packed_msg)

            if hash_to_hex_str(blk_hash) == \
                    "00000000000005dd95107801c8429403e3690435dbc3a919b21ba03387405470":
                self.logger.debug(f"got from queue: blk_height={blk_height}; blk_hash="
                                  f"{hash_to_hex_str(blk_hash)}; size_array={len_arr}")

            tx_offsets_partition = array.array("Q", packed_array)

            # self.logger.debug(f"self.lmdb_grpc_client.get_block_num() where blk_hash={blk_hash}")
            blk_num = lmdb_grpc_client.get_block_num(blk_hash)
            # self.logger.debug(f"blk_num={blk_num}; blk_height={blk_height}")

            # Todo - This is wasteful in that it is pulling the * entire * raw block when it will
            #  only need the relevant partition of the block
            raw_block = lmdb_grpc_client.get_block(blk_num)
            unprocessed_tx_offsets, processed_tx_offsets = \
                self.get_processed_vs_unprocessed_tx_offsets(raw_block, tx_offsets_partition,
                blk_height, mysql_db)


            t0 = time.time()


            # These txs have no entries yet for inputs, pushdata or output tables
            rows_not_previously_in_mempool: Tuple[List, Set, Set, Set] = \
                parse_txs(raw_block, unprocessed_tx_offsets, blk_height, True,
                first_tx_pos_batch)
            tx_rows, in_rows, out_rows, set_pd_rows = rows_not_previously_in_mempool

            # These txs already have entries for inputs, pushdata or output tables
            rows_previously_in_mempool = parse_txs(raw_block, processed_tx_offsets, blk_height, True,
                first_tx_pos_batch)
            tx_rows_now_confirmed, _, _, _ = rows_previously_in_mempool
            tx_rows.extend(tx_rows_now_confirmed)

            t1 = time.time() - t0
            self.total_tx_parse_time += t1
            if self.total_tx_parse_time - self.last_time > 1:  # show every 1 cumulative sec
                self.last_time = self.total_tx_parse_time
                self.logger.debug(f"total time for tx parsing algorithm: "
                                  f"{self.total_tx_parse_time} seconds")

            num_txs = len(tx_offsets_partition)
            self.confirmed_tx_flush_queue.put((tx_rows, in_rows, out_rows, set_pd_rows))
            # self.logger.debug(f"putting to ack queue... blk_hash={blk_hash}, num_txs={num_txs}")
            self.confirmed_tx_flush_ack_queue.put((blk_hash, num_txs))

        try:
            mysql_db: MySQLDatabase = mysql_connect(worker_id=self.worker_id)
            lmdb_grpc_client = ConduitRawAPIClient()
            while True:
                if iterate(self) == "stop":
                    break

        except KeyboardInterrupt:
            raise
        except Exception as e:
            self.logger.exception(e)

    def main_thread(self):
        try:
            threads = [
                threading.Thread(target=self.mempool_thread, daemon=True),
                threading.Thread(target=self.mined_blocks_thread, daemon=True)
            ]
            for t in threads:
                t.start()

            while True:
                # For some reason I am unable to catch a KeyboardInterrupt or SIGINT here so
                # need to rely on an overt "stop_signal" from the Controller for graceful shutdown
                message = self.kill_worker_socket.recv()
                if message == b"stop_signal":
                    break
                time.sleep(0.2)
        finally:
            self.logger.info(f"Process Stopped")
            sys.exit(0)
