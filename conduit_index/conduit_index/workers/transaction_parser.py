import array
import logging.handlers
import logging
import multiprocessing
import os
import queue
import socket
import struct
import sys
import threading
import time
from datetime import datetime

from typing import Tuple, List, Sequence, Optional, Dict, cast

import zmq
from MySQLdb import _mysql
from bitcoinx import hash_to_hex_str
from electrumsv_node import electrumsv_node

from conduit_lib.conduit_raw_api_client import ConduitRawAPIClient
from conduit_lib.database.mysql.mysql_database import MySQLDatabase, mysql_connect
from conduit_lib.logging_client import setup_tcp_logging
from conduit_lib.basic_socket_io import recv_msg

from ..types import BatchedRawBlockSlices, ProcessedBlockAcks, TxHashRows, TxHashes, \
    TxHashToWorkIdMap, TxHashToOffsetMap, BlockSliceOffsets, WorkUnit, WorkPart

try:
    from conduit_lib._algorithms import calc_mtree_base_level, parse_txs
except ImportError:
    from conduit_lib.algorithms import calc_mtree_base_level, parse_txs


def extend_batched_rows(blk_rows: Tuple[List, List, List, List], txs: List, ins: List, outs: List,
        pds: List) -> Tuple[List, List, List, List]:
    tx_rows, in_rows, out_rows, set_pd_rows = blk_rows
    txs.extend(tx_rows)
    ins.extend(in_rows)
    outs.extend(out_rows)
    pds.extend(set_pd_rows)
    return txs, ins, outs, pds


def append_acks(blk_acks, acks: List) -> List:
    acks.append(blk_acks)
    return acks


class TxParser(multiprocessing.Process):
    """
    in: blk_hash, blk_height, blk_start_pos, blk_end_pos, tx_positions_div
    out: N/A - directly updates posgres database
    ack_confirmed: blk_hash, count_txs_done
    ack_mempool: tx_counts
    """

    def __init__(self, worker_id, worker_ack_queue_tx_parse_confirmed,
            worker_ack_queue_mined_tx_hashes, ):
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

        # accumulate x seconds worth of txs or MAX_TX_BATCH_SIZE (whichever comes first)
        # TODO - during initial block download - should NOT rely on a timeout at all
        #  Should just keep on pumping the entire batch of blocks as fast as possible to
        #  Max out CPU.
        #  This ideally requires reliable PUB/SUB to do properly
        self.BLOCKS_MAX_TX_BATCH_LIMIT = 200_000
        self.BLOCK_BATCHING_RATE = 0.3
        self.MEMPOOL_MAX_TX_BATCH_LIMIT = 2000
        self.MEMPOOL_BATCHING_RATE = 0.1

        self.total_unprocessed_tx_sorting_time = 0
        self.last_time = 0
        self.grpc_time = 0
        self.last_grpc_time = 0

    def run(self):
        # PUB-SUB from Controller to worker to kill the worker
        context3 = zmq.Context()
        self.kill_worker_socket = context3.socket(zmq.SUB)
        self.kill_worker_socket.connect("tcp://127.0.0.1:63241")
        self.kill_worker_socket.setsockopt(zmq.SUBSCRIBE, b"stop_signal")

        # Todo - these should all be local to the thread's state only - not global to
        #  avoid race... Need to refactor to class-based thread module. For now it will
        #  be okay because of only having a single thread reading and/or writing these data structs.
        #  locking would be unnecessary if the state was encapsulated in the thread.

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind(('0.0.0.0', 0))
        self.sock_port = self.sock.getsockname()[1]  # get randomly allocated port by OS
        self.sock_callback_ip = os.getenv('CONDUIT_INDEX_HOST', '127.0.0.1')
        self.sock.listen()

        self.raw_blocks_array_cache: Dict[int, bytearray] = {}  # batch_id: array
        self.raw_blocks_array_recv_events: Dict[int, threading.Event] = {}  # batch_id: event
        self.batch_id = 0

        # the connection to MySQL is not thread safe
        # todo - write code in a way that a lock is not needed (more connections is preferable to
        #  using locks at the application layer)
        self.flush_lock = threading.Lock()
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
        threads = [threading.Thread(target=self.mysql_insert_confirmed_tx_rows_thread, daemon=True),
            threading.Thread(target=self.mysql_insert_mempool_tx_rows_thread, daemon=True), ]
        for t in threads:
            t.setDaemon(True)
            t.start()
        for t in threads:
            t.join()

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
                    # for work_item_id, blk_hash, tx_count in acks:
                    #     self.logger.debug(f"pre-flush work_item_id={work_item_id}, "
                    #                       f"block hash={hash_to_hex_str(blk_hash)}, "
                    #                       f"with tx_count={tx_count}")

                    mysql_db.mysql_bulk_load_confirmed_tx_rows(tx_rows)
                    self.mysql_flush_ins_outs_and_pushdata_rows(in_rows, out_rows, set_pd_rows,
                        mysql_db)

                    # Ack for all flushed blocks
                    for work_item_id, blk_hash, tx_count in acks:
                        # self.logger.debug(f"updated work_item_id={work_item_id}, "
                        #                   f"block hash={hash_to_hex_str(blk_hash)}, "
                        #                   f"with tx_count={tx_count}")
                        self.worker_ack_queue_tx_parse_confirmed.put(
                            (self.worker_id, work_item_id, blk_hash, tx_count))

                    # After each block (or batch of blocks) that is fully ACK'd -> atomically..  # 1) invalidate mempool rows  # 2) update api tip height

                    # This is done via a table join of the mempool with a temporary table of  # the mined tx hashes for this block (or batch of blocks)

                    # The allocate work -> ack -> invalidate mempool cycle spans all worker  # spans all worker processes. They must all ack for all allocated work  # to satisfy the controller

                else:
                    mysql_db.mysql_bulk_load_mempool_tx_rows(tx_rows)
                    self.mysql_flush_ins_outs_and_pushdata_rows(in_rows, out_rows, set_pd_rows,
                        mysql_db)

                    # Todo - ACKs for mempool txs back to the controller + check for them in the  #  controller.  # for tx_count in acks:  # self.worker_ack_queue_tx_parse_mempool.put((tx_count))

            except _mysql.IntegrityError as e:
                self.logger.exception(f"IntegrityError: {e}")
                raise

    def mysql_insert_confirmed_tx_rows_thread(self):
        txs, ins, outs, pds, acks = [], [], [], [], []
        mysql_db: MySQLDatabase = mysql_connect(worker_id=self.worker_id)
        try:
            while True:
                try:
                    # Pre-IBD do large batched flushes
                    confirmed_rows = self.confirmed_tx_flush_queue.get(
                        timeout=self.BLOCK_BATCHING_RATE)
                    if not confirmed_rows:  # poison pill
                        break

                    block_acks = self.confirmed_tx_flush_ack_queue.get()

                    txs, ins, outs, pds = extend_batched_rows(confirmed_rows, txs, ins, outs, pds)
                    acks = append_acks(block_acks, acks)

                    if len(txs) > self.BLOCKS_MAX_TX_BATCH_LIMIT:
                        self.mysql_flush_rows(txs, ins, outs, pds, acks, confirmed=True,
                            mysql_db=mysql_db)
                        txs, inputs, outs, pds, acks = [], [], [], [], []

                # Post-IBD
                except queue.Empty:
                    if len(txs) != 0:
                        self.mysql_flush_rows(txs, ins, outs, pds, acks, confirmed=True,
                            mysql_db=mysql_db)
                        txs, ins, outs, pds, acks = [], [], [], [], []
                    continue

        except Exception as e:
            self.logger.exception(e)
            raise e
        finally:
            mysql_db.close()

    def mysql_insert_mempool_tx_rows_thread(self):
        txs, ins, outs, pds, acks = [], [], [], [], []
        mysql_db: MySQLDatabase = mysql_connect(worker_id=self.worker_id)
        try:
            while True:
                try:
                    mempool_rows = self.mempool_tx_flush_queue.get(
                        timeout=self.MEMPOOL_BATCHING_RATE)
                    if not mempool_rows:  # poison pill
                        break
                    mempool_tx_acks = self.mempool_tx_flush_ack_queue.get()
                    txs, ins, outs, pds = extend_batched_rows(mempool_rows, txs, ins, outs, pds)
                    append_acks(mempool_tx_acks, acks)

                    if len(txs) > self.MEMPOOL_MAX_TX_BATCH_LIMIT - 1:
                        self.logger.debug(f"hit max mempool batch size ({len(txs)})")
                        self.mysql_flush_rows(txs, ins, outs, pds, acks, confirmed=False,
                            mysql_db=mysql_db)
                        txs, ins, outs, pds, acks = [], [], [], [], []

                except queue.Empty:
                    # self.logger.debug("mempool batch timer triggered")
                    if len(txs) != 0:
                        self.mysql_flush_rows(txs, ins, outs, pds, acks, confirmed=False,
                            mysql_db=mysql_db)
                        txs, ins, outs, pds, acks = [], [], [], [], []
                    continue

        except Exception as e:
            self.logger.exception(e)
            raise e
        finally:
            mysql_db.close()

    def get_block_part_tx_hashes(self, raw_block_slice, tx_offsets) -> Tuple[TxHashes, TxHashRows]:
        """Returns both a list of tx hashes and list of tuples containing tx hashes (the same
        data ready for database insertion)"""
        var_int_field_max_size = 9
        max_size_header_plus_tx_count_field = 80 + var_int_field_max_size
        # Is this the first slice of the block? Otherwise adjust the offsets to start at zero
        if tx_offsets[0] > max_size_header_plus_tx_count_field:
            tx_offsets = array.array("Q", map(lambda x: x - tx_offsets[0], tx_offsets))
        partition_tx_hashes = calc_mtree_base_level(0, len(tx_offsets), {}, raw_block_slice, tx_offsets)[
            0]
        tx_hash_rows = []
        for full_tx_hash in partition_tx_hashes:
            tx_hash_rows.append((full_tx_hash.hex(),))
        return partition_tx_hashes, tx_hash_rows

    def get_processed_vs_unprocessed_tx_offsets(self, merged_offsets_map: Dict[bytes, int],
            merged_tx_to_work_item_id_map: Dict[bytes, int], merged_part_tx_hash_rows,
            mysql_db: MySQLDatabase) -> Tuple[Dict[int, List[int]], Dict[int, List[int]]]:
        """
        input rows, output rows and pushdata rows must not be inserted again if this has
        already occurred for the mempool transaction hence we calculate which category each tx
        belongs in before parsing the transactions (This was a design decision to allow for
        most of the "heavy lifting" to be done on the mempool txs so that when the confirmation
        comes, the additional work the db has to do is fairly minimal - this should lead to a
        more responsive end user experience).

        It's a 'merged' offsets map because it has tx_hashes from potentially many blocks. Batching
        is critically important with this particular MySQL query (for performance reasons)

        Returns a map of {blk_num: offsets} for:
            a) not_in_mempool_offsets
            b) in_mempool_offsets
        """
        t0 = time.time()
        t1 = 0

        not_in_mempool_offsets = {}
        in_mempool_offsets = {}

        try:
            # unprocessed_tx_hashes is the list of tx hashes in this batch **NOT** in the mempool
            unprocessed_tx_hashes = mysql_db.mysql_get_unprocessed_txs(merged_part_tx_hash_rows)

            for row in unprocessed_tx_hashes:
                tx_hash = row[0]
                tx_offset = merged_offsets_map.pop(tx_hash)  # pop the non-mempool txs out
                work_item_id = merged_tx_to_work_item_id_map[tx_hash]

                has_block_num = not_in_mempool_offsets.get(work_item_id)
                if not has_block_num:  # init empty array of offsets
                    not_in_mempool_offsets[work_item_id] = []
                not_in_mempool_offsets[work_item_id].append(tx_offset)

            # left-overs are mempool txs
            for tx_hash, tx_offset in merged_offsets_map.items():
                work_item_id = merged_tx_to_work_item_id_map[tx_hash]

                has_block_num = in_mempool_offsets.get(work_item_id)
                if not has_block_num:  # init empty array of offsets
                    in_mempool_offsets[work_item_id] = []
                in_mempool_offsets[work_item_id].append(tx_offset)

            # Remember to sort the offsets!
            for work_item_id in not_in_mempool_offsets:
                not_in_mempool_offsets[work_item_id].sort()

            for work_item_id in in_mempool_offsets:
                in_mempool_offsets[work_item_id].sort()

            t1 = time.time() - t0

            return not_in_mempool_offsets, in_mempool_offsets
        except Exception:
            self.logger.exception("unexpected exception in get_processed_vs_unprocessed_tx_offsets")
            raise
        finally:
            self.total_unprocessed_tx_sorting_time += t1
            if self.total_unprocessed_tx_sorting_time - self.last_time > 1:  # show every 1 cumulative sec
                self.last_time = self.total_unprocessed_tx_sorting_time
                self.logger.debug(f"total unprocessed tx sorting time: "
                                  f"{self.total_unprocessed_tx_sorting_time} seconds")

    def process_mempool_batch(self, batch, lmdb_grpc_client, mysql_db):
        tx_rows_batched, in_rows_batched, out_rows_batched, set_pd_rows_batched = [], [], [], []
        for msg in batch:
            msg_type, size_tx = struct.unpack_from(f"<II", msg)
            msg_type, size_tx, rawtx = struct.unpack(f"<II{size_tx}s", msg)
            # self.logger.debug(f"Got mempool tx: {hash_to_hex_str(double_sha256(rawtx))}")

            # Todo only does 1 mempool tx at a time at present
            dt = datetime.utcnow()
            tx_offsets = array.array("Q", [0])
            timestamp = dt.strftime("%Y-%m-%d %H:%M:%S")
            result: Tuple[List, List, List, List] = parse_txs(rawtx, tx_offsets, timestamp, False)
            tx_rows, in_rows, out_rows, set_pd_rows = result
            tx_rows_batched.extend(tx_rows)
            in_rows_batched.extend(in_rows)
            out_rows_batched.extend(out_rows)
            set_pd_rows_batched.extend(set_pd_rows)

        num_mempool_txs_processed = len(tx_rows_batched)
        self.logger.debug(f"Flushing {num_mempool_txs_processed} parsed mempool txs")
        self.mempool_tx_flush_queue.put(
            (tx_rows_batched, in_rows_batched, out_rows_batched, set_pd_rows_batched))
        self.mempool_tx_flush_ack_queue.put(num_mempool_txs_processed)

    def mempool_thread(self):
        batch = []
        prev_time_check = time.time()

        context2 = zmq.Context()
        mempool_tx_socket = context2.socket(zmq.PULL)
        mempool_tx_socket.connect("tcp://127.0.0.1:55556")

        try:
            mysql_db: MySQLDatabase = mysql_connect(worker_id=self.worker_id)
            lmdb_grpc_client = ConduitRawAPIClient()
            while True:
                try:
                    if mempool_tx_socket.poll(100, zmq.POLLIN):
                        packed_msg = mempool_tx_socket.recv(zmq.NOBLOCK)
                        if not packed_msg:
                            return  # poison pill stop command
                        batch.append(bytes(packed_msg))
                    else:
                        time_diff = time.time() - prev_time_check
                        if time_diff > self.BLOCK_BATCHING_RATE:
                            prev_time_check = time.time()
                            if batch:
                                self.process_mempool_batch(batch, lmdb_grpc_client, mysql_db)
                            batch = []
                except zmq.error.Again:
                    self.logger.debug(f"zmq.error.Again")
                    continue
        except KeyboardInterrupt:
            raise
        except Exception as e:
            self.logger.exception(e)
        finally:
            self.logger.info("Closing mined_blocks_thread")
            mempool_tx_socket.close()
            mempool_tx_socket.term()

    def get_block_slices(self, block_hashes: List[bytes],
            block_slice_offsets: List[Tuple[int, int]],
            lmdb_grpc_client: ConduitRawAPIClient) -> bytes:
        t0 = time.time()
        try:
            self.batch_id += 1
            self.raw_blocks_array_recv_events[self.batch_id] = threading.Event()
            blk_nums = lmdb_grpc_client.get_block_num_batched(block_hashes)

            # Ordering of both of these arrays must be guaranteed
            block_requests = list(zip(blk_nums, block_slice_offsets))

            all_sent: bool = lmdb_grpc_client.get_block_batched(block_requests, self.batch_id,
                self.sock_callback_ip, self.sock_port)
            assert all_sent is True
            self.raw_blocks_array_recv_events[self.batch_id].wait()

            del self.raw_blocks_array_recv_events[self.batch_id]
            return self.raw_blocks_array_cache.pop(self.batch_id)
        finally:
            tdiff = time.time() - t0
            self.grpc_time += tdiff
            if self.grpc_time - self.last_grpc_time > 0.5:  # show every 1 cumulative sec
                self.last_grpc_time = self.grpc_time
                self.logger.debug(f"total time for grpc calls={self.grpc_time}")

    def unpack_batched_msgs(self, work_items: List[bytes]) -> Tuple[
        TxHashes, List[BlockSliceOffsets], List[WorkPart]]:
        """Batched messages from zmq PULL socket"""
        block_slice_offsets: List[BlockSliceOffsets] = []  # start_offset, end_offset
        block_hashes: TxHashes = []
        unpacked_work_items: List[WorkPart] = []

        for packed_msg in work_items:
            msg_type, len_arr = struct.unpack_from("<II", packed_msg)  # get size_array
            msg_type, len_arr, work_item_id, blk_hash, blk_height, first_tx_pos_batch, part_end_offset, packed_array = struct.unpack(
                f"<III32sIIQ{len_arr}s", packed_msg)
            tx_offsets_part = array.array("Q", packed_array)

            work_unit: WorkPart = cast(WorkPart, (work_item_id, blk_hash, blk_height,
                first_tx_pos_batch, part_end_offset, tx_offsets_part))
            unpacked_work_items.append(work_unit)

            # The first partition should include the 80 byte block header + tx_count varint field
            slice_start_offset = 0 if first_tx_pos_batch == 0 else tx_offsets_part[0]
            slice_end_offset = part_end_offset
            block_slice_offsets.append((slice_start_offset, slice_end_offset))
            block_hashes.append(blk_hash)

        return block_hashes, block_slice_offsets, unpacked_work_items

    def build_merged_data_structures(self, work_items: List[bytes],
            lmdb_grpc_client: ConduitRawAPIClient) -> \
            Tuple[
                TxHashToOffsetMap,
                TxHashToWorkIdMap,
                TxHashRows,
                BatchedRawBlockSlices,
                ProcessedBlockAcks
            ]:
        """NOTE: For a very large block the work_items can arrive out of sequential order
        (e.g. if the block is broken down into 10 parts you might receive part 3 + part 10) so
        we must avoid cross-talk for tx_offsets for the same block hash / block number by relating
        them always to the work_item_id to deal with each work item separately from one another"""
        # Merge into big data structures for batch-wise processing
        merged_offsets_map: TxHashToOffsetMap = {}  # tx_hash: byte offset in block
        merged_tx_to_work_item_id_map: TxHashToWorkIdMap = {}  # tx_hash: block_num
        merged_part_tx_hash_rows: TxHashRows = []
        batched_raw_block_slices: BatchedRawBlockSlices = []
        acks: ProcessedBlockAcks = []

        block_hashes, block_slice_offsets, unpacked_batch_msgs = self.unpack_batched_msgs(
            work_items)

        raw_block_slice_array = self.get_block_slices(block_hashes, block_slice_offsets,
            lmdb_grpc_client)

        offset = 0
        for work_item_id, blk_hash, blk_height, first_tx_pos_batch, part_end_offset, tx_offsets_part in unpacked_batch_msgs:

            # Todo - Maybe could make an iterator and call next() to read the next block slice
            #   from a cached memory view? Then the same mem allocation could be reused by
            #   parse_txs...
            blk_num, len_slice = struct.unpack_from(f"<IQ", raw_block_slice_array, offset)
            blk_num, len_slice, raw_block_slice = struct.unpack_from(f"<IQ{len_slice}s",
                raw_block_slice_array, offset)
            offset += 4 + 8 + len_slice  # move to next raw_block_slice in bytearray

            part_tx_hashes, part_tx_hash_rows = self.get_block_part_tx_hashes(raw_block_slice,
                tx_offsets_part)

            # Merge into big data structures
            merged_part_tx_hash_rows.extend(part_tx_hash_rows)
            merged_offsets_map.update(dict(zip(part_tx_hashes, tx_offsets_part)))
            for tx_hash in part_tx_hashes:
                merged_tx_to_work_item_id_map[tx_hash] = work_item_id

            # Todo - I don't like this re-allocation of raw_block_slice
            batched_raw_block_slices.append(
                (raw_block_slice, work_item_id, blk_num, blk_height, first_tx_pos_batch))
            acks.append((blk_height, work_item_id, blk_hash, part_tx_hashes))

        return merged_offsets_map, merged_tx_to_work_item_id_map, merged_part_tx_hash_rows, \
            batched_raw_block_slices, acks

    def parse_txs_and_push_to_queue(self, non_mempool_tx_offsets, mempool_tx_offsets,
            batched_raw_block_slices):

        # Todo - raw_block was re-allocated & can be avoided via memoryview over cached bytearray
        for raw_block_slice, work_item, blk_num, blk_height, first_tx_pos_batch in batched_raw_block_slices:

            # These txs have no entries yet for inputs, pushdata or output tables
            rows_not_previously_in_mempool: Tuple[List, List, List, List] = parse_txs(raw_block_slice,
                non_mempool_tx_offsets[work_item], blk_height, True, first_tx_pos_batch)
            tx_rows, in_rows, out_rows, set_pd_rows = rows_not_previously_in_mempool

            # Mempool txs already have entries for inputs, pushdata or output tables so we handle
            # them differently
            have_mempool_txs = mempool_tx_offsets.get(blk_num)
            if have_mempool_txs:
                self.logger.debug(f"mempool_tx_offsets[work_item]={mempool_tx_offsets[work_item]}")
                rows_previously_in_mempool: Tuple[List, List, List, List] = parse_txs(raw_block_slice,
                    mempool_tx_offsets[work_item], blk_height, True, first_tx_pos_batch)
                tx_rows_now_confirmed, _, _, _ = rows_previously_in_mempool
                tx_rows.extend(tx_rows_now_confirmed)

            self.confirmed_tx_flush_queue.put((tx_rows, in_rows, out_rows, set_pd_rows))

    def process_work_items(self, work_items: List[bytes], lmdb_grpc_client: ConduitRawAPIClient,
            mysql_db: MySQLDatabase):
        """Every step is done in a batchwise fashion mainly to mitigate network and disc / MySQL
        latency effects. CPU-bound tasks such as parsing the txs in a block slice are done
        iteratively.

        NOTE: For a very large block the work_items can arrive out of sequential order
        (e.g. if the block is broken down into 10 parts you might receive part 3 + part 10)
        """

        merged_offsets_map, merged_tx_to_work_item_id_map, merged_part_tx_hash_rows, \
            batched_raw_block_slices, acks = self.build_merged_data_structures(work_items, lmdb_grpc_client)

        non_mempool_tx_offsets, mempool_tx_offsets = self.get_processed_vs_unprocessed_tx_offsets(
            merged_offsets_map, merged_tx_to_work_item_id_map, merged_part_tx_hash_rows, mysql_db)

        self.parse_txs_and_push_to_queue(non_mempool_tx_offsets, mempool_tx_offsets,
            batched_raw_block_slices)

        for blk_height, work_item_id, blk_hash, part_tx_hashes in acks:
            num_txs = len(part_tx_hashes)
            self.confirmed_tx_flush_ack_queue.put((work_item_id, blk_hash, num_txs))
            self.worker_ack_queue_mined_tx_hashes.put({blk_height: part_tx_hashes})

    def mined_blocks_thread(self):
        t = threading.Thread(target=self.recv_blocks_thread, daemon=True)
        t.start()

        work_items = []
        prev_time_check = time.time()

        context: zmq.Context = zmq.Context()
        mined_tx_socket: zmq.Socket = context.socket(zmq.PULL)
        mined_tx_socket.connect("tcp://127.0.0.1:55555")

        try:
            mysql_db: MySQLDatabase = mysql_connect(worker_id=self.worker_id)
            lmdb_grpc_client = ConduitRawAPIClient()
            while True:
                try:
                    if mined_tx_socket.poll(100, zmq.POLLIN):
                        packed_work_item = mined_tx_socket.recv(zmq.NOBLOCK)
                        if not packed_work_item:
                            return  # poison pill stop command
                        work_items.append(bytes(packed_work_item))
                    else:
                        time_diff = time.time() - prev_time_check
                        if time_diff > self.BLOCK_BATCHING_RATE:
                            prev_time_check = time.time()
                            if work_items:
                                self.process_work_items(work_items, lmdb_grpc_client, mysql_db)
                            work_items = []
                except zmq.error.Again:
                    self.logger.debug(f"zmq.error.Again")
                    continue
        except KeyboardInterrupt:
            raise
        except Exception as e:
            self.logger.exception(e)
        finally:
            self.logger.info("Closing mined_blocks_thread")
            mined_tx_socket.close()
            context.term()

    def main_thread(self):
        try:
            threads = [
                threading.Thread(target=self.mempool_thread, daemon=True),
                threading.Thread(target=self.mined_blocks_thread, daemon=True),
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

    def recv_blocks_thread(self):
        # VERY IMPORTANT TO DELETE ENTRIES AFTER USE FROM CACHE
        self.logger.debug("Raw socket server listening on port: " + str(self.sock_port))
        while True:
            conn, sock_addr = self.sock.accept()
            # self.logger.debug(f"accepted connection from: {sock_addr}")
            while True:
                payload = recv_msg(conn)
                if not payload:
                    break
                if payload:
                    self.logger.debug(f"received batched raw blocks payload "
                                      f"with total size: {len(payload)}")
                    batch_id, len_bytearray = struct.unpack_from("<IQ", payload)

                    # self.logger.debug(f"batch_id={batch_id}, len_bytearray={len_bytearray}")
                    batch_id, len_bytearray, raw_blocks_array = struct.unpack_from(
                        f"<IQ{len_bytearray}s", payload)
                    self.raw_blocks_array_cache[batch_id] = raw_blocks_array
                    self.raw_blocks_array_recv_events[batch_id].set()
            conn.close()
