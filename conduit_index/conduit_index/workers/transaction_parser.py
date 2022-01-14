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

from typing import Tuple, List, Dict, cast, Union, Optional

import cbor2
import zmq
from MySQLdb import _mysql

from conduit_lib.constants import HashXLength
from conduit_lib.database.mysql.types import MempoolTransactionRow, InputRow, OutputRow, \
    PushdataRow, ConfirmedTransactionRow, MySQLFlushBatch, MySQLFlushBatchWithAcks, MempoolTxAck, \
    BlockAck
from conduit_lib.ipc_sock_client import IPCSocketClient
from conduit_lib.database.mysql.mysql_database import MySQLDatabase, mysql_connect
from conduit_lib.logging_client import setup_tcp_logging
from conduit_lib.algorithms import calc_mtree_base_level, parse_txs
from conduit_lib.types import BlockSliceRequestType

from ..types import ProcessedBlockAcks, TxHashRows, TxHashes, TxHashToWorkIdMap, TxHashToOffsetMap, \
    BlockSliceOffsets, WorkPart, BatchedRawBlockSlices




def extend_batched_rows(
        blk_rows: MySQLFlushBatch,
        txs: List[Union[MempoolTransactionRow, ConfirmedTransactionRow]],
        ins: List[InputRow],
        outs: List[OutputRow],
        pds: List[PushdataRow]) -> MySQLFlushBatch:
    """The updates are grouped as a safety precaution to not accidentally forget one of them"""
    tx_rows, in_rows, out_rows, set_pd_rows = blk_rows
    txs.extend(tx_rows)
    ins.extend(in_rows)
    outs.extend(out_rows)
    pds.extend(set_pd_rows)
    return MySQLFlushBatch(txs, ins, outs, pds)


def reset_rows() -> MySQLFlushBatchWithAcks:
    txs: List[Union[MempoolTransactionRow, ConfirmedTransactionRow]] = []
    ins: List[InputRow] = []
    outs: List[OutputRow] = []
    pds: List[PushdataRow] = []
    acks: List[Union[MempoolTxAck, BlockAck]] = []
    return MySQLFlushBatchWithAcks(txs, ins, outs, pds, acks)


class TxParser(multiprocessing.Process):
    """
    in: blk_hash, blk_height, blk_start_pos, blk_end_pos, tx_positions_div
    out: N/A - directly updates posgres database
    ack_confirmed: blk_hash, count_txs_done
    ack_mempool: tx_counts
    """

    def __init__(self, worker_id: int) -> None:
        super(TxParser, self).__init__()
        self.worker_id = worker_id

        # A dedicated in-memory only table exclusive to this worker
        # it is frequently dropped and recreated for each chip-away batch
        self.inbound_tx_table_name = f'inbound_tx_table_{worker_id}'

        # self.worker_ack_queue_tx_parse_mempool = worker_ack_queue_tx_parse_mempool

        self.confirmed_tx_flush_queue: Optional[queue.Queue[MySQLFlushBatch]] = None
        self.mempool_tx_flush_queue: Optional[queue.Queue[MySQLFlushBatch]] = None

        self.mysql: Optional[MySQLDatabase] = None

        # accumulate x seconds worth of txs or MAX_TX_BATCH_SIZE (whichever comes first)
        # TODO - during initial block download - should NOT rely on a timeout at all
        #  Should just keep on pumping the entire batch of blocks as fast as possible to
        #  Max out CPU.
        #  This ideally requires reliable PUB/SUB to do properly
        self.BLOCKS_MAX_TX_BATCH_LIMIT = 200_000
        self.BLOCK_BATCHING_RATE = 0.3
        self.MEMPOOL_MAX_TX_BATCH_LIMIT = 2000
        self.MEMPOOL_BATCHING_RATE = 0.1

        self.total_unprocessed_tx_sorting_time = 0.
        self.last_time = 0.
        self.grpc_time = 0.
        self.last_grpc_time = 0.

    def run(self) -> None:
        # PUB-SUB from Controller to worker to kill the worker
        context3 = zmq.Context()  # type: ignore
        self.kill_worker_socket: zmq.Socket = context3.socket(zmq.SUB)  # type: ignore
        self.kill_worker_socket.connect("tcp://127.0.0.1:63241")  # type: ignore
        self.kill_worker_socket.setsockopt(zmq.SUBSCRIBE, b"stop_signal")

        context4 = zmq.Context()  # type: ignore
        self.is_ibd_socket = context4.socket(zmq.SUB)  # type: ignore
        self.is_ibd_socket.connect("tcp://127.0.0.1:52841")
        self.is_ibd_socket.setsockopt(zmq.SUBSCRIBE, b"is_ibd_signal")

        context6 = zmq.Context()  # type: ignore
        self.tx_parse_ack_socket: zmq.Socket = context6.socket(zmq.PUSH)  # type: ignore
        self.tx_parse_ack_socket.connect("tcp://127.0.0.1:53213")  # type: ignore

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
        self.logger.info(f"Started {self.__class__.__name__}")

        self.confirmed_tx_flush_queue = queue.Queue()
        self.mempool_tx_flush_queue = queue.Queue()
        self.confirmed_tx_flush_ack_queue: queue.Queue[BlockAck] = queue.Queue()
        self.mempool_tx_flush_ack_queue: queue.Queue[MempoolTxAck] = queue.Queue()
        try:
            main_thread = threading.Thread(target=self.main_thread, daemon=True)
            main_thread.start()

            self.start_flush_threads()
            self.logger.info(f"{self.__class__.__name__} exiting")
        except Exception as e:
            self.logger.exception(e)
            raise

    def start_flush_threads(self) -> None:
        threads = [threading.Thread(target=self.mysql_insert_confirmed_tx_rows_thread, daemon=True),
            threading.Thread(target=self.mysql_insert_mempool_tx_rows_thread, daemon=True), ]
        for t in threads:
            t.setDaemon(True)
            t.start()
        for t in threads:
            t.join()

    def mysql_flush_ins_outs_and_pushdata_rows(self, in_rows: List[InputRow],
            out_rows: List[OutputRow], pd_rows: List[PushdataRow], mysql_db: MySQLDatabase) -> None:
        mysql_db.mysql_bulk_load_output_rows(out_rows)
        mysql_db.mysql_bulk_load_input_rows(in_rows)
        mysql_db.mysql_bulk_load_pushdata_rows(pd_rows)

    def mysql_flush_rows(self, flush_batch_with_acks: MySQLFlushBatchWithAcks,
            confirmed: bool, mysql_db: MySQLDatabase) -> None:
        tx_rows, in_rows, out_rows, pd_rows, acks = flush_batch_with_acks
        with self.flush_lock:
            try:
                if confirmed:
                    mysql_db.mysql_bulk_load_confirmed_tx_rows(cast(List[ConfirmedTransactionRow], tx_rows))
                    self.mysql_flush_ins_outs_and_pushdata_rows(in_rows, out_rows, pd_rows,
                        mysql_db)

                    # Ack for all flushed blocks
                    for ack in acks:
                        ack = cast(BlockAck, ack)
                        work_item_id, blk_hash, tx_count = ack
                        self.tx_parse_ack_socket.send(  # type: ignore
                            cbor2.dumps((self.worker_id, work_item_id, blk_hash, tx_count)))
                else:
                    mysql_db.mysql_bulk_load_mempool_tx_rows(cast(List[MempoolTransactionRow], tx_rows))
                    self.mysql_flush_ins_outs_and_pushdata_rows(in_rows, out_rows, pd_rows,
                        mysql_db)

            except _mysql.IntegrityError as e:
                self.logger.exception(f"IntegrityError: {e}")
                raise

    def mysql_insert_confirmed_tx_rows_thread(self) -> None:
        assert self.confirmed_tx_flush_queue is not None
        txs, ins, outs, pds, acks = reset_rows()
        mysql_db: MySQLDatabase = mysql_connect(worker_id=self.worker_id)
        try:
            while True:
                try:
                    # Pre-IBD do large batched flushes
                    confirmed_rows = self.confirmed_tx_flush_queue.get(
                        timeout=self.BLOCK_BATCHING_RATE)
                    if not confirmed_rows:  # poison pill
                        break

                    block_ack = self.confirmed_tx_flush_ack_queue.get()

                    txs, ins, outs, pds = extend_batched_rows(confirmed_rows, txs, ins, outs, pds)
                    acks.append(block_ack)

                    if len(txs) > self.BLOCKS_MAX_TX_BATCH_LIMIT:
                        self.mysql_flush_rows(MySQLFlushBatchWithAcks(txs, ins, outs, pds, acks),
                            confirmed=True, mysql_db=mysql_db)
                        txs, ins, outs, pds, acks = reset_rows()

                # Post-IBD
                except queue.Empty:
                    if len(txs) != 0:
                        self.mysql_flush_rows(MySQLFlushBatchWithAcks(txs, ins, outs, pds, acks),
                            confirmed=True, mysql_db=mysql_db)
                        txs, ins, outs, pds, acks = reset_rows()
                    continue

        except Exception as e:
            self.logger.exception(e)
            raise e
        finally:
            mysql_db.close()

    def mysql_insert_mempool_tx_rows_thread(self) -> None:
        assert self.mempool_tx_flush_queue is not None
        txs, ins, outs, pds, acks = reset_rows()
        mysql_db: MySQLDatabase = mysql_connect(worker_id=self.worker_id)
        try:
            while True:
                try:
                    mempool_rows = self.mempool_tx_flush_queue.get(
                        timeout=self.MEMPOOL_BATCHING_RATE)
                    if not mempool_rows:  # poison pill
                        break
                    mempool_tx_ack = self.mempool_tx_flush_ack_queue.get()
                    txs, ins, outs, pds = extend_batched_rows(mempool_rows, txs, ins, outs, pds)
                    acks.append(mempool_tx_ack)

                    if len(txs) > self.MEMPOOL_MAX_TX_BATCH_LIMIT - 1:
                        self.logger.debug(f"hit max mempool batch size ({len(txs)})")
                        self.mysql_flush_rows(MySQLFlushBatchWithAcks(txs, ins, outs, pds, acks),
                            confirmed=False, mysql_db=mysql_db)
                        txs, ins, outs, pds, acks = reset_rows()

                except queue.Empty:
                    # self.logger.debug("mempool batch timer triggered")
                    if len(txs) != 0:
                        self.mysql_flush_rows(MySQLFlushBatchWithAcks(txs, ins, outs, pds, acks),
                            confirmed=False, mysql_db=mysql_db)
                        txs, ins, outs, pds, acks = reset_rows()
                    continue

        except Exception as e:
            self.logger.exception(e)
            raise e
        finally:
            mysql_db.close()

    # typing(AustEcon) - array.ArrayType doesn't let me specify int or bytes
    def get_block_part_tx_hashes(self, raw_block_slice: bytes,
            tx_offsets: array.ArrayType) -> Tuple[TxHashes, TxHashRows]:  # type: ignore
        """Returns both a list of tx hashes and list of tuples containing tx hashes (the same
        data ready for database insertion)"""
        var_int_field_max_size = 9
        max_size_header_plus_tx_count_field = 80 + var_int_field_max_size
        # Is this the first slice of the block? Otherwise adjust the offsets to start at zero
        if tx_offsets[0] > max_size_header_plus_tx_count_field:
            tx_offsets = array.array("Q", map(lambda x: x - tx_offsets[0], tx_offsets))  # type: ignore
        partition_tx_hashes = calc_mtree_base_level(0, len(tx_offsets), {}, raw_block_slice, tx_offsets)[
            0]
        tx_hash_rows = []

        partition_tx_hashXes = [x[0:HashXLength] for x in partition_tx_hashes]
        for tx_hashX in partition_tx_hashXes:
            # .hex() not hash_to_hex_str() because it's for csv bulk loading
            tx_hash_rows.append((tx_hashX.hex(),))
        return partition_tx_hashXes, tx_hash_rows

    def get_processed_vs_unprocessed_tx_offsets(self, is_reorg: bool,
            merged_offsets_map: Dict[bytes, int],
            merged_tx_to_work_item_id_map: Dict[bytes, int],
            merged_part_tx_hash_rows: TxHashRows,
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
            a) new_tx_offsets  # Must not be in an orphaned block
            b) not_new_tx_offsets  # Either in mempool or an orphaned block
        """
        t0 = time.time()
        t1 = 0.

        new_tx_offsets: Dict[int, List[int]] = {}
        not_new_tx_offsets: Dict[int, List[int]] = {}

        try:
            # unprocessed_tx_hashes is the list of tx hashes in this batch **NOT** in the mempool
            unprocessed_tx_hashes = mysql_db.mysql_get_unprocessed_txs(is_reorg,
                merged_part_tx_hash_rows, self.inbound_tx_table_name)

            len_merged_offsets_map_before = len(merged_offsets_map)
            for tx_hash in unprocessed_tx_hashes:
                tx_offset = merged_offsets_map[tx_hash]  # pop the non-mempool txs out
                del merged_offsets_map[tx_hash]
                work_item_id = merged_tx_to_work_item_id_map[tx_hash]

                has_block_num = new_tx_offsets.get(work_item_id)
                if not has_block_num:  # init empty array of offsets
                    new_tx_offsets[work_item_id] = []
                new_tx_offsets[work_item_id].append(tx_offset)

            # left-overs are not new txs
            for tx_hash, tx_offset in merged_offsets_map.items():
                work_item_id = merged_tx_to_work_item_id_map[tx_hash]
                has_block_num = not_new_tx_offsets.get(work_item_id)
                if not has_block_num:  # init empty array of offsets
                    not_new_tx_offsets[work_item_id] = []
                not_new_tx_offsets[work_item_id].append(tx_offset)

            # Remember to sort the offsets!
            for work_item_id in new_tx_offsets:
                new_tx_offsets[work_item_id].sort()

            for work_item_id in not_new_tx_offsets:
                not_new_tx_offsets[work_item_id].sort()

            t1 = time.time() - t0
            return new_tx_offsets, not_new_tx_offsets
        except KeyError as e:
            self.logger.exception("KeyError in get_processed_vs_unprocessed_tx_offsets")
            raise
        except Exception:
            self.logger.exception("unexpected exception in get_processed_vs_unprocessed_tx_offsets")
            raise
        finally:
            self.total_unprocessed_tx_sorting_time += t1
            if self.total_unprocessed_tx_sorting_time - self.last_time > 1:  # show every 1 cumulative sec
                self.last_time = self.total_unprocessed_tx_sorting_time
                self.logger.debug(f"total unprocessed tx sorting time: "
                                  f"{self.total_unprocessed_tx_sorting_time} seconds")

    def process_mempool_batch(self, batch: list[bytes]) -> None:
        assert self.mempool_tx_flush_queue is not None
        tx_rows_batched, in_rows_batched, out_rows_batched, set_pd_rows_batched = [], [], [], []
        for msg in batch:
            msg_type, size_tx = struct.unpack_from(f"<II", msg)
            msg_type, size_tx, rawtx = struct.unpack(f"<II{size_tx}s", msg)
            # self.logger.debug(f"Got mempool tx: {hash_to_hex_str(double_sha256(rawtx))}")

            # Todo only does 1 mempool tx at a time at present
            dt = datetime.utcnow()
            tx_offsets = [0]
            rawtx = array.array('B', rawtx)
            timestamp = dt.isoformat()
            result: MySQLFlushBatch = parse_txs(rawtx, tx_offsets, timestamp, False, 0)
            tx_rows, in_rows, out_rows, set_pd_rows = result
            tx_rows_batched.extend(tx_rows)
            in_rows_batched.extend(in_rows)
            out_rows_batched.extend(out_rows)
            set_pd_rows_batched.extend(set_pd_rows)

        num_mempool_txs_processed = len(tx_rows_batched)
        self.logger.debug(f"Flushing {num_mempool_txs_processed} parsed mempool txs")
        self.mempool_tx_flush_queue.put(
            MySQLFlushBatch(tx_rows_batched, in_rows_batched, out_rows_batched,
                set_pd_rows_batched))
        self.mempool_tx_flush_ack_queue.put(MempoolTxAck(num_mempool_txs_processed))

    def mempool_thread(self) -> None:
        while True:
            # For some reason I am unable to catch a KeyboardInterrupt or SIGINT here so
            # need to rely on an overt "stop_signal" from the Controller for graceful shutdown
            message = self.is_ibd_socket.recv()
            if message == b"is_ibd_signal":
                self.logger.debug(f"Got initial block download signal. "
                    f"Starting mempool tx parsing thread.")
                break

        batch = []
        prev_time_check = time.time()

        context2 = zmq.Context()  # type: ignore
        mempool_tx_socket = context2.socket(zmq.PULL)  # type: ignore
        mempool_tx_socket.connect("tcp://127.0.0.1:55556")
        try:
            while True:
                try:
                    if mempool_tx_socket.poll(1000, zmq.POLLIN):
                        packed_msg = mempool_tx_socket.recv(zmq.NOBLOCK)
                        if not packed_msg:
                            return  # poison pill stop command
                        batch.append(bytes(packed_msg))
                    else:
                        time_diff = time.time() - prev_time_check
                        # Todo: This might be redundant with poll(1000)
                        #  1 second is longer than self.BLOCK_BATCHING_RATE
                        if time_diff > self.BLOCK_BATCHING_RATE:
                            prev_time_check = time.time()
                            if batch:
                                self.process_mempool_batch(batch)
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
            # mempool_tx_socket.term()

    def get_block_slices(self, block_hashes: List[bytes],
            block_slice_offsets: List[Tuple[int, int]],
            ipc_sock_client: IPCSocketClient) -> bytes:
        t0 = time.time()
        try:
            response = ipc_sock_client.block_number_batched(block_hashes)

            # Ordering of both of these arrays must be guaranteed
            block_requests = cast(List[BlockSliceRequestType], list(zip(response.block_numbers, block_slice_offsets)))

            raw_blocks_array = ipc_sock_client.block_batched(block_requests)
            # len_bytearray = struct.unpack_from("<Q", response)
            # self.logger.debug(f"len_bytearray={len_bytearray}")
            self.logger.debug(f"received batched raw blocks payload "
                              f"with total size: {len(raw_blocks_array)}")

            return cast(bytes, raw_blocks_array)
        finally:
            tdiff = time.time() - t0
            self.grpc_time += tdiff
            if self.grpc_time - self.last_grpc_time > 0.5:  # show every 1 cumulative sec
                self.last_grpc_time = self.grpc_time
                self.logger.debug(f"total time for grpc calls={self.grpc_time}")

    def unpack_batched_msgs(self, work_items: List[bytes]) \
            -> Tuple[TxHashes, List[BlockSliceOffsets], List[WorkPart], bool]:
        """Batched messages from zmq PULL socket"""
        block_slice_offsets: List[BlockSliceOffsets] = []  # start_offset, end_offset
        block_hashes: TxHashes = []
        unpacked_work_items: List[WorkPart] = []

        reorg = False
        for packed_msg in work_items:
            msg_type, len_arr = struct.unpack_from("<II", packed_msg)  # get size_array
            msg_type, len_arr, work_item_id, is_reorg, blk_hash, block_num, first_tx_pos_batch, \
                part_end_offset, packed_array = struct.unpack(f"<IIII32sIIQ{len_arr}s", packed_msg)
            tx_offsets_part = array.array("Q", packed_array)

            if bool(is_reorg) is True:
                reorg = True

            work_unit: WorkPart = cast(WorkPart, (work_item_id, blk_hash, block_num,
                first_tx_pos_batch, part_end_offset, tx_offsets_part))
            unpacked_work_items.append(work_unit)

            # The first partition should include the 80 byte block header + tx_count varint field
            slice_start_offset = 0 if first_tx_pos_batch == 0 else tx_offsets_part[0]
            slice_end_offset = part_end_offset
            block_slice_offsets.append((slice_start_offset, slice_end_offset))
            block_hashes.append(blk_hash)

        return block_hashes, block_slice_offsets, unpacked_work_items, reorg

    def build_merged_data_structures(self, work_items: List[bytes],
            ipc_socket_client: IPCSocketClient) -> \
            Tuple[
                TxHashToOffsetMap,
                TxHashToWorkIdMap,
                TxHashRows,
                BatchedRawBlockSlices,
                ProcessedBlockAcks,
                bool
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

        block_hashes, block_slice_offsets, unpacked_batch_msgs, is_reorg = self.unpack_batched_msgs(
            work_items)

        raw_block_slice_array = self.get_block_slices(block_hashes, block_slice_offsets,
            ipc_socket_client)

        # self.logger.debug(f"len(raw_block_slice_array)={len(raw_block_slice_array)}")
        # self.logger.debug(f"len(block_slice_offsets)={len(block_slice_offsets)}")
        # self.logger.debug(f"block_hashes={[hash_to_hex_str(x) for x in block_hashes]}")

        offset = 0
        contains_reorg_tx = False
        # todo - block_num may not be needed, only block_hash
        for work_item_id, blk_hash, block_num, first_tx_pos_batch, part_end_offset, \
                tx_offsets_part in unpacked_batch_msgs:

            is_reorg = bool(is_reorg)
            if is_reorg is True:
                contains_reorg_tx = True
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
            raw_block_slice = array.array('B', raw_block_slice)
            batched_raw_block_slices.append(
                (raw_block_slice, work_item_id, is_reorg, blk_num, first_tx_pos_batch))
            acks.append((block_num, work_item_id, blk_hash, part_tx_hashes))

        return merged_offsets_map, merged_tx_to_work_item_id_map, merged_part_tx_hash_rows, \
            batched_raw_block_slices, acks, contains_reorg_tx

    def parse_txs_and_push_to_queue(self, new_tx_offsets: Dict[int, List[int]],
            not_new_tx_offsets: Dict[int, List[int]],
            batched_raw_block_slices: BatchedRawBlockSlices) -> None:
        # Todo - raw_block memory allocations
        assert self.confirmed_tx_flush_queue is not None

        for raw_block_slice, work_item, is_reorg, blk_num, first_tx_pos_batch \
                in batched_raw_block_slices:
            tx_rows, in_rows, out_rows, pd_rows, _ = reset_rows()
            # Mempool txs already have entries for inputs, pushdata or output tables so we handle
            # them differently

            # These txs have no entries yet for inputs, pushdata or output tables
            have_new_mined_txs = work_item in new_tx_offsets
            if have_new_mined_txs:
                rows_not_previously_in_mempool: \
                    MySQLFlushBatch = parse_txs(raw_block_slice, new_tx_offsets[work_item],
                        blk_num, True, first_tx_pos_batch)
                tx_rows, in_rows, out_rows, pd_rows = rows_not_previously_in_mempool

            have_not_new_txs = work_item in not_new_tx_offsets
            if have_not_new_txs:  # Includes orphaned block txs
                rows_previously_in_mempool: MySQLFlushBatch = parse_txs(raw_block_slice,
                    not_new_tx_offsets[work_item], blk_num, True, first_tx_pos_batch)
                tx_rows_now_confirmed, _, _, _ = rows_previously_in_mempool
                tx_rows_now_confirmed_fixed = []
                if is_reorg:  # the tx_pos will actually be wrong so need to fix it here
                    all_tx_offsets: List[int] = new_tx_offsets[work_item]
                    all_tx_offsets.extend(not_new_tx_offsets[work_item])
                    all_tx_offsets.sort()
                    corrected_tx_positions = []
                    for tx_offset in not_new_tx_offsets[work_item]:
                        corrected_tx_pos = all_tx_offsets.index(tx_offset)
                        corrected_tx_positions.append(corrected_tx_pos)
                    for row, corrected_tx_pos in zip(tx_rows_now_confirmed, corrected_tx_positions):
                        tx_rows_now_confirmed_fixed.append(
                            ConfirmedTransactionRow(row[0], int(row[1]), corrected_tx_pos))

                    tx_rows.extend(tx_rows_now_confirmed_fixed)
                else:
                    tx_rows.extend(tx_rows_now_confirmed)

            self.confirmed_tx_flush_queue.put(MySQLFlushBatch(tx_rows, in_rows, out_rows, pd_rows))

    def process_work_items(self, work_items: List[bytes], ipc_socket_client: IPCSocketClient,
            mysql_db: MySQLDatabase, ack_for_mined_tx_socket: zmq.Socket) -> None:
        """Every step is done in a batchwise fashion mainly to mitigate network and disc / MySQL
        latency effects. CPU-bound tasks such as parsing the txs in a block slice are done
        iteratively.

        NOTE: For a very large block the work_items can arrive out of sequential order
        (e.g. if the block is broken down into 10 parts you might receive part 3 + part 10)
        """

        merged_offsets_map, merged_tx_to_work_item_id_map, merged_part_tx_hash_rows, \
            batched_raw_block_slices, acks, is_reorg = \
                self.build_merged_data_structures(work_items, ipc_socket_client)

        new_tx_offsets, not_new_tx_offsets = self.get_processed_vs_unprocessed_tx_offsets(is_reorg,
            merged_offsets_map, merged_tx_to_work_item_id_map, merged_part_tx_hash_rows, mysql_db)

        self.parse_txs_and_push_to_queue(new_tx_offsets, not_new_tx_offsets,
            batched_raw_block_slices)

        for blk_num, work_item_id, blk_hash, part_tx_hashes in acks:
            num_txs = len(part_tx_hashes)
            self.confirmed_tx_flush_ack_queue.put(BlockAck(work_item_id, blk_hash, num_txs))
            msg = cbor2.dumps({blk_num: part_tx_hashes})
            ack_for_mined_tx_socket.send(msg)  # type: ignore

    def mined_blocks_thread(self) -> None:
        work_items = []
        prev_time_check = time.time()

        context: zmq.Context = zmq.Context()  # type: ignore
        mined_tx_socket: zmq.Socket = context.socket(zmq.PULL)  # type: ignore
        mined_tx_socket.connect("tcp://127.0.0.1:55555")  # type: ignore

        context2 = zmq.Context()  # type: ignore
        ack_for_mined_tx_socket: zmq.Socket = context2.socket(zmq.PUSH)  # type: ignore
        ack_for_mined_tx_socket.connect("tcp://127.0.0.1:55889")  # type: ignore

        try:
            mysql_db: MySQLDatabase = mysql_connect(worker_id=self.worker_id)
            ipc_socket_client = IPCSocketClient()
            while True:
                try:
                    if mined_tx_socket.poll(100, zmq.POLLIN):  # type: ignore
                        packed_work_item: bytes = cast(bytes, mined_tx_socket.recv(zmq.NOBLOCK))
                        if not packed_work_item:
                            return  # poison pill stop command
                        work_items.append(bytes(packed_work_item))
                    else:
                        time_diff = time.time() - prev_time_check
                        if time_diff > self.BLOCK_BATCHING_RATE:
                            prev_time_check = time.time()
                            if work_items:
                                self.process_work_items(work_items, ipc_socket_client, mysql_db,
                                    ack_for_mined_tx_socket)
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
            mined_tx_socket.close()  # type: ignore
            ack_for_mined_tx_socket.close()  # type: ignore
            context.term()  # type: ignore

    def main_thread(self) -> None:
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
