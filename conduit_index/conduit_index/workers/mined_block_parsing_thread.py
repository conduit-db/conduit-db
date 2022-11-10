import array
import logging
import queue
import struct
import threading
import time
from functools import partial
from typing import Callable, cast

import cbor2
import refcuckoo
import zmq

from conduit_lib.constants import ZERO_HASH
from conduit_raw.conduit_raw.aiohttp_api.constants import UTXO_REGISTRATION_TOPIC, \
    PUSHDATA_REGISTRATION_TOPIC
from conduit_raw.conduit_raw.aiohttp_api.mysql_db_tip_filtering import MySQLTipFilterQueries
from conduit_raw.conduit_raw.aiohttp_api.types import TipFilterRegistrationEntry, \
    IndexerPushdataRegistrationFlag, OutpointType, PushdataFilterStateUpdate, \
    PushdataFilterMessageType, OutpointStateUpdate, OutpointMessageType, output_spend_struct, \
    CuckooResult
from .flush_blocks_thread import FlushConfirmedTransactionsThread
from ..types import BlockSliceOffsets, TxHashes, WorkPart, TxHashToOffsetMap, TxHashToWorkIdMap, \
    TxHashRows, BatchedRawBlockSlices, ProcessedBlockAcks, ProcessedBlockAck, \
    AlreadySeenMempoolTxOffsets, NewNotSeenBeforeTxOffsets, WorkItemId
from ..workers.common import maybe_refresh_mysql_connection, \
    convert_pushdata_rows_for_flush, convert_input_rows_for_flush

from conduit_lib import IPCSocketClient, MySQLDatabase
from conduit_lib.algorithms import calc_mtree_base_level, parse_txs
from conduit_lib.database.mysql.mysql_database import mysql_connect
from conduit_lib.database.mysql.types import MySQLFlushBatch, PushdataRowParsed, \
    ConfirmedTransactionRow, MempoolTransactionRow, OutputRow, InputRowParsed
from conduit_lib.types import BlockSliceRequestType
from conduit_lib.utils import zmq_recv_and_process_batchwise_no_block
from conduit_lib.zmq_sockets import connect_non_async_zmq_socket


class MinedBlockParsingThread(threading.Thread):

    def __init__(self, worker_id: int,
            confirmed_tx_flush_queue: queue.Queue[tuple[MySQLFlushBatch, ProcessedBlockAcks]],
            daemon: bool = True) -> None:
        self.logger = logging.getLogger(f"mined-block-parsing-thread-{worker_id}")
        self.logger.setLevel(logging.DEBUG)
        threading.Thread.__init__(self, daemon=daemon)

        self.worker_id = worker_id
        self.confirmed_tx_flush_queue = confirmed_tx_flush_queue

        self.zmq_context = zmq.Context[zmq.Socket[bytes]]()

        # A dedicated in-memory only table exclusive to this worker
        # it is frequently dropped and recreated for each chip-away batch
        self.inbound_tx_table_name = f'inbound_tx_table_{worker_id}'

        self.last_mysql_activity = int(time.time())

        # Metrics
        self.total_unprocessed_tx_sorting_time = 0.
        self.last_time = 0.
        self.ipc_sock_time = 0.
        self.last_ipc_sock_time = 0.

    def register_tip_filter_pushdatas(self,
            registration_entries: list[TipFilterRegistrationEntry]) -> None:
        """
        This adds in the hashes to the common cuckoo filter. The caller must have filtered out
        duplicate registrations, and only the first registration for this pushdata filter should
        ever be added.

        A difference between these and output spend notifications is that the indexer needs to
        know which user registered these, in order to do peer channel notifications.
        """
        for i, entry in enumerate(registration_entries):
            result = self._common_cuckoo.add(entry.pushdata_hash)
            if result == CuckooResult.OK:
                continue

            # Something was wrong, so we remove all the entries we just added as a bad batch.
            for entry in registration_entries[:i+1]:
                removal_result = self._common_cuckoo.remove(entry.pushdata_hash)
                if removal_result != CuckooResult.OK:
                    self.logger.error("Hash removal on filter error errored %d", removal_result)

            if result == CuckooResult.NOT_ENOUGH_SPACE:
                # A production implementation should recreate the filter with a higher number of
                # maximum entries (the next power of two). We are going to just raise an error and
                # obviously error because of it. First we will remove the hashes we added, but
                # really who cares as this error should be considered extreme corruption.
                raise NotImplementedError("Cuckoo filter addition encountered fullness")
            else:
                raise RuntimeError(f"Cuckoo filter addition encountered error {result}")

    def unregister_tip_filter_pushdatas(self, pushdata_hashes: list[bytes]) -> None:
        """
        This removes the hashes from the common cuckoo filter. The caller must have filtered out
        all hashes other than those whose final instance was just unregistered. It must not remove
        hashes that do not exist, or multiple times.
        """
        for pushdata_hash in pushdata_hashes:
            result = self._common_cuckoo.remove(pushdata_hash)
            if result != CuckooResult.OK:
                # This is not necessarily the wrong response to this event, but encountering it
                # should be an emergency for production indexer implementations.
                self.logger.error("Unexpected hash removal '%s' with result %d",
                    pushdata_hash.hex(), result)

    def run(self) -> None:
        mysql_db: MySQLDatabase = mysql_connect(worker_id=self.worker_id)
        mysql_db_tip_filter_queries = MySQLTipFilterQueries(mysql_db)
        socket_mined_tx = connect_non_async_zmq_socket(self.zmq_context, 'tcp://127.0.0.1:55555',
            zmq.SocketType.PULL, options=[(zmq.SocketOption.RCVHWM, 10000)])
        self.socket_utxo_spend_registrations = connect_non_async_zmq_socket(self.zmq_context,
            'tcp://127.0.0.1:60000', zmq.SocketType.SUB,
            options=[(zmq.SocketOption.SUBSCRIBE, b'utxo_registration')])
        self.socket_utxo_spend_notifications = connect_non_async_zmq_socket(self.zmq_context,
            'tcp://127.0.0.1:60001', zmq.SocketType.PUSH)
        self.socket_pushdata_registrations = connect_non_async_zmq_socket(self.zmq_context,
            'tcp://127.0.0.1:60002', zmq.SocketType.SUB,
            options=[(zmq.SocketOption.SUBSCRIBE, b'pushdata_registration')])
        self.socket_pushdata_notifications = connect_non_async_zmq_socket(self.zmq_context,
            'tcp://127.0.0.1:60003', zmq.SocketType.PUSH)

        state_update_to_server = OutpointStateUpdate('ffffffff',
            OutpointMessageType.READY, None, None, self.worker_id)
        self.socket_utxo_spend_notifications.send(cbor2.dumps(state_update_to_server))
        # It would be redundant to send a "READY" signal for socket_pushdata_notifications

        # TODO: Read self._unspent_output_registrations from database. At present there is
        #  no persistence of registrations
        self._unspent_output_registrations: set[OutpointType] = set()

        # Populate the shared cuckoo filter with all existing registrations. Remember that the
        # filter handles duplicate registrations, and it is a lot easier to just register them
        # and unregister them for every account, than try and manage duplicates.
        # Note that at the time of writing the bits per item is 12 (compiled into `refcuckoo`).
        self._common_cuckoo = refcuckoo.CuckooFilter(500000)  # pylint: disable=I1101
        self._filter_expiry_next_time = int(time.time()) + 30
        registration_entries = mysql_db_tip_filter_queries.read_tip_filter_registrations(
            mask=IndexerPushdataRegistrationFlag.DELETING | IndexerPushdataRegistrationFlag.FINALISED,
            expected_flags=IndexerPushdataRegistrationFlag.FINALISED)
        for registration_entry in registration_entries:
            # TODO(1.4.0) Tip filter. Expiry dates should be tracked and entries removed.
            if not self._common_cuckoo.contains(registration_entry.pushdata_hash):
                self._common_cuckoo.add(registration_entry.pushdata_hash)
        self.logger.debug("Populated the common cuckoo filter with %d entries",
            len(registration_entries))

        try:
            # Database flush thread
            t = FlushConfirmedTransactionsThread(self.worker_id, self.confirmed_tx_flush_queue)
            t.start()
            ipc_socket_client = IPCSocketClient()

            threads = [
                threading.Thread(target=self.unspent_output_registrations_thread, daemon=True),
                threading.Thread(target=self.pushdata_registrations_thread, daemon=True),
            ]
            for thread in threads:
                thread.start()

            process_batch_func: Callable[[list[bytes]], None] = partial(self.process_work_items,
                ipc_socket_client=ipc_socket_client, mysql_db=mysql_db)

            zmq_recv_and_process_batchwise_no_block(
                sock=socket_mined_tx,
                process_batch_func=process_batch_func,
                on_blocked_msg=None,
                batching_rate=0.3,
                poll_timeout_ms=100
            )
        except KeyboardInterrupt:
            return
        except Exception as e:
            self.logger.exception("Caught exception")
        finally:
            self.logger.info("Closing mined_blocks_thread")
            socket_mined_tx.close()

    def unspent_output_registrations_thread(self) -> None:
        self.logger.debug(f"Entering `unspent_output_registrations_thread` main loop")
        while True:
            # Get new registration from external API
            msg = self.socket_utxo_spend_registrations.recv()
            self.logger.debug(f"Got msg from external API: {msg!r}")
            state_update_from_server = OutpointStateUpdate(
                *cbor2.loads(msg.lstrip(UTXO_REGISTRATION_TOPIC)))
            self.logger.debug(f"Got state update from external API: {state_update_from_server}")
            assert state_update_from_server.outpoint is not None
            outpoint_obj = OutpointType.from_outpoint_struct(state_update_from_server.outpoint)

            if state_update_from_server.command & OutpointMessageType.REGISTER:
                self._unspent_output_registrations.add(outpoint_obj)
            elif state_update_from_server.command & OutpointMessageType.UNREGISTER:
                # TODO: As a temporary workaround for the risk of one client unregistering
                #  another client's utxo, could defer any unregistrations until ofter > 6 block
                #  confirmations and have it be automated on the server side
                if outpoint_obj in self._unspent_output_registrations:
                    self._unspent_output_registrations.remove(outpoint_obj)
            elif state_update_from_server.command & OutpointMessageType.CLEAR_ALL:
                self._unspent_output_registrations.clear()
            else:
                raise RuntimeError("The unspent_output_registrations_thread only handles "
                    "REGISTER, UNREGISTER and CLEAR_ALL message types")

            # ACK to external API that the outpoint is now added to the local cache for this worker
            state_update_to_server = OutpointStateUpdate(state_update_from_server.request_id,
                OutpointMessageType.ACK, state_update_from_server.outpoint, None, self.worker_id)
            self.socket_utxo_spend_notifications.send(cbor2.dumps(state_update_to_server))

    def pushdata_registrations_thread(self) -> None:
        self.logger.debug(f"Entering `pushdata_registrations_thread` main loop")
        while True:
            # Get new registration from external API
            msg = self.socket_pushdata_registrations.recv()
            state_update_from_server = PushdataFilterStateUpdate(
                *cbor2.loads(msg.lstrip(PUSHDATA_REGISTRATION_TOPIC)))
            self.logger.debug(f"Got state update from external API: {state_update_from_server}")
            if state_update_from_server.command & PushdataFilterMessageType.REGISTER:
                for entry in state_update_from_server.entries:
                    entry_obj = TipFilterRegistrationEntry(*entry)
                    if not self._common_cuckoo.contains(entry_obj.pushdata_hash):
                        self._common_cuckoo.add(entry_obj.pushdata_hash)

            elif state_update_from_server.command & PushdataFilterMessageType.UNREGISTER:
                for entry in state_update_from_server.entries:
                    entry_obj = TipFilterRegistrationEntry(*entry)
                    if self._common_cuckoo.contains(entry_obj.pushdata_hash):
                        self._common_cuckoo.remove(entry_obj.pushdata_hash)
            else:
                raise RuntimeError("The pushdata_registrations_thread only handles REGISTER"
                    "or UNREGISTER message types")

            # ACK to external API that the pushdatas are added to the local cache for this worker
            state_update_to_server = PushdataFilterStateUpdate(state_update_from_server.request_id,
                PushdataFilterMessageType.ACK, state_update_from_server.entries, [], ZERO_HASH)
            self.socket_pushdata_notifications.send(cbor2.dumps(state_update_to_server))

    def get_block_slices(self, block_hashes: list[bytes],
            block_slice_offsets: list[tuple[int, int]],
            ipc_sock_client: IPCSocketClient) -> bytes:
        t0 = time.time()
        try:
            response = ipc_sock_client.block_number_batched(block_hashes)

            # Ordering of both of these arrays must be guaranteed
            block_requests = cast(list[BlockSliceRequestType], list(zip(response.block_numbers, block_slice_offsets)))

            raw_blocks_array = ipc_sock_client.block_batched(block_requests)
            # len_bytearray = struct.unpack_from("<Q", response)
            # self.logger.debug(f"len_bytearray={len_bytearray}")
            # self.logger.debug(f"received batched raw blocks payload "
            #                   f"with total size: {len(raw_blocks_array)}")

            return cast(bytes, raw_blocks_array)
        finally:
            tdiff = time.time() - t0
            self.ipc_sock_time += tdiff
            if self.ipc_sock_time - self.last_ipc_sock_time > 0.5:  # show every 1 cumulative sec
                self.last_ipc_sock_time = self.ipc_sock_time
                self.logger.debug(f"total time for ipc socket calls={self.ipc_sock_time}")

    def unpack_batched_msgs(self, work_items: list[bytes]) \
            -> tuple[TxHashes, list[BlockSliceOffsets], list[WorkPart], bool]:
        """Batched messages from zmq PULL socket"""
        block_slice_offsets: list[BlockSliceOffsets] = []  # start_offset, end_offset
        block_hashes: TxHashes = []
        unpacked_work_items: list[WorkPart] = []

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

    # typing(AustEcon) - array.ArrayType doesn't let me specify int or bytes
    def get_block_part_tx_hashes(self, raw_block_slice: bytes,
            tx_offsets: 'array.ArrayType[int]') -> tuple[TxHashes, TxHashRows]:
        """Returns both a list of tx hashes and list of tuples containing tx hashes (the same
        data ready for database insertion)"""
        var_int_field_max_size = 9
        max_size_header_plus_tx_count_field = 80 + var_int_field_max_size
        # Is this the first slice of the block? Otherwise adjust the offsets to start at zero
        if tx_offsets[0] > max_size_header_plus_tx_count_field:
            tx_offsets = array.array("Q", map(lambda x: x - tx_offsets[0], tx_offsets))
        partition_tx_hashes = calc_mtree_base_level(0, len(tx_offsets), {}, raw_block_slice,
            tx_offsets)[0]
        tx_hash_rows = []
        for tx_hashX in partition_tx_hashes:
            # .hex() not hash_to_hex_str() because it's for csv bulk loading
            tx_hash_rows.append((tx_hashX.hex(),))
        return partition_tx_hashes, tx_hash_rows

    def build_merged_data_structures(self, work_items: list[bytes],
            ipc_socket_client: IPCSocketClient) -> \
            tuple[
                TxHashToOffsetMap,
                TxHashToWorkIdMap,
                TxHashRows,
                BatchedRawBlockSlices,
                dict[WorkItemId, ProcessedBlockAcks],
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
        acks: dict[WorkItemId, ProcessedBlockAcks] = {}

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

            batched_raw_block_slices.append(
                (raw_block_slice, work_item_id, is_reorg, blk_num, blk_hash, first_tx_pos_batch))

            # Needs full tx hashes for invalidating rows from mempool table
            if acks.get(work_item_id) is None:
                acks[work_item_id] = []
            acks[work_item_id].append(
                ProcessedBlockAck(block_num, work_item_id, blk_hash, part_tx_hashes))

        return merged_offsets_map, merged_tx_to_work_item_id_map, merged_part_tx_hash_rows, \
            batched_raw_block_slices, acks, contains_reorg_tx

    def parse_txs_and_push_to_queue(self, new_tx_offsets: NewNotSeenBeforeTxOffsets,
            not_new_tx_offsets: AlreadySeenMempoolTxOffsets,
            batched_raw_block_slices: BatchedRawBlockSlices,
            acks: dict[int, list[ProcessedBlockAck]]) -> None:
        assert self.confirmed_tx_flush_queue is not None

        for raw_block_slice, work_item, _is_reorg, blk_num, blk_hash, first_tx_pos_batch \
                in batched_raw_block_slices:
            tx_rows: list[ConfirmedTransactionRow]
            tx_rows_mempool: list[MempoolTransactionRow]
            in_rows_parsed: list[InputRowParsed]
            out_rows: list[OutputRow]
            pd_rows_parsed: list[PushdataRowParsed]
            tx_rows, tx_rows_mempool, in_rows_parsed, out_rows, pd_rows_parsed, _ = [], [], [], [], [], []

            # Mempool and Reorg txs already have entries for inputs, pushdata and output tables,
            # so we avoid re-inserting these rows a second time (`parse_txs` skips over them)
            all_tx_offsets: set[int] = new_tx_offsets.get(work_item, set()) | \
                not_new_tx_offsets.get(work_item, set())
            all_tx_offsets_sorted = array.array('Q', sorted(all_tx_offsets))

            tx_rows, tx_rows_mempool, in_rows_parsed, out_rows, pd_rows_parsed = parse_txs(raw_block_slice,
                all_tx_offsets_sorted, blk_num, True, first_tx_pos_batch,
                already_seen_offsets=not_new_tx_offsets.get(work_item, set()))

            # -----------------  TIP FILTERING SERVICE  --------------------- #

            # Send UTXO spend notifications
            for input_row in in_rows_parsed:
                spent_output = OutpointType(input_row.out_tx_hash, input_row.out_idx)
                if spent_output in self._unspent_output_registrations:
                    notification = OutpointStateUpdate('ffffffff', OutpointMessageType.SPEND,
                        None, output_spend_struct.pack(*input_row), self.worker_id)
                    self.socket_utxo_spend_notifications.send(cbor2.dumps(notification))

            # Send Cuckoo Filter match notifications
            filter_matches = []
            for pushdata_row in pd_rows_parsed:
                if self._common_cuckoo.contains(pushdata_row.pushdata_hash) == CuckooResult.OK:
                    filter_matches.append(pushdata_row)

            if len(filter_matches):
                # NOTE: if there is message delivery failure, the acks
                #  below should not be allowed to occur which will result in a redo of the same
                #  blocks on startup when it performs a "repair"
                # ZMQ send over websocket fitler_matches
                notification_pushdata = PushdataFilterStateUpdate('ffffffff',
                    PushdataFilterMessageType.NOTIFICATION, [], filter_matches, blk_hash)
                self.socket_pushdata_notifications.send(cbor2.dumps(notification_pushdata))

            # -----------------  TIP FILTERING SERVICE  --------------------- #

            pushdata_rows_for_flushing = convert_pushdata_rows_for_flush(pd_rows_parsed)
            input_rows_for_flushing = convert_input_rows_for_flush(in_rows_parsed)
            self.confirmed_tx_flush_queue.put(
                (MySQLFlushBatch(tx_rows, tx_rows_mempool, input_rows_for_flushing, out_rows,
                    pushdata_rows_for_flushing),
                acks[work_item]))

    def get_processed_vs_unprocessed_tx_offsets(self, is_reorg: bool,
            merged_offsets_map: dict[bytes, int],
            merged_tx_to_work_item_id_map: dict[bytes, int],
            merged_part_tx_hash_rows: TxHashRows,
            mysql_db: MySQLDatabase) \
                -> tuple[NewNotSeenBeforeTxOffsets, AlreadySeenMempoolTxOffsets]:
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

        new_tx_offsets: NewNotSeenBeforeTxOffsets = {}
        not_new_tx_offsets: AlreadySeenMempoolTxOffsets = {}

        try:
            # unprocessed_tx_hashes is the list of tx hashes in this batch **NOT** in the mempool
            unprocessed_tx_hashes = mysql_db.mysql_get_unprocessed_txs(is_reorg,
                merged_part_tx_hash_rows, self.inbound_tx_table_name)

            for tx_hash in unprocessed_tx_hashes:
                tx_offset = merged_offsets_map[tx_hash]  # pop the non-mempool txs out
                del merged_offsets_map[tx_hash]
                work_item_id = merged_tx_to_work_item_id_map[tx_hash]

                has_block_num = new_tx_offsets.get(work_item_id)
                if not has_block_num:  # init empty array of offsets
                    new_tx_offsets[work_item_id] = set()
                new_tx_offsets[work_item_id].add(tx_offset)

            # left-overs are not new txs
            for tx_hash, tx_offset in merged_offsets_map.items():
                work_item_id = merged_tx_to_work_item_id_map[tx_hash]
                has_block_num = not_new_tx_offsets.get(work_item_id)
                if not has_block_num:  # init empty array of offsets
                    not_new_tx_offsets[work_item_id] = set()
                not_new_tx_offsets[work_item_id].add(tx_offset)

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

    def process_work_items(self, work_items: list[bytes], ipc_socket_client: IPCSocketClient,
            mysql_db: MySQLDatabase) -> None:
        """Every step is done in a batchwise fashion mainly to mitigate network and disc / MySQL
        latency effects. CPU-bound tasks such as parsing the txs in a block slice are done
        iteratively.

        NOTE: For a very large block the work_items can arrive out of sequential order
        (e.g. if the block is broken down into 10 parts you might receive part 3 + part 10)
        """
        mysql_db, self.last_mysql_activity = maybe_refresh_mysql_connection(mysql_db,
            self.last_mysql_activity, self.logger)

        merged_offsets_map, merged_tx_to_work_item_id_map, merged_part_tx_hash_rows, \
            batched_raw_block_slices, acks, is_reorg = \
                self.build_merged_data_structures(work_items, ipc_socket_client)

        new_tx_offsets, not_new_tx_offsets = self.get_processed_vs_unprocessed_tx_offsets(is_reorg,
            merged_offsets_map, merged_tx_to_work_item_id_map, merged_part_tx_hash_rows, mysql_db)
        self.last_mysql_activity = int(time.time())
        self.parse_txs_and_push_to_queue(new_tx_offsets, not_new_tx_offsets,
            batched_raw_block_slices, acks)
