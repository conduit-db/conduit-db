# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.

import array
import typing
import cbor2
from functools import partial
import logging
import queue
import struct
import threading
import time
from typing import Callable, cast

import psutil
import zmq

from conduit_lib.constants import PROFILING, CONDUIT_INDEX_SERVICE_NAME
from conduit_lib.database.db_interface.tip_filter_types import TipFilterRegistrationEntry
from conduit_lib.database.db_interface.types import (
    MySQLFlushBatch,
    ConfirmedTransactionRow,
    MempoolTransactionRow,
    InputRowParsed,
    PushdataRowParsed,
    ProcessedBlockAcks,
    TipFilterNotifications,
    ProcessedBlockAck,
)
from conduit_raw.conduit_raw.aiohttp_api.constants import (
    UTXO_REGISTRATION_TOPIC,
    PUSHDATA_REGISTRATION_TOPIC,
)
from conduit_raw.conduit_raw.aiohttp_api.types import (
    PushdataFilterStateUpdate,
    PushdataFilterMessageType,
    OutpointStateUpdate,
    OutpointMessageType,
    CuckooResult,
)
from .flush_blocks_thread import FlushConfirmedTransactionsThread
from ..types import (
    TxHashes,
    TxHashRows,
)
from ..workers.common import (
    convert_pushdata_rows_for_flush,
    convert_input_rows_for_flush,
)

from conduit_lib import IPCSocketClient, DBInterface
from conduit_lib.algorithms import calc_mtree_base_level, parse_txs
from conduit_lib.types import BlockSliceRequestType, OutpointType, Slice
from conduit_lib.utils import zmq_recv_and_process_batchwise_no_block, get_log_level
from conduit_lib.zmq_sockets import connect_non_async_zmq_socket

if typing.TYPE_CHECKING:
    from .transaction_parser import TxParser


class MinedBlockParsingThread(threading.Thread):
    def __init__(
        self,
        parent: "TxParser",
        worker_id: int,
        daemon: bool = True,
    ) -> None:
        self.logger = logging.getLogger(f"mined-block-parsing-thread-{worker_id}")
        self.logger.setLevel(get_log_level(CONDUIT_INDEX_SERVICE_NAME))
        threading.Thread.__init__(self, daemon=daemon)

        self.parent = parent
        self.worker_id = worker_id
        self.confirmed_tx_flush_queue: queue.Queue[
            tuple[MySQLFlushBatch, ProcessedBlockAcks, TipFilterNotifications]
        ] = queue.Queue(maxsize=10000)

        self.zmq_context = zmq.Context[zmq.Socket[bytes]]()

        # A dedicated in-memory only table exclusive to this worker
        # it is frequently dropped and recreated for each chip-away batch
        self.inbound_tx_table_name = f"inbound_tx_table_{worker_id}"

        self.last_activity = int(time.time())

        # Metrics
        self.total_unprocessed_tx_sorting_time = 0.0
        self.last_time = 0.0
        self.ipc_sock_time = 0.0
        self.last_ipc_sock_time = 0.0

    def register_tip_filter_pushdatas(self, registration_entries: list[TipFilterRegistrationEntry]) -> None:
        """
        This adds in the hashes to the common cuckoo filter. The caller must have filtered out
        duplicate registrations, and only the first registration for this pushdata filter should
        ever be added.

        A difference between these and output spend notifications is that the indexer needs to
        know which user registered these, in order to do peer channel notifications.
        """
        for i, entry in enumerate(registration_entries):
            result = self.parent.common_cuckoo.add(entry.pushdata_hash)
            if result == CuckooResult.OK:
                continue

            # Something was wrong, so we remove all the entries we just added as a bad batch.
            for entry in registration_entries[: i + 1]:
                removal_result = self.parent.common_cuckoo.remove(entry.pushdata_hash)
                if removal_result != CuckooResult.OK:
                    self.logger.error(
                        "Hash removal on filter error errored %d",
                        removal_result,
                    )

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
            result = self.parent.common_cuckoo.remove(pushdata_hash)
            if result != CuckooResult.OK:
                # This is not necessarily the wrong response to this event, but encountering it
                # should be an emergency for production indexer implementations.
                self.logger.error(
                    "Unexpected hash removal '%s' with result %d",
                    pushdata_hash.hex(),
                    result,
                )

    def run(self) -> None:
        socket_mined_tx = connect_non_async_zmq_socket(
            self.zmq_context,
            "tcp://127.0.0.1:55555",
            zmq.SocketType.PULL,
            options=[(zmq.SocketOption.RCVHWM, 10000)],
        )
        db: DBInterface = DBInterface.load_db(worker_id=self.worker_id, wait_time=10)

        try:
            # Database flush thread
            t = FlushConfirmedTransactionsThread(self.parent, self.worker_id, self.confirmed_tx_flush_queue)
            t.start()
            ipc_socket_client = IPCSocketClient()

            threads = [
                threading.Thread(target=self.unspent_output_registrations_thread, daemon=True),
                threading.Thread(target=self.pushdata_registrations_thread, daemon=True),
            ]
            for thread in threads:
                thread.start()

            process_batch_func: Callable[[list[bytes]], None] = partial(
                self.process_work_items,
                ipc_socket_client=ipc_socket_client,
                db=db,
            )

            zmq_recv_and_process_batchwise_no_block(
                sock=socket_mined_tx,
                process_batch_func=process_batch_func,
                on_blocked_msg=None,
                batching_rate=0.3,
                poll_timeout_ms=100,
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
            msg = cast(bytes, self.parent.socket_utxo_spend_registrations.recv())  # type: ignore[redundant-cast]
            self.logger.debug(f"Got msg from external API: {msg!r}")
            outpoint_msg = cast(OutpointStateUpdate, cbor2.loads(msg.lstrip(UTXO_REGISTRATION_TOPIC)))
            state_update_from_server = OutpointStateUpdate(*outpoint_msg)
            self.logger.debug(f"Got state update from external API: {state_update_from_server}")
            assert state_update_from_server.outpoint is not None
            outpoint_obj = OutpointType.from_outpoint_struct(state_update_from_server.outpoint)

            if state_update_from_server.command & OutpointMessageType.REGISTER:
                self.parent.unspent_output_registrations.add(outpoint_obj)
            elif state_update_from_server.command & OutpointMessageType.UNREGISTER:
                # TODO: As a temporary workaround for the risk of one client unregistering
                #  another client's utxo, could defer any unregistrations until ofter > 6 block
                #  confirmations and have it be automated on the server side
                if outpoint_obj in self.parent.unspent_output_registrations:
                    self.parent.unspent_output_registrations.remove(outpoint_obj)
            elif state_update_from_server.command & OutpointMessageType.CLEAR_ALL:
                self.parent.unspent_output_registrations.clear()
            else:
                raise RuntimeError(
                    "The unspent_output_registrations_thread only handles "
                    "REGISTER, UNREGISTER and CLEAR_ALL message types"
                )

            # ACK to external API that the outpoint is now added to the local cache for this worker
            state_update_to_server = OutpointStateUpdate(
                state_update_from_server.request_id,
                OutpointMessageType.ACK,
                state_update_from_server.outpoint,
                None,
                self.worker_id,
            )
            self.parent.socket_utxo_spend_notifications.send(cbor2.dumps(state_update_to_server))

    def pushdata_registrations_thread(self) -> None:
        self.logger.debug(f"Entering `pushdata_registrations_thread` main loop")
        while True:
            # Get new registration from external API
            msg = self.parent.socket_pushdata_registrations.recv()
            msg_unpacked = cast(
                PushdataFilterStateUpdate, cbor2.loads(msg.lstrip(PUSHDATA_REGISTRATION_TOPIC))
            )
            state_update_from_server = PushdataFilterStateUpdate(*msg_unpacked)
            self.logger.debug(
                f"Got state update from external API of type: " f"{state_update_from_server.command}"
            )
            if state_update_from_server.command & PushdataFilterMessageType.REGISTER:
                for entry in state_update_from_server.entries:
                    entry_obj = TipFilterRegistrationEntry(*entry)
                    self.logger.debug(f"adding pushdata hash: " f"{entry_obj.pushdata_hash.hex()}")
                    self.parent.common_cuckoo.add(entry_obj.pushdata_hash)

            elif state_update_from_server.command & PushdataFilterMessageType.UNREGISTER:
                for entry in state_update_from_server.entries:
                    entry_obj = TipFilterRegistrationEntry(*entry)
                    self.parent.common_cuckoo.remove(entry_obj.pushdata_hash)
            else:
                raise RuntimeError(
                    "The pushdata_registrations_thread only handles REGISTER" "or UNREGISTER message types"
                )

            # ACK to external API that the pushdatas are added to the local cache for this worker
            state_update_to_server = PushdataFilterStateUpdate(
                state_update_from_server.request_id,
                PushdataFilterMessageType.ACK,
                state_update_from_server.entries,
                [],
                None,
            )
            self.parent.socket_pushdata_notifications.send(cbor2.dumps(state_update_to_server))

    def get_block_part_tx_hashes(
        self, raw_block_slice: bytes, tx_offsets: "array.ArrayType[int]"
    ) -> tuple[TxHashes, TxHashRows]:
        """Returns both a list of tx hashes and list of tuples containing tx hashes (the same
        data ready for database insertion)"""
        var_int_field_max_size = 9
        max_size_header_plus_tx_count_field = 80 + var_int_field_max_size
        # Is this the first slice of the block? Otherwise adjust the offsets to start at zero
        if tx_offsets[0] > max_size_header_plus_tx_count_field:
            tx_offsets = array.array("Q", map(lambda x: x - tx_offsets[0], tx_offsets))
        partition_tx_hashes = calc_mtree_base_level(0, len(tx_offsets), {}, raw_block_slice, tx_offsets)[0]
        tx_hash_rows = []
        for tx_hashX in partition_tx_hashes:
            # .hex() not hash_to_hex_str() because it's for csv bulk loading
            tx_hash_rows.append((tx_hashX.hex(),))
        return partition_tx_hashes, tx_hash_rows

    def process_work_items(
        self,
        work_items: list[bytes],
        ipc_socket_client: IPCSocketClient,
        db: DBInterface,
    ) -> None:
        process = psutil.Process()
        self.logger.log(
            PROFILING,
            f"Memory usage check before processing work items: " f"{process.memory_info().rss//1024**2}MB",
        )
        blk_hash: bytes
        block_num: int
        is_reorg: bool
        work_item_id: int
        first_tx_pos_batch: int
        part_end_offset: int
        packed_array: bytes
        for packed_msg in work_items:
            msg_type, len_arr = struct.unpack_from("<II", packed_msg)  # get size_array
            (
                msg_type,
                len_arr,
                work_item_id,
                is_reorg,
                blk_hash,
                block_num,
                first_tx_pos_batch,
                part_end_offset,
                packed_array,
            ) = struct.unpack(f"<IIII32sIIQ{len_arr}s", packed_msg)
            tx_offsets_part = array.array("Q", packed_array)
            is_reorg = bool(is_reorg)

            # The first partition should include the 80 byte block header + tx_count varint field
            slice_start_offset = 0 if first_tx_pos_batch == 0 else tx_offsets_part[0]
            slice_end_offset = part_end_offset

            # This can bottleneck when blocks are big and ConduitRaw is on a magnetic HDD
            raw_block_slice_array = ipc_socket_client.block_batched(
                [BlockSliceRequestType(block_num, Slice(slice_start_offset, slice_end_offset))]
            )
            # self.logger.log(
            #     PROFILING,
            #     f"Size of received raw_block slice {blk_hash.hex()[0:8]} (worker-{self.worker_id}): "
            #     f"{(len(raw_block_slice_array) / 1024 ** 2):.2f}MB",
            # )

            is_reorg = bool(is_reorg)
            blk_num, len_slice = struct.unpack_from(f"<IQ", raw_block_slice_array, offset=0)
            blk_num, len_slice, raw_block_slice = struct.unpack_from(
                f"<IQ{len_slice}s", raw_block_slice_array, offset=0
            )
            del raw_block_slice_array  # free memory eagerly
            part_tx_hashes, part_tx_hash_rows = self.get_block_part_tx_hashes(
                raw_block_slice, tx_offsets_part
            )
            # It is wasted disc IO for input and pushdata rows to be inserted again if this has
            # already occurred for the mempool transaction or on a reorg hence we calculate which
            # category each tx belongs in before parsing the transactions.
            # This was a design decision to allow for most of the "heavy lifting" to be done on
            # the mempool txs so that when the confirmation comes, the additional work the db has
            # to do is fairly minimal - this should lead to a more responsive end user experience.

            # new_tx_offsets  # Must not be in an orphaned block or the mempool
            not_new_tx_offsets: set[int] = set(tx_offsets_part)
            map_tx_hashes_to_offsets = dict(zip(part_tx_hashes, tx_offsets_part))
            unprocessed_tx_hashes = db.get_unprocessed_txs(
                is_reorg, part_tx_hash_rows, self.inbound_tx_table_name
            )
            for tx_hash in unprocessed_tx_hashes:
                tx_offset = map_tx_hashes_to_offsets[tx_hash]
                not_new_tx_offsets.remove(tx_offset)
            assert len(tx_offsets_part) - len(unprocessed_tx_hashes) - len(not_new_tx_offsets) == 0

            # Parse and flush
            tx_rows: list[ConfirmedTransactionRow]
            tx_rows_mempool: list[MempoolTransactionRow]
            in_rows_parsed: list[InputRowParsed]
            pd_rows_parsed: list[PushdataRowParsed]

            # Mempool and Reorg txs already have entries for inputs, pushdata and output tables,
            # so we avoid re-inserting these rows a second time (`parse_txs` skips over them)
            #
            # Block 804157 is a great stress test for peak memory allocation here.
            # namely transactions like:
            # - 326a5af37f6c98421aa50c113944912c082036b1ee1c4f26ed291ae8e6733ecf
            # - b685e76fe6cd080dac8d2cde0cb7daf0a4ba26535f784466037a105c99b500ba
            # There are a ton of them like this producing over 68 million p2pkh scripts of 1sat value
            # for one slice of the block i.e. that doesn't even include the full block.
            all_tx_offsets_sorted = array.array("Q", sorted(tx_offsets_part))

            for i, result in enumerate(parse_txs(
                raw_block_slice,
                all_tx_offsets_sorted,
                blk_num,
                True,
                first_tx_pos_batch,
                already_seen_offsets=not_new_tx_offsets,
            )):
                (
                    tx_rows,
                    tx_rows_mempool,
                    in_rows_parsed,
                    pd_rows_parsed,
                    utxo_spends,
                    pushdata_matches_tip_filter,
                ) = result
                pushdata_rows_for_flushing = convert_pushdata_rows_for_flush(pd_rows_parsed)
                input_rows_for_flushing = convert_input_rows_for_flush(in_rows_parsed)
                acks = [ProcessedBlockAck(block_num, work_item_id, blk_hash, part_tx_hashes[i:i+1])]
                self.confirmed_tx_flush_queue.put(
                    (
                        MySQLFlushBatch(
                            tx_rows,
                            tx_rows_mempool,
                            input_rows_for_flushing,
                            pushdata_rows_for_flushing,
                        ),
                        acks,
                        TipFilterNotifications(utxo_spends, pushdata_matches_tip_filter, blk_hash),
                    )
                )
            del raw_block_slice  # free memory eagerly
        self.confirmed_tx_flush_queue.join()
