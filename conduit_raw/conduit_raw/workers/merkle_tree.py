import array
import logging
import multiprocessing
import sys
import threading
import time

import cbor2
import zmq
from bitcoinx import double_sha256

from conduit_lib.database.lmdb.lmdb_database import LMDB_Database
from conduit_lib.database.lmdb.types import MerkleTreeRow
from conduit_lib.logging_client import setup_tcp_logging

from conduit_lib.algorithms import build_mtree_from_base, calc_depth, unpack_varint
from conduit_lib.types import MultiprocessingQueue


class MTreeCalculator(multiprocessing.Process):
    """This worker type is a multiprocessing.Process because building merkle trees requires a ton of
    sha256 hashing which we do not want on the main asyncio event loop of the controller

    There are two types of messages corresponding to two different `threading.Thread` threads.:
    - BIG_BLOCK
        - These are streamed and processed in chunks equivalent to the BitcoinP2PClient
        receive buffer
    - SMALL_BLOCK
        - These are streamed and processed wholesale as complete raw block messages over zmq
        - They are concatenated together and written to spinning HDD in batches (multiple
        raw blocks to a single file) to avoid the HDD 'stuttering' from too many IOPS.
    """

    def __init__(
        self, worker_id: int, worker_ack_queue_mtree: MultiprocessingQueue[bytes]) -> None:
        super(MTreeCalculator, self).__init__()
        self.worker_id = worker_id
        self.worker_ack_queue_mtree = worker_ack_queue_mtree
        self.logger = logging.getLogger(f"merkle-tree={self.worker_id}")

        self.lmdb: LMDB_Database | None = None

        self.BATCHING_RATE = 0.3
        self.tx_hashes_map: dict[bytes, bytearray] = {}
        self.tx_offsets_map: dict[bytes, array.ArrayType[int]] = {}
        self.tx_count_map: dict[bytes, int] = {}  # purely for checking data integrity

        self.batched_merkle_trees: list[MerkleTreeRow] = []
        self.batched_acks: list[bytes] = []

    def process_merkle_tree_batch(self, batch: list[bytes], lmdb: LMDB_Database) -> None:
        """If the batch is for a 'BIG BLOCK' then it will accumulate the tx_hashes and counts
        in `tx_hashes_map` and `tx_count_map` and wait for subsequent batches until it receives
        the last batch where `chunk_num == num_of_chunks`."""
        t0 = time.perf_counter()
        for packed_msg in batch:
            chunk_num, num_of_chunks, blk_hash, tx_offsets_bytes_for_chunk, raw_block_chunk = \
                cbor2.loads(packed_msg)

            tx_offsets_for_chunk = array.array("Q", tx_offsets_bytes_for_chunk)

            if not self.tx_hashes_map.get(blk_hash):
                # self.logger.debug(f"Got new block: {hash_to_hex_str(blk_hash)}")
                self.tx_hashes_map[blk_hash] = bytearray()

            if chunk_num == 1:
                total_tx_count, _ = unpack_varint(raw_block_chunk[80:89], offset=0)
                self.tx_count_map[blk_hash] = total_tx_count

            for i in range(len(tx_offsets_for_chunk)):
                start_offset = tx_offsets_for_chunk[i]
                if i == (len(tx_offsets_for_chunk) - 1):  # last tx in chunk
                    rawtx = raw_block_chunk[start_offset:]
                else:  # last tx in chunk
                    end_offset = tx_offsets_for_chunk[i + 1]
                    rawtx = raw_block_chunk[start_offset:end_offset]
                tx_hash = double_sha256(rawtx)
                self.tx_hashes_map[blk_hash] += tx_hash

            def tx_hashes_to_list(tx_hashes: bytearray) -> list[bytes]:
                return [tx_hashes[i*32:(i+1)*32] for i in range(tx_count)]

            # Final chunk received
            if chunk_num == num_of_chunks:
                tx_count = self.tx_count_map[blk_hash]
                tx_hashes = self.tx_hashes_map[blk_hash]
                base_level_index = calc_depth(leaves_count=tx_count) - 1
                mtree = {base_level_index: tx_hashes_to_list(tx_hashes)}
                mtree = build_mtree_from_base(base_level_index, mtree)
                self.batched_merkle_trees.append(MerkleTreeRow(blk_hash, mtree, tx_count))
                self.batched_acks.append(blk_hash)

        # self.logger.debug(f"batched_merkle_trees={batched_merkle_trees}")
        if self.batched_merkle_trees:
            lmdb.put_merkle_trees(self.batched_merkle_trees)
            t1 = time.perf_counter() - t0
            self.logger.debug(f"mtree & transaction offsets batch flush took {t1} seconds")
            self.batched_merkle_trees = []

        if self.batched_acks:
            for blk_hash in self.batched_acks:
                self.worker_ack_queue_mtree.put(blk_hash)
            self.batched_acks = []

    def run(self) -> None:
        self.zmq_context = zmq.Context[zmq.Socket[bytes]]()
        if sys.platform == "win32":
            setup_tcp_logging(port=54545)
        self.logger.setLevel(logging.DEBUG)
        self.logger.info(f"Starting {self.__class__.__name__}...")

        # PUB-SUB from Controller to worker to kill the worker
        self.kill_worker_socket = self.zmq_context.socket(zmq.SUB)
        self.kill_worker_socket.connect("tcp://127.0.0.1:46464")
        self.kill_worker_socket.setsockopt(zmq.SUBSCRIBE, b"stop_signal")

        t1 = threading.Thread(target=self.kill_thread, daemon=True)
        t1.start()

        lmdb = LMDB_Database(lock=True)

        batch = []
        prev_time_check = time.time()

        BASE_PORT_NUM = 41830
        self.merkle_tree_socket = self.zmq_context.socket(zmq.PULL)
        self.merkle_tree_socket.connect(f"tcp://127.0.0.1:{BASE_PORT_NUM + self.worker_id}")
        self.logger.debug(f"Connected merkle tree ZMQ socket on: "
                          f"tcp://127.0.0.1:{BASE_PORT_NUM + self.worker_id}")
        try:
            while True:
                try:
                    if self.merkle_tree_socket.poll(1000, zmq.POLLIN):
                        packed_msg = self.merkle_tree_socket.recv(zmq.NOBLOCK)
                        if not packed_msg:
                            return  # poison pill stop command
                        batch.append(bytes(packed_msg))
                    else:
                        time_diff = time.time() - prev_time_check
                        if time_diff > self.BATCHING_RATE:
                            prev_time_check = time.time()
                            if batch:
                                self.process_merkle_tree_batch(batch, lmdb)
                            batch = []
                except zmq.error.Again:
                    self.logger.debug(f"zmq.error.Again")
                    continue
        except KeyboardInterrupt:
            # This will get logged by the multi-processing if we raise it. The expected behaviour
            # is therefore that we catch this and recognise it as an exit condition.
            return
        except Exception as e:
            self.logger.exception("Caught exception")
        finally:
            self.logger.info("Closing merkle tree Process")
            try:
                self.kill_worker_socket.close()
                self.merkle_tree_socket.close()
            except Exception:
                self.logger.exception("Caught exception")

    def kill_thread(self) -> None:
        try:
            while True:
                message = self.kill_worker_socket.recv()
                if message == b"stop_signal":
                    self.logger.info(f"Process Stopped")
                    break
                time.sleep(0.2)
        except KeyboardInterrupt:
            # This will get logged by the multi-processing if we raise it. The expected behaviour
            # is therefore that we catch this and recognise it as an exit condition.
            return
        except Exception as e:
            self.logger.exception("Caught exception")
        finally:
            try:
                self.kill_worker_socket.close()
                self.merkle_tree_socket.close()
            except Exception:
                self.logger.exception("Caught exception")
            self.logger.info(f"MTreeCalculator process stopped")
