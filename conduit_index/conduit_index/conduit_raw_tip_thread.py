import logging
import os
import threading
import time
from os import urandom

import asyncio
import bitcoinx
from bitcoinx import double_sha256
from confluent_kafka.cimpl import Consumer

from conduit_lib.store import Storage
from .sync_state import SyncState


class ConduitRawTipThread(threading.Thread):
    """Connects to multiprocessing.SyncManager server to synchronize state of ConduitRaw headers"""

    def __init__(self, sync_state: SyncState, storage: Storage):
        threading.Thread.__init__(self, daemon=True)
        self.logger = logging.getLogger("sync-conduit-raw-tip")
        self.sync_state = sync_state
        self.storage = storage

        # auto.offset.reset is set to 'earliest' which should be fine because the retention period
        # will be 24 hours in which case old headers will be removed from the topic in time
        group = urandom(8)  # will give a pub/sub arrangement for all consumers
        self.headers_state_consumer: Consumer = Consumer({
            'bootstrap.servers': os.environ.get('KAFKA_HOST', "127.0.0.1:26638"),
            'group.id': group,
            'auto.offset.reset': 'earliest'
        })
        self.headers_state_consumer.subscribe(['conduit-raw-headers-state'])
        self.loop = asyncio.get_event_loop()

    def convert_to_header_obj(self, tip_raw: bytes):
        while True:
            try:
                tip_raw = tip_raw
                tip_hash = double_sha256(tip_raw)
                tip_height = self.storage.get_header_for_hash(tip_hash).height
                deserialized_tip = bitcoinx.Header(*bitcoinx.unpack_header(tip_raw), tip_hash, tip_raw,
                    tip_height)
                return deserialized_tip
            except bitcoinx.MissingHeader:
                time.sleep(2)
                self.logger.debug(f"ConduitRaw has a header we do not yet have: Syncing headers...")

    def run(self):
        try:
            while True:
                for new_raw_tip in self.headers_state_consumer.consume():
                    new_tip = self.convert_to_header_obj(new_raw_tip.value())
                    self.sync_state.set_conduit_raw_header_tip(new_tip)
                    # self.logger.debug(f"new_tip.height={new_tip.height}")

                    if self.sync_state.get_local_block_tip_height() < new_tip.height:
                        # self.logger.debug(f"putting to async queue {new_tip.height}")
                        asyncio.run_coroutine_threadsafe(
                            self.sync_state.conduit_raw_headers_queue.put(new_tip), self.loop)
                    else:
                        # self.logger.debug(f"ConduitIndex has already synced this tip at "
                        #     f"height: {new_tip.height} - skipping...")
                        pass  # drain topic until we get to the last message (actual tip)
        except ConnectionResetError:
            time.sleep(2)
            self.logger.debug("Retrying connection to ConduitRaw HeadersStateServer")
            self.run()
