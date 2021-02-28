import logging
import threading
import time
from typing import Tuple

import asyncio
import bitcoinx

from conduit_lib.headers_state_client import HeadersStateClient
from .sync_state import SyncState


class ConduitRawTipThread(threading.Thread):
    """Connects to multiprocessing.SyncManager server to synchronize state of ConduitRaw headers"""

    def __init__(self, sync_state: SyncState, headers_state_client: HeadersStateClient):
        threading.Thread.__init__(self, daemon=True)
        self.logger = logging.getLogger("sync-conduit-raw-tip")
        self.sync_state = sync_state
        self.headers_state_client = headers_state_client
        self.loop = asyncio.get_event_loop()

    def convert_to_header_obj(self, new_tip: Tuple):
        tip_raw = new_tip[0]
        tip_hash = new_tip[1]
        tip_height = new_tip[2]
        deserialized_tip = bitcoinx.Header(*bitcoinx.unpack_header(tip_raw), tip_hash, tip_raw,
            tip_height)
        return deserialized_tip

    def run(self):
        self.headers_state_client.connect_or_keep_trying()  # Blocking
        self.sync_state.headers_queue = self.headers_state_client.get_remote_headers_queue()
        while True:
            # Todo - this starts spinning if ConduitRaw DCs...
            new_tip = self.sync_state.headers_queue.get()
            # self.logger.debug(f"new_tip.height={new_tip.height}")
            if self.sync_state.get_local_block_tip_height() < new_tip.height:
                # self.logger.debug(f"putting to async queue {new_tip.height}")
                asyncio.run_coroutine_threadsafe(self.sync_state.headers_queue_async.put(new_tip),
                    self.loop)
            else:
                pass  # drain
            # time.sleep(0.1)  # otherwise a forceful DC from ConduitIndex causes spinning...
