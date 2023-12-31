# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.
import asyncio
import json
import logging
import os
import time
import typing
from typing import Any, TypedDict
from pathlib import Path
from typing import Iterable
from random import random

from bitcoinx import double_sha256, hex_str_to_hash, Header

from .client import BitcoinClient, DEFAULT_LARGE_MESSAGE_LIMIT
from .deserializer import Inv
from .types import BitcoinClientMode, Reject, BroadcastResult
from .utils import cast_to_valid_ipv4, create_task, cancel_tasks

if typing.TYPE_CHECKING:
    from .handlers import HandlersDefault


MODULE_DIR = Path(os.path.dirname(os.path.abspath(__file__)))


class PeerInfo(TypedDict):
    last_connected: int | None  # unix timestamp | None for never connected
    banned: bool


class PeerManagerFailedError(Exception):
    pass


class BitcoinClientManager:
    """This can also manage multiple connections to the same node on the same port to overcome
    latency effects on data transfer rates.
    See: https://en.wikipedia.org/wiki/Bandwidth-delay_product"""

    def __init__(self, message_handler: 'HandlersDefault', peers_list: list[str],
            large_message_limit: int = DEFAULT_LARGE_MESSAGE_LIMIT,
            mode: BitcoinClientMode = BitcoinClientMode.HIGH_LEVEL,
            relay_transactions: bool = True,
            use_persisted_peers: bool = True,
            last_known_height: int = 0,
            concurrency: int = 1,
            wait_for_n_peers: int = 1
    ) -> None:
        self.closing: bool = False
        self._logger = logging.getLogger("conduit.p2p.client.pool")
        self._logger.setLevel(logging.DEBUG)
        self.message_handler = message_handler
        self.message_handler.client_manager = self
        self.net_config = message_handler.net_config
        self.serializer = message_handler.serializer
        self.deserializer = message_handler.deserializer
        self.large_message_limit = large_message_limit
        self.clients: list[BitcoinClient] = []
        self.mode = mode
        self.relay_transactions = relay_transactions
        self.last_added_peer_id = 0
        self.addresses: dict[str, PeerInfo] = {}
        self.use_persisted_peers = use_persisted_peers
        self.last_known_height = last_known_height
        self.concurrency = concurrency
        self.wait_for_n_peers = wait_for_n_peers
        self.peers_dir = Path(os.getenv('CONDUIT_P2P_PEERS_DIR', Path(os.getcwd())))
        self.peers_filepath = self.peers_dir / f'peers.{self.net_config.NET}.json'
        if use_persisted_peers:
            self._logger.debug(f"Using peers file at: {self.peers_filepath}")
            self.addresses = self.read_peers_file()
            for host_string in peers_list:
                if host_string not in self.addresses:
                    self.addresses[host_string] = PeerInfo(last_connected=None, banned=False)
        else:
            self.addresses = dict((host_string, PeerInfo(last_connected=None, banned=False))
                for host_string in peers_list)

        for host_string in self.addresses:
            self.add_client(host_string)

        self._connection_pool: list[BitcoinClient] = []
        self._disconnected_pool: set[BitcoinClient] = set()
        self._currently_selected_peer_index: int = 0
        self._currently_selected_available_peer_index: int = 0

        # On tx broadcast, record acceptance by other peers
        self.tx_inv_queue_map: dict[bytes, list[int]] = {}  # tx_hash -> list[BitcoinClient.id]
        self.tx_inv_queue: asyncio.Queue[tuple[list[Inv], BitcoinClient]] = asyncio.Queue(maxsize=1000)
        self.wanted_blocks: set[bytes] = set()
        self.wanted_block_first: Header | None = None
        self.wanted_block_last: Header | None = None
        self.tasks: list[asyncio.Task[Any]] = []

    async def __aenter__(self) -> "BitcoinClientManager":
        await self.connect_all_peers(self.wait_for_n_peers)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:  # type: ignore
        await self.close()

    def __len__(self) -> int:
        return len(self._connection_pool)

    def read_peers_file(self) -> dict[str, PeerInfo]:
        """Because the key is the host_string, it will automatically de-duplicate"""
        if not self.peers_filepath.exists():
            self.peers_dir.mkdir(parents=True, exist_ok=True)
            self.peers_filepath.touch(exist_ok=True)
        with open(self.peers_filepath, 'r') as file:
            content = file.read()
            if content:
                self.addresses = json.loads(content)
            else:
                self.addresses = {}
        return self.addresses

    def write_peers_file(self) -> None:
        """Completely overwrites the file every time. Low-tech solution.
        Because the key is the host_string, it will automatically de-duplicate"""
        with open(self.peers_filepath, 'w') as file:
            file.write(json.dumps(self.addresses, indent=4))

    def mark_block_done(self, block_hash: bytes) -> None:
        if block_hash in self.wanted_blocks:
            self.wanted_blocks.remove(block_hash)

    def add_client(self, host_string: str) -> BitcoinClient | None:
        if host_string not in self.addresses:
            self.addresses[host_string] = PeerInfo(last_connected=None, banned=False)

        if self.use_persisted_peers:
            self.write_peers_file()

        peer_info = self.addresses[host_string]
        if peer_info['banned']:
            self._logger.info(f"Skipping banned peer at: {host_string}")
            return None

        host, port = host_string.split(":")
        host = cast_to_valid_ipv4(host)  # important for dns resolution of docker container IP addresses
        assert self.concurrency > 0
        have_blocks: set[bytes] = set()  # have_blocks should be shared state for the 'group'
        for i in range(self.concurrency):
            self.last_added_peer_id += 1
            client = BitcoinClient(id=self.last_added_peer_id, remote_host=host, remote_port=int(port),
                message_handler=self.message_handler, mode=self.mode,
                relay_transactions=self.relay_transactions, last_known_height=self.last_known_height,
            )
            client.have_blocks = have_blocks
            self.clients.append(client)
        return client

    def select_next_peer(self) -> None:
        """This will iterate through all clients regardless of whether they are connected or not"""
        self._currently_selected_peer_index += 1
        if self._currently_selected_peer_index == len(self.clients) - 1:
            self._currently_selected_peer_index = 0

    def select_next_available_peer(self) -> None:
        """This will iterate through all available clients"""
        self._currently_selected_available_peer_index += 1
        if self._currently_selected_available_peer_index >= len(self._connection_pool) - 1:
            self._currently_selected_available_peer_index = 0

    def get_connected_peers(self) -> list[BitcoinClient]:
        return self._connection_pool

    def get_disconnected_peers(self) -> set[BitcoinClient]:
        return self._disconnected_pool

    def get_next_peer(self) -> BitcoinClient:
        self.select_next_peer()
        return self.get_current_peer()

    async def get_next_available_peer(self) -> BitcoinClient:
        while len(self._connection_pool) < 1:
            self._logger.info(f"Waiting for peers to connect")
            await asyncio.sleep(1)
        self.select_next_available_peer()
        return self.get_current_available_peer()

    def _reallocate_outstanding_getdatas(self, client: BitcoinClient) -> None:
        """This includes headers, txs, blocks"""
        raise NotImplementedError()

    def get_current_peer(self) -> BitcoinClient:
        """This peer might be disconnected. Requires filtering"""
        return self.clients[self._currently_selected_peer_index]

    def get_current_available_peer(self) -> BitcoinClient:
        """This peer might be disconnected. Requires filtering"""
        return self._connection_pool[self._currently_selected_available_peer_index]

    async def reallocate_work_task(self, client: BitcoinClient) -> None:
        allocated_getdatas = []
        while True:
            try:
                item = client.queued_getdata_requests.get_nowait()
                allocated_getdatas.append(item)
            except asyncio.QueueEmpty:
                break
        # If none are available it will block here and wait until at least
        # 1 peer becomes available
        available_client = await self.get_next_available_peer()
        for item in allocated_getdatas:
            await available_client.queued_getdata_requests.put(item)
            # This will populate the BitcoinClient's have_blocks cache
            if self.wanted_blocks:
                assert self.wanted_block_first is not None
                assert self.wanted_block_last is not None
                message = self.serializer.getblocks(len([self.wanted_block_first.hash]), [self.wanted_block_first.hash],
                    self.wanted_block_last.hash)
                available_client.send_message(message)

    async def connect_task(self, client: BitcoinClient) -> None:
        """On a disconnection, if the BitcoinClient has `queued_getdata_requests` it must
        be re-allocated otherwise the indexing process will hang waiting for the block data
        to be returned that never comes."""
        self._disconnected_pool.add(client)
        sleep_times = [5, 30, 90, 180, 600]  # Sleep 10 minutes for each retry thereafter

        sleep_index = 0
        while True:
            try:
                # raise ConnectionResetError if connection doesn't complete handshake
                await client.connect()
            except ConnectionResetError:  # < -- disconnected
                sleep_index = min(sleep_index + 1, len(sleep_times) - 1)
                self._logger.debug(f"Peer at: {client.remote_host}:{client.remote_port} "
                                   f"unavailable. Retry in {sleep_times[sleep_index]} seconds.")
            except (ConnectionRefusedError, OSError):
                sleep_index = len(sleep_times) - 1
                self._logger.error(f"Peer at: {client.remote_host}:{client.remote_port} "
                                   f"Refused connection. Retry in {sleep_times[sleep_index]} seconds.")
            else:
                sleep_index = 0
                self._logger.info(f"Connected to peer: {client.host_string}")
                self._disconnected_pool.remove(client)
                if client not in self._connection_pool:
                    self._connection_pool.append(client)
                self.addresses[client.host_string]['last_connected'] = int(time.time())
                self.write_peers_file()
                if self.wanted_blocks:
                    # getblocks will populate the BitcoinClient.has_blocks cache
                    # with block hashes to know what it has available of the
                    # ones that we want
                    assert self.wanted_block_first is not None
                    assert self.wanted_block_last is not None
                    message = self.serializer.getblocks(len([self.wanted_block_first.hash]),
                        [self.wanted_block_first.hash], self.wanted_block_last.hash)
                    client.send_message(message)

                await client.connection_lost_event.wait()
                client.connection_lost_event.clear()
                self._logger.info(f"Peer (id={client.id}) at: {client.remote_host}:{client.remote_port} "
                                  f"disconnected gracefully. Retry in {sleep_times[sleep_index]} seconds.")

            # Was connected.
            self._logger.debug(f"Removing disconnected peer {client.id} from connection pool")
            if client in self._connection_pool:
                self._connection_pool.remove(client)
            self._disconnected_pool.add(client)
            if client.queued_getdata_requests.qsize() > 0:
                create_task(self.reallocate_work_task(client))

            await asyncio.sleep(sleep_times[sleep_index])
            client.reset_state()

    async def connect_client(self, client: BitcoinClient) -> None:
        self.tasks.append(create_task(self.connect_task(client)))

    async def connect_all_peers(self, wait_for_n_peers: int = 1) -> None:
        """Should have tolerance for some failed connections to offline peers"""
        self.tasks.append(create_task(self.listen_for_tx_relay_task()))

        for client in self.clients:
            await self.connect_client(client)
            await asyncio.sleep(random()*1)  # avoid thundering herd on the same node

        if wait_for_n_peers:
            waits = 0
            sleep_time = 1
            while len(self._connection_pool) < wait_for_n_peers:
                waits += 1
                await asyncio.sleep(sleep_time)
                if waits > 5:
                    self._logger.info(f"Only {len(self._connection_pool)} of {wait_for_n_peers} connected. Waiting.")
                    sleep_time = 5
                    waits = 0

    async def close(self) -> None:
        self.closing = True
        # Force close all outstanding connections
        peers = []
        for peer in self._connection_pool:
            if not peer.closing:
                peers.append(peer)
        for peer in peers:
            await peer.close()
        await asyncio.sleep(1)
        await cancel_tasks(self.tasks)

    async def listen(self) -> None:
        """Should periodically check for disconnected peers and try to reconnect them on a sensible
        time schedule (frequently at first) and then less frequently."""
        while not self.closing:
            await asyncio.sleep(2)

    async def _concurrent_broadcast(self, rawtx: bytes, wait_time: float,
            broadcast_peer_count: int, check_malformed: bool, available_peers_count: int) -> Iterable[Reject | None]:
        tasks = []
        try:
            # Concurrently broadcast to (up to) `broadcast_peer_count` peers
            # If fewer than this many peers are available, broadcast to as many
            # as are available
            for _ in range(min(broadcast_peer_count, available_peers_count)):
                client = await self.get_next_available_peer()
                task = create_task(client.broadcast_transaction(rawtx, wait_time=wait_time,
                    check_malformed=check_malformed))
                tasks.append(task)

            return await asyncio.gather(*tasks, return_exceptions=False)
        except asyncio.CancelledError:
            await cancel_tasks(tasks)
            return []

    async def listen_for_tx_relay_task(self) -> None:
        """A long-running background task that tracks which peer_ids relayed back to us
        a tx_hash that we broadcasted (to another peer). This provides tangible feedback that
        the transaction has propagated to the mempool of other nodes."""
        # Add peer id to list of `relaying_peer_ids` for the transaction_broadcast
        while not self.closing:
            inv_vect_txs, client = await self.tx_inv_queue.get()
            for inv in inv_vect_txs:
                tx_hash = hex_str_to_hash(inv['inv_hash'])
                if tx_hash in self.tx_inv_queue_map:
                    self.tx_inv_queue_map[tx_hash].append(client.id)

    def queue_message_for_peer(self, message: bytes, client: BitcoinClient) -> None:
        """This should only be used for data fetching requests that can be re-allocated to any other peer
        in the event of a disconnection."""
        client.queued_getdata_requests = asyncio.Queue(maxsize=100)  # New queue
        client.queued_getdata_requests.put_nowait(message)
        client.send_message(message)

    async def broadcast_transaction_and_listen(self, rawtx: bytes, wait_time: float=5.0,
            broadcast_peer_count: int = 1, check_malformed: bool = True) -> BroadcastResult:
        if self.mode != BitcoinClientMode.HIGH_LEVEL:
            raise ValueError("This helper method is only available in BitcoinClientMode.SIMPLE mode")

        available_peers_count = len(self.get_connected_peers())
        if available_peers_count == 0:
            raise ValueError(f"No available peers for broadcast")

        if len(rawtx) == 0:
            rejected: Reject | None = Reject(message='tx', ccode_translation='REJECT_MALFORMED',
                reason='error parsing message', item_hash='')
            return BroadcastResult(rejected=rejected, relaying_peer_ids=[])

        tx_hash = double_sha256(rawtx)
        try:
            if self.tx_inv_queue_map.get(tx_hash):
                raise ValueError("This transaction is already in the process of being broadcast")
            self.tx_inv_queue_map[tx_hash] = []

            results = await self._concurrent_broadcast(rawtx, wait_time, broadcast_peer_count,
                check_malformed, available_peers_count)

            for reject in results:
                if reject is not None:
                    return BroadcastResult(rejected=reject, relaying_peer_ids=[])

            relaying_peer_ids = self.tx_inv_queue_map[tx_hash]
            return BroadcastResult(rejected=None, relaying_peer_ids=relaying_peer_ids)
        finally:
            del self.tx_inv_queue_map[tx_hash]
