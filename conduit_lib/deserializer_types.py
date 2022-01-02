from typing import TypedDict, NamedTuple

from conduit_lib.types import Hash256


class MessageHeader(TypedDict):
    magic: str
    command: str
    length: int
    checksum: str


class NodeAddr(NamedTuple):
    services: int
    ip: str
    port: int


class SendCmpct(NamedTuple):
    enable: bool
    version: int


class NodeAddrListItem(NamedTuple):
    timestamp: str
    node_addr: NodeAddr


class Version(TypedDict):
    version: int
    services: str
    timestamp: str
    addr_recv: NodeAddr
    addr_from: NodeAddr
    nonce: int
    user_agent: str
    start_height: int
    relay: bool


class Protoconf(TypedDict):
    number_of_fields: int
    max_recv_payload_length: int


class Inv(TypedDict):
    inv_type: int
    inv_hash: bytes


class BlockLocator(NamedTuple):
    version: int
    block_locator_hashes: list[Hash256]
    hash_stop: Hash256
