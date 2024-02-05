# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.

import struct

VERSION = "version"
VERACK = "verack"
PROTOCONF = "protoconf"
INV = "inv"
ADDR = "addr"
GETDATA = "getdata"
NOTFOUND = "notfound"
GETBLOCKS = "getblocks"
GETHEADERS = "getheaders"
TX = "tx"
BLOCK = "block"
HEADERS = "headers"
GETADDR = "getaddr"
MEMPOOL = "mempool"
CHECKORDER = "checkorder"
SUBMITORDER = "submitorder"
REPLY = "reply"
PING = "ping"
PONG = "pong"
REJECT = "reject"
FILTERCLEAR = "filterclear"
FILTERLOAD = "filterload"
MERKELBLOCK = "merkleblock"
SENDHEADERS = "sendheaders"
FEEFILTER = "feefilter"
CMPCTBLOCK = "cmpctblock"
GETBLOCKTXN = "getblocktxn"
BLOCKTXN = "blocktxn"
SENDCMPCT = "sendcmpct"
EXTMSG = "extmsg"
AUTHCH = "authch"

VERSION_BIN = struct.pack("12s", "version".encode("ASCII"))
VERACK_BIN = struct.pack("12s", "verack".encode("ASCII"))
PROTOCONF_BIN = struct.pack("12s", "protoconf".encode("ASCII"))
INV_BIN = struct.pack("12s", "inv".encode("ASCII"))
ADDR_BIN = struct.pack("12s", "addr".encode("ASCII"))
GETDATA_BIN = struct.pack("12s", "getdata".encode("ASCII"))
NOTFOUND_BIN = struct.pack("12s", "notfound".encode("ASCII"))
GETBLOCKS_BIN = struct.pack("12s", "getblocks".encode("ASCII"))
GETHEADERS_BIN = struct.pack("12s", "getheaders".encode("ASCII"))
TX_BIN = struct.pack("12s", "tx".encode("ASCII"))
BLOCK_BIN = struct.pack("12s", "block".encode("ASCII"))
HEADERS_BIN = struct.pack("12s", "headers".encode("ASCII"))
GETADDR_BIN = struct.pack("12s", "getaddr".encode("ASCII"))
MEMPOOL_BIN = struct.pack("12s", "mempool".encode("ASCII"))
CHECKORDER_BIN = struct.pack("12s", "checkorder".encode("ASCII"))
SUBMITORDER_BIN = struct.pack("12s", "submitorder".encode("ASCII"))
REPLY_BIN = struct.pack("12s", "reply".encode("ASCII"))
PING_BIN = struct.pack("12s", "ping".encode("ASCII"))
PONG_BIN = struct.pack("12s", "pong".encode("ASCII"))
REJECT_BIN = struct.pack("12s", "reject".encode("ASCII"))
FILTERCLEAR_BIN = struct.pack("12s", "filterclear".encode("ASCII"))  # ignore filterload & filteradd
FILTERLOAD_BIN = struct.pack("12s", "filterload".encode("ASCII"))
MERKELBLOCK_BIN = struct.pack("12s", "merkleblock".encode("ASCII"))
SENDHEADERS_BIN = struct.pack("12s", "sendheaders".encode("ASCII"))
FEEFILTER_BIN = struct.pack("12s", "feefilter".encode("ASCII"))
GETBLOCKTXN_BIN = struct.pack("12s", "getblocktxn".encode("ASCII"))
CMPCTBLOCK_BIN = struct.pack("12s", "cmpctblock".encode("ASCII"))
BLOCKTXN_BIN = struct.pack("12s", "blocktxn".encode("ASCII"))
SENDCMPCT_BIN = struct.pack("12s", "sendcmpct".encode("ASCII"))
EXTMSG_BIN = struct.pack("12s", "extmsg".encode("ASCII"))
AUTHCH_BIN = struct.pack("12s", "authch".encode("ASCII"))
