# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.

import bitcoinx
from aiohttp import web
from bitcoinx import hash_to_hex_str, hex_str_to_hash, Header, Chain
import logging
import typing

from .types import HeaderJSONType, HeaderTipJSONType, HeaderTipState

if typing.TYPE_CHECKING:
    from .server import ApplicationState

logger = logging.getLogger("handlers-headers")


async def get_header(request: web.Request) -> web.Response:
    app_state: ApplicationState = request.app["app_state"]
    headers_threadsafe = app_state.headers_threadsafe

    accept_type = request.headers.get("Accept", "application/json")
    blockhash = request.match_info.get("hash")
    if not blockhash:
        raise web.HTTPBadRequest(reason=f"'hash' path parameter not supplied")

    if len(blockhash) != 64:
        raise web.HTTPBadRequest(reason=f"'hash' path parameter must be 32 hex bytes in length")

    try:
        header = headers_threadsafe.get_header_for_hash(hex_str_to_hash(blockhash))
    except bitcoinx.MissingHeader:
        raise web.HTTPNotFound()

    if accept_type == "application/octet-stream":
        response_headers = {
            "Content-Type": "application/octet-stream",
            "User-Agent": "ESV-Ref-Server",
        }
        return web.Response(body=header.raw, status=200, reason="OK", headers=response_headers)

    response_headers = {"User-Agent": "ESV-Ref-Server"}
    header_json = HeaderJSONType(
        hash=hash_to_hex_str(header.hash),
        version=header.version,
        prevBlockHash=hash_to_hex_str(header.prev_hash),
        merkleRoot=hash_to_hex_str(header.merkle_root),
        creationTimestamp=header.timestamp,
        difficultyTarget=header.bits,
        nonce=header.nonce,
        transactionCount=0,
        work=header.work(),
    )
    return web.json_response(header_json, status=200, reason="OK", headers=response_headers)


async def get_headers_by_height(request: web.Request) -> web.Response:
    app_state: ApplicationState = request.app["app_state"]
    headers_threadsafe = app_state.headers_threadsafe

    accept_type = request.headers.get("Accept", "application/json")
    params = request.rel_url.query
    height = int(params.get("height", "0"))
    count = int(params.get("count", "1"))
    available_count = min(headers_threadsafe.tip().height - height + 1, count)
    if accept_type == "application/octet-stream":
        response_headers = {
            "Content-Type": "application/octet-stream",
            "User-Agent": "ESV-Ref-Server",
        }
        headers_bytearray = bytearray()
        for height in range(height, height + available_count):
            header = headers_threadsafe.get_header_for_height(height)
            headers_bytearray += header.raw
        return web.Response(
            body=headers_bytearray,
            status=200,
            reason="OK",
            headers=response_headers,
        )

    # else: application/json
    headers_json_list = []
    response_headers = {"User-Agent": "ESV-Ref-Server"}
    for height in range(height, height + available_count):
        header = headers_threadsafe.get_header_for_height(height)
        hash: str
        version: int
        prevBlockHash: str
        merkleRoot: str
        creationTimestamp: int
        difficultyTarget: int
        nonce: int
        transactionCount: int
        work: int
        headers_json_list.append(
            HeaderJSONType(
                hash=hash_to_hex_str(header.hash),
                version=header.version,
                prevBlockHash=hash_to_hex_str(header.prev_hash),
                merkleRoot=hash_to_hex_str(header.merkle_root),
                creationTimestamp=header.timestamp,
                difficultyTarget=header.bits,
                nonce=header.nonce,
                transactionCount=0,
                work=header.work(),
            )
        )
    return web.json_response(headers_json_list, status=200, reason="OK", headers=response_headers)


async def get_chain_tips(request: web.Request) -> web.Response:
    app_state: ApplicationState = request.app["app_state"]
    headers_threadsafe = app_state.headers_threadsafe
    accept_type = request.headers.get("Accept", "application/json")
    if accept_type not in {"application/json", "*/*"}:
        raise web.HTTPNotAcceptable(reason="Can only give an 'application/json response type")

    params = request.rel_url.query
    longest_chain = int(params.get("longest_chain", "0"))
    response_headers = {"User-Agent": "ESV-Ref-Server"}

    chain: Chain
    tips = []
    for chain in headers_threadsafe.headers.chains():
        if chain.tip().hash == headers_threadsafe.headers.longest_chain().tip().hash:
            state = HeaderTipState.LONGEST_CHAIN
        else:
            state = HeaderTipState.STALE
            if longest_chain:
                continue  # skip other STALE chains if `longest_chain` query param == 1
        tip_header: Header = chain.tip()
        tip_header_json = HeaderJSONType(
            hash=hash_to_hex_str(tip_header.hash),
            version=tip_header.version,
            prevBlockHash=hash_to_hex_str(tip_header.prev_hash),
            merkleRoot=hash_to_hex_str(tip_header.merkle_root),
            creationTimestamp=tip_header.timestamp,
            difficultyTarget=tip_header.bits,
            nonce=tip_header.nonce,
            transactionCount=0,
            work=tip_header.work(),
        )
        chain_work = headers_threadsafe.chain_work_for_chain_and_height(chain, tip_header.height)
        chainWork: int
        height: int
        confirmations: int

        tip_response_json = HeaderTipJSONType(
            header=tip_header_json,
            state=state,
            chainWork=chain_work,
            height=tip_header.height,
        )
        tips.append(tip_response_json)
    return web.json_response(tips, status=200, reason="OK", headers=response_headers)
