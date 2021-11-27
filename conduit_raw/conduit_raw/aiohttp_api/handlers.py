from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

import aiohttp
import bitcoinx
from aiohttp import web
from bitcoinx import hex_str_to_hash, hash_to_hex_str, double_sha256

from conduit_lib.algorithms import calc_depth
from conduit_lib.constants import HashXLength
from conduit_lib.database.lmdb.lmdb_database import LMDB_Database
from conduit_lib.database.mysql.mysql_database import MySQLDatabase
from conduit_lib.types import TransactionQueryResult, TxLocation, RestorationFilterRequest, \
    FILTER_RESPONSE_SIZE, filter_response_struct, RestorationFilterQueryResult, \
    _pack_pushdata_match_response

if TYPE_CHECKING:
    from .server import ApplicationState


logger = logging.getLogger('handlers')


async def ping(request: web.Request) -> web.Response:
    return web.Response(text="true")


async def error(request: web.Request) -> web.Response:
    raise web.HTTPBadRequest(reason="This is a test of raising an exception in the handler")


def _get_tx_metadata(tx_hash: bytes, mysql_db: MySQLDatabase) \
        -> TransactionQueryResult:
    tx_query_result: TransactionQueryResult = mysql_db.api_queries.get_transaction_metadata(
        tx_hash[0:HashXLength])
    if not tx_query_result:
        return
    return tx_query_result


def _get_full_tx_hash(tx_location: TxLocation, lmdb: LMDB_Database):
    # get base level of merkle tree with the tx hashes array
    block_metadata = lmdb.get_block_metadata(tx_location.block_hash)
    base_level = calc_depth(block_metadata.tx_count)

    tx_loc = TxLocation(tx_location.block_hash, tx_location.block_num,
        tx_location.tx_position)
    tx_hash = lmdb.get_tx_hash_by_loc(tx_loc, base_level)
    return tx_hash


async def get_pushdata_filter_matches(request: web.Request):
    """This the main endpoint for the rapid restoration API"""
    app_state: 'ApplicationState' = request.app['app_state']
    mysql_db: MySQLDatabase = app_state.mysql_db
    lmdb: LMDB_Database = app_state.lmdb
    accept_type = request.headers.get('Accept')

    body = await request.content.read()
    if body:
        pushdata_hashes: RestorationFilterRequest = json.loads(body.decode('utf-8'))['filterKeys']
        pushdata_hashXes = [h[0:HashXLength*2].lower() for h in pushdata_hashes]
        pushdata_hashX_map = dict(zip(pushdata_hashXes, pushdata_hashes))
    else:
        return web.Response(status=400)

    if accept_type == 'application/octet-stream':
        headers = {'Content-Type': 'application/octet-stream', 'User-Agent': 'ConduitDB'}
    else:
        headers = {'Content-Type': 'application/json', 'User-Agent': 'ConduitDB'}
    response = aiohttp.web.StreamResponse(status=200, reason='OK', headers=headers)
    await response.prepare(request)

    count = 0
    result_generator = mysql_db.api_queries.get_pushdata_filter_matches(pushdata_hashXes)
    for match in result_generator:
        match: RestorationFilterQueryResult
        # logger.debug(f"Sending {match}")

        # Get Full tx hashes and pushdata hashes for response object
        full_tx_hash = hash_to_hex_str(_get_full_tx_hash(match.tx_location, lmdb))
        full_pushdata_hash = pushdata_hashX_map[match.pushdata_hashX.hex().lower()].lower()
        if match.spend_transaction_hash is not None:
            full_spend_transaction_hash = hash_to_hex_str(_get_full_tx_hash(match.tx_location, lmdb))
        else:
            full_spend_transaction_hash = "00"*32

        if accept_type == 'application/octet-stream':
            response_obj = _pack_pushdata_match_response(
                match, full_tx_hash, full_pushdata_hash,
                full_spend_transaction_hash, json=False)
            packed_match = filter_response_struct.pack(*response_obj)
            await response.write(packed_match)
            count += 1
        else:  # application/json
            response_obj = _pack_pushdata_match_response(
                match, full_tx_hash, full_pushdata_hash,
                full_spend_transaction_hash, json=True)
            row = (json.dumps(response_obj) + "\n").encode('utf-8')
            await response.write(row)

    if accept_type == 'application/octet-stream':
        total_size = count * FILTER_RESPONSE_SIZE
        logger.debug(
            f"Total pushdata filter match response size: {total_size} for count: {count}")
    finalization_flag = b'\x00'
    await response.write(finalization_flag)
    return response


async def get_transaction(request: web.Request) -> web.Response:
    app_state: 'ApplicationState' = request.app['app_state']
    mysql_db: MySQLDatabase = app_state.mysql_db
    lmdb: LMDB_Database = app_state.lmdb
    accept_type = request.headers.get('Accept')

    try:
        txid = request.match_info['txid']
        if not txid:
            raise ValueError('no txid submitted')

        tx_metadata = _get_tx_metadata(hex_str_to_hash(txid), mysql_db)
        if not tx_metadata:
            web.Response(status=404)

        tx_location = TxLocation(tx_metadata.block_hash, tx_metadata.block_num,
            tx_metadata.tx_position)
        rawtx = lmdb.get_rawtx_by_loc(tx_location)
        # logger.debug(f"Sending rawtx for tx_hash: {hash_to_hex_str(double_sha256(rawtx))}")
    except ValueError:
        return web.Response(status=400)

    if accept_type == 'application/octet-stream':
        return web.Response(body=rawtx)
    else:
        return web.json_response(data=rawtx.hex())
