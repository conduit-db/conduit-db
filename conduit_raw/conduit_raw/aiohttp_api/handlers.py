from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

import aiohttp
import bitcoinx
from aiohttp import web
from bitcoinx import hex_str_to_hash, hash_to_hex_str

from conduit_lib.algorithms import calc_depth
from conduit_lib.constants import HashXLength
from conduit_lib.database.lmdb.lmdb_database import LMDB_Database
from conduit_lib.database.mysql.mysql_database import MySQLDatabase
from conduit_lib.types import TxMetadata, TxLocation, RestorationFilterRequest, \
    FILTER_RESPONSE_SIZE, filter_response_struct, RestorationFilterQueryResult, \
    _pack_pushdata_match_response, tsc_merkle_proof_json_to_binary, BlockHeaderRow

if TYPE_CHECKING:
    from .server import ApplicationState


logger = logging.getLogger('handlers')


async def ping(request: web.Request) -> web.Response:
    return web.Response(text="true")


async def error(request: web.Request) -> web.Response:
    raise web.HTTPBadRequest(reason="This is a test of raising an exception in the handler")


# @functools.lru_cache(maxsize=256)  # on reorg need to call _get_tx_metadata.cache_clear()
def _get_tx_metadata(tx_hash: bytes, mysql_db: MySQLDatabase) \
        -> TxMetadata:
    """Truncates full hash -> hashX length"""
    tx_metadata: TxMetadata = \
        mysql_db.api_queries.get_transaction_metadata_hashX(tx_hash[0:HashXLength])
    if not tx_metadata:
        return
    return tx_metadata


def _get_full_tx_hash(tx_location: TxLocation, lmdb: LMDB_Database):
    # get base level of merkle tree with the tx hashes array
    block_metadata = lmdb.get_block_metadata(tx_location.block_hash)
    base_level = calc_depth(block_metadata.tx_count) - 1

    tx_loc = TxLocation(tx_location.block_hash, tx_location.block_num,
        tx_location.tx_position)
    tx_hash = lmdb.get_tx_hash_by_loc(tx_loc, base_level)
    return tx_hash


def _get_tsc_merkle_proof(tx_metadata: TxMetadata, mysql_db: MySQLDatabase, lmdb: LMDB_Database,
        include_full_tx: bool=True, target_type: str="hash") -> dict:
    """
    Return a pair (tsc_proof, cost) where tsc_proof is a dictionary with fields:
        index - the position of the transaction
        txOrId - if True returns full rawtx
        target - either "hash", "header" or "merkleroot"
        nodes - the nodes in the merkle branch excluding the "target
    """
    tsc_proof = {}

    # Merkle Branch + Root
    merkle_branch, merkle_root = lmdb.get_merkle_branch(tx_metadata)

    # Txid or Raw Transaction
    tx_location = TxLocation(tx_metadata.block_hash, tx_metadata.block_num, tx_metadata.tx_position)
    if include_full_tx:
        txid_or_tx_field = lmdb.get_rawtx_by_loc(tx_location).hex()
    else:
        txid_or_tx_field = hash_to_hex_str(_get_full_tx_hash(tx_location, lmdb))

    # Target Type
    if target_type == 'header':
        header_row: BlockHeaderRow = mysql_db.api_queries.get_header_data(tx_metadata.block_hash,
            raw_header_data=True)
        target = header_row.block_header  # as hex
    elif target_type == 'merkleroot':
        target = merkle_root
    else:  # target == 'hash'
        target = hash_to_hex_str(tx_metadata.block_hash)

    # Sanity check - Todo: remove when satisfied
    header_row: BlockHeaderRow = mysql_db.api_queries.get_header_data(tx_metadata.block_hash,
        raw_header_data=True)
    root_from_header = bytes.fromhex(header_row.block_header)[36:36 + 32]
    if target_type == 'merkleroot' and merkle_root != hash_to_hex_str(root_from_header):
        logger.debug(f"merkleroot: {merkle_root}; root_from_header: {root_from_header} ")
        raise ValueError("Merkle root does not match expected value from header")

    tsc_proof['index'] = tx_metadata.tx_position
    tsc_proof['txOrId'] = txid_or_tx_field
    tsc_proof['target'] = target
    tsc_proof['nodes'] = merkle_branch
    if target_type != 'hash':
        tsc_proof['targetType'] = target_type
    return tsc_proof


async def get_pushdata_filter_matches(request: web.Request):
    """This the main endpoint for the rapid restoration API"""
    app_state: 'ApplicationState' = request.app['app_state']
    mysql_db: MySQLDatabase = app_state.mysql_db
    lmdb: LMDB_Database = app_state.lmdb
    accept_type = request.headers.get('Accept')

    try:
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

        count = 0
        result_generator = mysql_db.api_queries.get_pushdata_filter_matches(pushdata_hashXes)
        for match in result_generator:
            if count == 0:
                response = aiohttp.web.StreamResponse(status=200, reason='OK', headers=headers)
                await response.prepare(request)

            match: RestorationFilterQueryResult
            # logger.debug(f"Sending {match}")

            # Get Full tx hashes and pushdata hashes for response object

            full_tx_hash = hash_to_hex_str(_get_full_tx_hash(match.tx_location, lmdb))
            full_pushdata_hash = pushdata_hashX_map[match.pushdata_hashX.hex().lower()].lower()
            if match.spend_transaction_hash is not None:
                tx_metadata = _get_tx_metadata(match.spend_transaction_hash, mysql_db)
                spend_tx_loc = TxLocation(
                    block_hash=tx_metadata.block_hash,
                    block_num=tx_metadata.block_num,
                    tx_position=tx_metadata.tx_position)
                full_spend_transaction_hash = hash_to_hex_str(_get_full_tx_hash(spend_tx_loc, lmdb))
            else:
                full_spend_transaction_hash = None

            if accept_type == 'application/octet-stream':
                response_obj = _pack_pushdata_match_response(
                    match, full_tx_hash, full_pushdata_hash,
                    full_spend_transaction_hash, json=False)
                packed_match = filter_response_struct.pack(*response_obj)
                await response.write(packed_match)
            else:  # application/json
                response_obj = _pack_pushdata_match_response(
                    match, full_tx_hash, full_pushdata_hash,
                    full_spend_transaction_hash, json=True)
                row = (json.dumps(response_obj) + "\n").encode('utf-8')
                await response.write(row)
            count += 1

        if count == 0:
            return web.HTTPNotFound(reason="No pushdata matches found")

        if accept_type == 'application/octet-stream':
            total_size = count * FILTER_RESPONSE_SIZE
            logger.debug(
                f"Total pushdata filter match response size: {total_size} for count: {count}")
        finalization_flag = b'\x00'
        await response.write(finalization_flag)
        return response
    except Exception:
        # Todo - maybe we need a flag to indicate an error occurred mid-way through streaming
        logger.exception("Unexpected exception in get_pushdata_filter_matches")
        return web.HTTPInternalServerError()


async def get_transaction(request: web.Request) -> web.Response:
    app_state: 'ApplicationState' = request.app['app_state']
    mysql_db: MySQLDatabase = app_state.mysql_db
    lmdb: LMDB_Database = app_state.lmdb
    accept_type = request.headers.get('Accept')
    txid = request.match_info['txid']
    if not txid:
        raise web.HTTPBadRequest(reason='no txid submitted')

    tx_metadata = _get_tx_metadata(hex_str_to_hash(txid), mysql_db)
    if not tx_metadata:
        raise web.HTTPNotFound(reason="tx_metadata not found")

    tx_location = TxLocation(tx_metadata.block_hash, tx_metadata.block_num,
        tx_metadata.tx_position)

    if not tx_location:
        raise web.HTTPNotFound(reason="tx location not found")

    rawtx = lmdb.get_rawtx_by_loc(tx_location)
    # logger.debug(f"Sending rawtx for tx_hash: {hash_to_hex_str(double_sha256(rawtx))}")

    if accept_type == 'application/octet-stream':
        return web.Response(body=rawtx)
    else:
        return web.json_response(data=rawtx.hex())


async def get_tsc_merkle_proof(request: web.Request) -> web.Response:
    app_state: 'ApplicationState' = request.app['app_state']
    mysql_db: MySQLDatabase = app_state.mysql_db
    lmdb: LMDB_Database = app_state.lmdb
    accept_type = request.headers.get('Accept')

    txid = request.match_info['txid']
    include_full_tx = False
    target_type = 'hash'
    body = await request.content.read()
    if body:
        json_body = json.loads(body.decode('utf-8'))
        include_full_tx = json_body.get('includeFullTx')
        target_type = json_body.get('targetType')
        if include_full_tx is not None and include_full_tx not in {True, False}:
            raise web.HTTPBadRequest(reason="includeFullTx needs to be a boolean value")

        if target_type is not None and target_type not in {'hash', 'header', 'merkleroot'}:
            raise web.HTTPBadRequest(reason="target type needs to be one of: 'hash', 'header' or"
                                            " 'merkleroot'")

    # Construct JSON format TSC merkle proof
    tx_hash = bitcoinx.hex_str_to_hash(txid)
    tx_metadata = _get_tx_metadata(tx_hash, mysql_db)
    if not tx_metadata:
        raise web.HTTPNotFound(reason="transaction metadata not found")
    tsc_merkle_proof = _get_tsc_merkle_proof(tx_metadata, mysql_db, lmdb,
        include_full_tx, target_type)

    if accept_type == 'application/octet-stream':
        binary_response = tsc_merkle_proof_json_to_binary(tsc_merkle_proof,
            include_full_tx=include_full_tx,
            target_type=target_type)
        return web.Response(body=binary_response)
    else:
        return web.json_response(data=tsc_merkle_proof)
