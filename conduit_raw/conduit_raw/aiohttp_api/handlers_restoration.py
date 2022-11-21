import asyncio
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from enum import IntEnum
from typing import cast, TYPE_CHECKING

import MySQLdb
import bitcoinx
from aiohttp import web
from aiohttp.web_response import StreamResponse
from bitcoinx import hex_str_to_hash, hash_to_hex_str

from conduit_lib.constants import HashXLength
from conduit_lib.database.lmdb.lmdb_database import LMDB_Database
from conduit_lib.database.mysql.mysql_database import MySQLDatabase
from conduit_lib.types import TxMetadata, TxLocation, RestorationFilterRequest, \
    FILTER_RESPONSE_SIZE, filter_response_struct, tsc_merkle_proof_json_to_binary, BlockHeaderRow, \
    TSCMerkleProof, _pack_pushdata_match_response_bin, _pack_pushdata_match_response_json
from conduit_lib.utils import address_to_pushdata_hash
from .mysql_db_tip_filtering import _get_full_tx_hash, _get_tx_metadata_async

if TYPE_CHECKING:
    from .server import ApplicationState


logger = logging.getLogger('handlers')


class MatchFormat(IntEnum):
    PUSHDATA = 1 << 0
    P2PKH = 1 << 1


async def ping(request: web.Request) -> web.Response:
    return web.Response(text="true")


async def error(request: web.Request) -> web.Response:
    raise web.HTTPBadRequest(reason="This is a test of raising an exception in the handler")


def _get_tsc_merkle_proof(tx_metadata: TxMetadata, mysql_db: MySQLDatabase, lmdb: LMDB_Database,
        include_full_tx: bool=True, target_type: str="hash") -> TSCMerkleProof:
    """
    Return a pair (tsc_proof, cost) where tsc_proof is a dictionary with fields:
        index - the position of the transaction
        txOrId - if True returns full rawtx
        target - either "hash", "header" or "merkleroot"
        nodes - the nodes in the merkle branch excluding the "target
    """
    # Merkle Branch + Root
    result = lmdb.get_merkle_branch(tx_metadata)
    assert result is not None
    merkle_branch, merkle_root = result

    # Txid or Raw Transaction
    tx_location = TxLocation(tx_metadata.block_hash, tx_metadata.block_num, tx_metadata.tx_position)
    if include_full_tx:
        rawtx = lmdb.get_rawtx_by_loc(tx_location)
        assert rawtx is not None
        txid_or_tx_field = rawtx.hex()
    else:
        txid_or_tx_field = hash_to_hex_str(_get_full_tx_hash(tx_location, lmdb))

    # Sanity check - Todo: remove when satisfied
    header_row = cast(BlockHeaderRow, mysql_db.api_queries.get_header_data(tx_metadata.block_hash,
        raw_header_data=True))
    root_from_header: bytes = bytes.fromhex(header_row.block_header)[36:36 + 32]
    if target_type == 'merkleroot' and merkle_root != hash_to_hex_str(root_from_header):
        logger.debug(f"merkleroot: {merkle_root}; "
                     f"root_from_header: {hash_to_hex_str(root_from_header)} ")
        raise ValueError("Merkle root does not match expected value from header")

    # Target Type
    if target_type == 'header':
        target = header_row.block_header  # as hex
    elif target_type == 'merkleroot':
        target = merkle_root
    else:  # target == 'hash'
        target = hash_to_hex_str(tx_metadata.block_hash)

    return TSCMerkleProof(
        index=tx_metadata.tx_position,
        txOrId=txid_or_tx_field,
        target=target,
        nodes=merkle_branch,
        targetType=target_type
    )


async def _get_tsc_merkle_proof_async(executor: ThreadPoolExecutor,
        tx_metadata: TxMetadata, mysql_db: MySQLDatabase, lmdb: LMDB_Database,
        include_full_tx: bool = True, target_type: str = "hash") -> TSCMerkleProof:
    tsc_merkle_proof = await asyncio.get_running_loop().run_in_executor(executor,
        _get_tsc_merkle_proof, tx_metadata, mysql_db, lmdb, include_full_tx, target_type)
    return tsc_merkle_proof


async def _get_pushdata_filter_matches(request: web.Request, match_format: MatchFormat) -> StreamResponse:
    try:
        app_state: 'ApplicationState' = request.app['app_state']
        mysql_db: MySQLDatabase = app_state.mysql_db
        lmdb: LMDB_Database = app_state.lmdb
        accept_type = request.headers.get('Accept')
        body = await request.content.read()
        if body:
            request_body: RestorationFilterRequest = json.loads(body.decode('utf-8'))
            if match_format & MatchFormat.PUSHDATA:
                pushdata_hashes: list[str] = request_body['filterKeys']

            # Conversion to the universal MatchFormat.PUSHDATA format
            elif match_format & MatchFormat.P2PKH:
                p2pkh_addresses: list[str] = request_body['filterKeys']
                pushdata_hashes = []
                for p2pkh_address in p2pkh_addresses:
                    try:
                        pushdata_hash = address_to_pushdata_hash(p2pkh_address,
                            app_state.BITCOINX_COIN)
                    except (ValueError, bitcoinx.Base58Error):
                        return web.HTTPBadRequest(reason=f"bad address '{p2pkh_address}'")
                    pushdata_hashes.append(pushdata_hash.hex())
            else:
                return web.HTTPInternalServerError(
                    reason='Used a match format that was not recognized')
        else:
            return web.HTTPBadRequest(reason="empty body")

        if accept_type == 'application/octet-stream':
            headers = {'Content-Type': 'application/octet-stream', 'User-Agent': 'ConduitDB'}
        else:
            headers = {'Content-Type': 'application/json', 'User-Agent': 'ConduitDB'}

        pushdata_hashXes = [h[0:HashXLength * 2].lower() for h in pushdata_hashes]
        pushdata_hashX_map = dict(zip(pushdata_hashXes, pushdata_hashes))

        count = 0
        try:
            result_generator = mysql_db.api_queries.get_pushdata_filter_matches(pushdata_hashXes)
        except MySQLdb.OperationalError:
            # I have only seen this when MySQL is on spinning HDD during the midst of initial
            # block download when it is under heavy strain
            return web.HTTPServiceUnavailable(reason="Database is potentially overloaded at present")

        response: StreamResponse | None = None
        for match in result_generator:
            if count == 0:
                response = StreamResponse(status=200, reason='OK', headers=headers)
                await response.prepare(request)
            assert response is not None

            # logger.debug(f"Sending {match}")

            # Get Full tx hashes and pushdata hashes for response object
            full_tx_hash = hash_to_hex_str(_get_full_tx_hash(match.tx_location, lmdb))
            assert full_tx_hash is not None
            full_pushdata_hash = pushdata_hashX_map[match.pushdata_hashX.hex().lower()].lower()
            if match.spend_transaction_hash is not None:
                tx_metadata = await _get_tx_metadata_async(
                    match.spend_transaction_hash, mysql_db, app_state.executor)
                assert tx_metadata is not None
                spend_tx_loc = TxLocation(
                    block_hash=tx_metadata.block_hash,
                    block_num=tx_metadata.block_num,
                    tx_position=tx_metadata.tx_position)
                full_spend_transaction_hash = hash_to_hex_str(_get_full_tx_hash(spend_tx_loc, lmdb))
            else:
                full_spend_transaction_hash = None

            if accept_type == 'application/octet-stream':
                response_obj = _pack_pushdata_match_response_bin(
                    match, full_tx_hash, full_pushdata_hash,
                    full_spend_transaction_hash)
                packed_match = filter_response_struct.pack(*response_obj)
                await response.write(packed_match)
            else:  # application/json
                response_json = _pack_pushdata_match_response_json(
                    match, full_tx_hash, full_pushdata_hash,
                    full_spend_transaction_hash)
                row = (json.dumps(response_json) + "\n").encode('utf-8')
                await response.write(row)
            count += 1

        if count == 0:
            return web.HTTPNotFound(reason="No pushdata matches found")

        if accept_type == 'application/octet-stream':
            total_size = count * FILTER_RESPONSE_SIZE
            logger.debug(
                f"Total pushdata filter match response size: {total_size} for count: {count}")
        finalization_flag = b'\x00'

        assert response is not None
        await response.write(finalization_flag)
        return response
    except Exception:
        # Todo - maybe we need a flag to indicate an error occurred mid-way through streaming
        logger.exception("Unexpected exception in get_pushdata_filter_matches")
        return web.HTTPInternalServerError()


async def get_pushdata_filter_matches(request: web.Request) -> StreamResponse:
    """This the main endpoint for the rapid restoration API"""
    # TODO - ensure the data is the correct format and not e.g. an address.
    # TODO - ensure that input pushdata hashes match output otherwise the short hashing will allow
    #  modification of the last bytes and still get a result instead of 404 Not Found
    return await _get_pushdata_filter_matches(request, MatchFormat.PUSHDATA)


async def get_p2pkh_address_filter_matches(request: web.Request) -> StreamResponse:
    """A convenience endpoint that accepts legacy P2PKH addresses instead of pushdata hashes.
    Internally, it is just a conversion to the universal pushdata hash format."""
    # TODO - ensure the data is the correct format e.g. all valid addresses
    return await _get_pushdata_filter_matches(request, MatchFormat.P2PKH)


async def get_transaction(request: web.Request) -> web.Response:
    app_state: 'ApplicationState' = request.app['app_state']
    mysql_db: MySQLDatabase = app_state.mysql_db
    lmdb: LMDB_Database = app_state.lmdb
    accept_type = request.headers.get('Accept')
    txid = request.match_info['txid']
    if not txid:
        raise web.HTTPBadRequest(reason='no txid submitted')

    tx_metadata = await _get_tx_metadata_async(hex_str_to_hash(txid), mysql_db, app_state.executor)
    if not tx_metadata:
        raise web.HTTPNotFound(reason="tx_metadata not found")

    tx_location = TxLocation(tx_metadata.block_hash, tx_metadata.block_num,
        tx_metadata.tx_position)

    if not tx_location:
        raise web.HTTPNotFound(reason="tx location not found")

    rawtx = await lmdb.get_rawtx_by_loc_async(app_state.executor, tx_location)
    # logger.debug(f"Sending rawtx for tx_hash: {hash_to_hex_str(double_sha256(rawtx))}")
    assert rawtx is not None
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
    tx_metadata = await _get_tx_metadata_async(tx_hash, mysql_db, app_state.executor)
    if not tx_metadata:
        raise web.HTTPNotFound(reason="transaction metadata not found")
    tsc_merkle_proof = await _get_tsc_merkle_proof_async(app_state.executor, tx_metadata, mysql_db,
        lmdb, include_full_tx, target_type)

    if accept_type == 'application/octet-stream':
        binary_response = tsc_merkle_proof_json_to_binary(tsc_merkle_proof,
            include_full_tx=include_full_tx,
            target_type=target_type)
        return web.Response(body=binary_response)
    else:
        return web.json_response(data=tsc_merkle_proof)
