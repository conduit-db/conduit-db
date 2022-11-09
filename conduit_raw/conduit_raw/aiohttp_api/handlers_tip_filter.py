import asyncio
import json
import logging
import typing
from datetime import datetime
from json import JSONDecodeError
from typing import Optional, cast

import MySQLdb
from aiohttp import web
from bitcoinx import hex_str_to_hash, hash_to_hex_str

from conduit_lib import LMDB_Database
from .mysql_db_tip_filtering import MySQLTipFilterQueries, DatabaseStateModifiedError
from .types import IndexerPushdataRegistrationFlag, TipFilterRegistrationResponse, OutpointType, \
    OutpointJSONType, output_spend_struct, outpoint_struct, ZEROED_OUTPOINT, \
    tip_filter_entry_struct, TipFilterRegistrationEntry, BackendWorkerOfflineError, \
    PushdataRegistrationJSONType

if typing.TYPE_CHECKING:
    from .server import ApplicationState

logger = logging.getLogger('handlers-real-time-processing')


async def get_output_spends(request: web.Request) -> web.Response:
    """
    Return the metadata for each provided outpoint if they are spent.
    """
    app_state: 'ApplicationState' = request.app['app_state']
    if not app_state.worker_state_manager.all_workers_connected_event.is_set():
        raise web.HTTPServiceUnavailable(reason="backend worker processes are offline")

    accept_type = request.headers.get("Accept", "*/*")
    if accept_type == "*/*":
        accept_type = "application/json"
    content_type = request.headers.get('Content-Type')

    body = await request.content.read()
    if not body:
        raise web.HTTPBadRequest(reason="no body")

    client_outpoints: list[OutpointType] = []
    if content_type == 'application/json':
        # Convert the incoming JSON representation to the internal binary representation.
        client_outpoints_json: list[OutpointJSONType] = json.loads(body.decode('utf-8'))
        if not isinstance(client_outpoints_json, list):
            raise web.HTTPBadRequest(reason="payload is not a list")
        for entry in client_outpoints_json:
            if not isinstance(entry, list) or len(entry) != 2 or not isinstance(entry[1], int):
                raise web.HTTPBadRequest(reason="one or more payload entries are incorrect")
            try:
                tx_hash = hex_str_to_hash(entry[0])
            except (ValueError, TypeError):
                raise web.HTTPBadRequest(reason="one or more payload entries are incorrect")
            client_outpoints.append(OutpointType(tx_hash, entry[1]))
    elif content_type == 'application/octet-stream':
        raise web.HTTPBadRequest(reason="binary request body support not implemented yet")
    else:
        raise web.HTTPBadRequest(reason="unknown request body content type")

    mysql_db: MySQLTipFilterQueries = app_state.mysql_db_tip_filter_queries
    lmdb: LMDB_Database = app_state.lmdb
    existing_rows = mysql_db.get_spent_outpoints(client_outpoints, lmdb)

    if accept_type == 'application/octet-stream':
        result_bytes = b""
        for row in existing_rows:
            result_bytes += output_spend_struct.pack(row.out_tx_hash, row.out_idx,
                row.in_tx_hash, row.in_idx, row.block_hash if row.block_hash else bytes(32))
        return web.Response(body=result_bytes)
    else:
        json_list: list[tuple[str, int, str, int, Optional[str]]] = []
        for row in existing_rows:
            json_list.append((hash_to_hex_str(row.out_tx_hash), row.out_idx,
                hash_to_hex_str(row.in_tx_hash), row.in_idx,
                row.block_hash.hex() if row.block_hash else None))
        return web.json_response(data=json_list)


async def post_output_spend_notifications_register(request: web.Request) -> web.Response:
    """
    Register the caller provided UTXO references so that we send notifications if they get
    spent. We also return the current state for any that are known as a response.

    This is a bit clumsy, but this is the simple indexer and it is intended to be the minimum
    effort to allow ElectrumSV to be used against regtest. It is expected that the caller
    has connected to the notification web socket before making this call, and can keep up
    with the notifications.
    """
    app_state: ApplicationState = request.app['app_state']
    if not app_state.worker_state_manager.all_workers_connected_event.is_set():
        raise web.HTTPServiceUnavailable(reason="backend worker processes are offline")

    # TODO This should all be relative to a client account_id
    accept_type = request.headers.get("Accept", "*/*")
    if accept_type == "*/*":
        accept_type = "application/json"

    content_type = request.headers.get("Content-Type")
    body = await request.content.read()
    if not body:
        raise web.HTTPBadRequest(reason="no body")

    client_outpoints: list[OutpointType] = []
    if content_type == 'application/json':
        # Convert the incoming JSON representation to the internal binary representation.
        client_outpoints_json: list[OutpointJSONType] = json.loads(body.decode('utf-8'))
        if not isinstance(client_outpoints_json, list):
            raise web.HTTPBadRequest(reason="payload is not a list")
        for entry in client_outpoints_json:
            if not isinstance(entry, list) or len(entry) != 2 or not isinstance(entry[1], int):
                raise web.HTTPBadRequest(reason="one or more payload entries are incorrect")
            try:
                tx_hash = hex_str_to_hash(entry[0])
            except (ValueError, TypeError):
                raise web.HTTPBadRequest(reason="one or more payload entries are incorrect")
            client_outpoints.append(OutpointType(tx_hash, entry[1]))
    elif content_type == 'application/octet-stream':
        if len(body) % outpoint_struct.size != 0:
            raise web.HTTPBadRequest(reason="binary request body malformed")

        for outpoint_index in range(len(body) // outpoint_struct.size):
            outpoint = cast(OutpointType,
                outpoint_struct.unpack_from(body, outpoint_index * outpoint_struct.size))
            client_outpoints.append(outpoint)
    else:
        raise web.HTTPBadRequest(reason="unknown request body content type")

    try:
        existing_rows = await app_state.worker_state_manager\
            .register_output_spend_notifications(client_outpoints)
    except BackendWorkerOfflineError:
        raise web.HTTPServiceUnavailable(reason="Backend worker processes are not responding")

    if accept_type == 'application/octet-stream':
        result_bytes = b""
        for row in existing_rows:
            result_bytes += output_spend_struct.pack(row.out_tx_hash, row.out_idx,
                row.in_tx_hash, row.in_idx, row.block_hash if row.block_hash else bytes(32))
        return web.Response(body=result_bytes)
    else:
        json_list: list[tuple[str, int, str, int, Optional[str]]] = []
        for row in existing_rows:
            json_list.append((hash_to_hex_str(row.out_tx_hash), row.out_idx,
                hash_to_hex_str(row.in_tx_hash), row.in_idx,
                row.block_hash.hex() if row.block_hash else None))
        return web.json_response(data=json.dumps(json_list))


async def post_output_spend_notifications_unregister(request: web.Request) -> web.Response:
    """
    This provides a way for the monitored output spends to be unregistered or cleared. It is
    assumed that whomever has access to this endpoint, has control over the registration and
    can do this on behalf of all users.

    The reference server manages who is subscribed to what, and what should be monitored, and
    uses this method to ensure the simple indexer is only monitoring what it needs to.

    If the reference server wishes to clear all monitored output spends, it should send one
    outpoint and it should be zeroed (null tx hash and zero index).
    """
    app_state: ApplicationState = request.app['app_state']
    if not app_state.worker_state_manager.all_workers_connected_event.is_set():
        raise web.HTTPServiceUnavailable(reason="backend worker processes are offline")

    content_type = request.headers.get('Content-Type')
    body = await request.content.read()
    if not body:
        raise web.HTTPBadRequest(reason="no body")

    client_outpoints: list[OutpointType] = []
    if content_type == 'application/json':
        # Convert the incoming JSON representation to the internal binary representation.
        try:
            client_outpoints_json: list[OutpointJSONType] = json.loads(body.decode('utf-8'))
        except JSONDecodeError as e:
            raise web.HTTPBadRequest(reason="JSONDecodeError: %s" % e)
        if not isinstance(client_outpoints_json, list):
            raise web.HTTPBadRequest(reason="payload is not a list")
        for entry in client_outpoints_json:
            if not isinstance(entry, list) or len(entry) != 2 or not isinstance(entry[1], int):
                raise web.HTTPBadRequest(reason="one or more payload entries are incorrect")
            try:
                tx_hash = hex_str_to_hash(entry[0])
            except (ValueError, TypeError):
                raise web.HTTPBadRequest(reason="one or more payload entries are incorrect")
            client_outpoints.append(OutpointType(tx_hash, entry[1]))
    elif content_type == 'application/octet-stream':
        if len(body) % outpoint_struct.size != 0:
            raise web.HTTPBadRequest(reason="binary request body malformed")

        for outpoint_index in range(len(body) // outpoint_struct.size):
            outpoint = cast(OutpointType,
                outpoint_struct.unpack_from(body, outpoint_index * outpoint_struct.size))
            client_outpoints.append(outpoint)
    else:
        raise web.HTTPBadRequest(reason="unknown request body content type")
    try:
        if len(client_outpoints) == 1 and client_outpoints[0] == ZEROED_OUTPOINT:
            await app_state.worker_state_manager.clear_output_spend_notifications()
        else:
            await app_state.worker_state_manager\
                .unregister_output_spend_notifications(client_outpoints)
    except BackendWorkerOfflineError:
        raise web.HTTPServiceUnavailable(reason="Backend worker processes are not responding")

    return web.Response()


async def indexer_post_transaction_filter(request: web.Request) -> web.Response:
    """
    Used by the client to register pushdata hashes for new/updated transactions so that they can
    know about new occurrences of their pushdatas. This should be safe for consecutive calls
    even for the same pushdata, as the database unique constraint should raise an integrity
    error if there is an ongoing registration.
    """
    # TODO(1.4.0) Payment. This should be monetised with a free quota.
    app_state: ApplicationState = request.app['app_state']
    if not app_state.worker_state_manager.all_workers_connected_event.is_set():
        raise web.HTTPServiceUnavailable(reason="backend worker processes are offline")
    mysql_db: MySQLTipFilterQueries = app_state.mysql_db_tip_filter_queries

    accept_type = request.headers.get("Accept", "*/*")
    if accept_type == "*/*":
        accept_type = "application/json"
    if accept_type != "application/json":
        raise web.HTTPBadRequest(reason="only json response body supported")

    content_type = request.headers.get("Content-Type")
    if content_type not in ("application/octet-stream", "application/json", "*/*"):
        raise web.HTTPBadRequest(reason="only binary or json request body supported")

    try:
        account_id_text = request.query.get("account_id", "")
        external_account_id = int(account_id_text)
    except (KeyError, ValueError):
        raise web.HTTPBadRequest(reason="`account_id` is not a number")

    try:
        date_created_text = request.query.get("date_created", "")
        date_created = int(date_created_text)
    except (KeyError, ValueError):
        raise web.HTTPBadRequest(reason="`date_created` is not a number")

    body = await request.content.read()
    if not body:
        raise web.HTTPBadRequest(reason="no body")

    # Currently we only allow registrations between five minutes and seven days.
    minimum_seconds = 5 * 60
    maximum_seconds = 7 * 24 * 60 * 60
    registration_entries = list[TipFilterRegistrationEntry]()
    if content_type == "application/octet-stream":
        if len(body) % tip_filter_entry_struct.size != 0:
            raise web.HTTPBadRequest(reason="binary request body malformed")
        for entry_index in range(len(body) // tip_filter_entry_struct.size):
            entry = TipFilterRegistrationEntry(*tip_filter_entry_struct.unpack_from(body,
                entry_index * tip_filter_entry_struct.size))
            if entry.duration_seconds < minimum_seconds:
                raise web.HTTPBadRequest(
                    reason=f"An entry has a duration of {entry.duration_seconds} which is lower than "
                        f"the minimum value {minimum_seconds}")
            if entry.duration_seconds > maximum_seconds:
                raise web.HTTPBadRequest(
                    reason=f"An entry has a duration of {entry.duration_seconds} which is higher than "
                        f"the maximum value {maximum_seconds}")
            registration_entries.append(entry)
    elif content_type in ("application/json", "*/*"):
        try:
            json_body = cast(list[PushdataRegistrationJSONType], json.loads(body))
        except JSONDecodeError as e:
            raise web.HTTPBadRequest(reason="JSONDecodeError: %s" % e)
        for pushdata_hash_hex, duration_seconds in json_body:
            pushdata_hash = bytes.fromhex(pushdata_hash_hex)
            entry = TipFilterRegistrationEntry(pushdata_hash, duration_seconds)
            if entry.duration_seconds < minimum_seconds:
                raise web.HTTPBadRequest(
                    reason=f"An entry has a duration of {entry.duration_seconds} which is lower than "
                        f"the minimum value {minimum_seconds}")
            if entry.duration_seconds > maximum_seconds:
                raise web.HTTPBadRequest(
                    reason=f"An entry has a duration of {entry.duration_seconds} which is higher than "
                        f"the maximum value {maximum_seconds}")
            registration_entries.append(entry)

    logger.debug("Adding tip filter entries to database %s", registration_entries)
    # It is required that the client knows what it is doing and this is enforced by disallowing
    # these registrations if any of the given pushdatas are already registered.
    if not await asyncio.get_running_loop().run_in_executor(app_state.executor,
            mysql_db.create_tip_filter_registrations_write,
            external_account_id, date_created, registration_entries):
        raise web.HTTPBadRequest(reason="one or more hashes were already registered")

    logger.debug("Registering tip filter entries with synchroniser")
    # This can be registering hashes that other accounts have already registered, that is fine
    # as long as the registrations match the unregistrations.
    try:
        await app_state.worker_state_manager.register_pushdata_notifications(registration_entries)
    except BackendWorkerOfflineError:
        raise web.HTTPServiceUnavailable(reason="Backend worker processes are not responding")

    # Convert the timestamps to ISO 8601 time string. The posix timestamp is fine but we want to
    # default the public-facing API to JSON-developer level formats. We bundle it as an "object"
    # so that we can put additional things in there.
    date_created_text = datetime.utcfromtimestamp(date_created).isoformat()
    json_lump: TipFilterRegistrationResponse = {
        "dateCreated": date_created_text,
    }
    return web.json_response(data=json_lump)


async def indexer_post_transaction_filter_delete(request: web.Request) -> web.Response:
    """
    Optional endpoint if running an indexer.

    Used by the client to unregister pushdata hashes they are monitoring.
    """
    # TODO(1.4.0) Payment. This should be monetised with a free quota.
    app_state: ApplicationState = request.app['app_state']
    if not app_state.worker_state_manager.all_workers_connected_event.is_set():
        raise web.HTTPServiceUnavailable(reason="backend worker processes are offline")
    mysql_db: MySQLTipFilterQueries = app_state.mysql_db_tip_filter_queries

    content_type = request.headers.get("Content-Type")
    if content_type not in ("application/octet-stream", "application/json", "*/*"):
        raise web.HTTPBadRequest(reason="only binary or json request body supported")

    try:
        account_id_text = request.query.get("account_id", "")
        external_account_id = int(account_id_text)
    except (KeyError, ValueError):
        raise web.HTTPBadRequest(reason="`account_id` is not a number")

    body = await request.content.read()
    if not body:
        raise web.HTTPBadRequest(reason="no body")
    pushdata_hashes: set[bytes] = set()
    if content_type == "application/octet-stream":
        if len(body) % 32 != 0:
            raise web.HTTPBadRequest(reason="binary request body malformed")
        for pushdata_index in range(len(body) // 32):
            pushdata_hashes.add(body[pushdata_index:pushdata_index+32])
        if not len(pushdata_hashes):
            raise web.HTTPBadRequest(reason="no pushdata hashes provided")
    elif content_type in ("application/json", "*/*"):
        try:
            json_body = json.loads(body)
        except JSONDecodeError as e:
            raise web.HTTPBadRequest(reason="JSONDecodeError: %s" % e)
        for pushdata_hex in json_body:
            pushdata_hash = bytes.fromhex(pushdata_hex)
            pushdata_hashes.add(pushdata_hash)
        if not len(pushdata_hashes):
            raise web.HTTPBadRequest(reason="no pushdata hashes provided")

    pushdata_hash_list = list(pushdata_hashes)

    # This is required to update all the given pushdata filtering registration from finalised
    # (and not being deleted by any other concurrent task) to finalised and being deleted. If
    # any of the registrations are not in this state, it is assumed that the client application
    # is broken and mismanaging it's own state.
    try:
        await asyncio.get_running_loop().run_in_executor(app_state.executor,
            mysql_db.update_tip_filter_registrations_flags_write, external_account_id,
            pushdata_hash_list, IndexerPushdataRegistrationFlag.DELETING, None,
            IndexerPushdataRegistrationFlag.FINALISED,
            IndexerPushdataRegistrationFlag.FINALISED | IndexerPushdataRegistrationFlag.DELETING,
            True)
    except DatabaseStateModifiedError:
        raise web.HTTPBadRequest(reason="some pushdata hashes are not registered")

    try:
        await asyncio.get_running_loop().run_in_executor(app_state.executor,
            mysql_db.delete_tip_filter_registrations_write, external_account_id,
            pushdata_hash_list, IndexerPushdataRegistrationFlag.FINALISED,
            IndexerPushdataRegistrationFlag.FINALISED)
    except MySQLdb.Error:
        raise web.HTTPInternalServerError(reason="database delete operation failed")

    try:
        # It is essential that these registrations are in place either from a call during the
        # current run or read from the database on load, and that we are removing those
        # registrations. If entries not present get unregistered, this can corrupt the filter.
        # If entries present get unregistered but do not belong to this account, this will
        # corrupt the filter.
        registration_entries: list[TipFilterRegistrationEntry] = \
            [TipFilterRegistrationEntry(pushdata_hash, 0xffffffff)
                for pushdata_hash in pushdata_hash_list]
        await app_state.worker_state_manager.unregister_pushdata_notifications(registration_entries)
    except BackendWorkerOfflineError:
        raise web.HTTPServiceUnavailable(reason='backend workers not responding')

    return web.Response(status=200)

