Conduit: for Bitcoin SV's p2p network.
===============================================================================

Makes heavy use of bitcoinx for serialization / deserialization: https://github.com/kyuupichan/bitcoinx

Introduction
------------
This is basically a lightweight, **asyncio-based** client for connecting to the bitcoin p2p network
in the most straight-forward way possible but with scalability in mind.

On example use-case might be asynchronous broadcasting to multiple nodes at >200 tx/second (much
higher rates are achievable than the built-in node's JSON-RPC will allow).

In combination with services like ElectrumX it obviates the need to run a local node for pretty
much any application.

There are three main worker tasks that get spawned at startup with three corresponding queues.
- writer and outbound_queue
- reader and inbound_queue
- message task and message_queue

Planned improvements
--------------------
- Work in progress...

----------------------------

Examples
--------

Can be used as a 'one and done' client for pre-specified requests (Not the recommended approach)

.. code-block:: python

    >>> conduit = Conduit(network='main')
    >>> conduit.add_peers([("104.238.220.203", 8333)])  # optional
    >>> conduit.set_main_peer(("104.238.220.203", 8333))  # optional
    >>>
    >>> """push messages to 'outbound_queue' for immediate processing after handshake"""
    >>> conduit.send_request_nowait('tx', *args)
    >>> conduit.send_request_nowait('getdata', *args)
    >>> conduit.send_request_nowait('getdata', *args)
    >>>
    >>> """terminates as soon as all queued requests are completed"""
    >>> result = conduit.run_until_queue_empty()
    >>> print(result)
    '[{<json>}, {<json>}, {<json>}, {<json>}]'

But can also be spawned as a worker task to maintain a set of connections to the p2p network.

.. code-block:: python

    >>> async def main_app(conduit):
    ...     """or queue messages *after* startup for the lifetime of your application (recommended)"""
    ...     await conduit.send_request('getaddr', *args)
    ...
    ...     # send an inv + update getdata handler to provide the 'tx' message on_getdata
    ...     await conduit.broadcast_transaction(rawtx='<rawtx>', nodes=5)

    ...     while True:
    ...         command, message = await conduit.message_queue.get()
    ...         inv_hash = message.get('inv_hash')
    ...         if command == 'inv' and inv_hash == '<txid>':
    ...             print(f"one node now has our sent txid: {inv_hash}!")
    ...         else:
                    print(message)  # will mostly be a constant influx of 'inv' messages
                await asyncio.sleep(0.05)
    ...     """both blocking and non-blocking methods are permissible"""
    >>>
    >>>
    >>> async def main():
    ...     conduit = Conduit(network='main')
    ...     conduit.add_peers([("104.238.220.203", 8333)])  # optional
    ...     conduit.set_main_peer(("104.238.220.203", 8333))  # optional
    ...
    ...     """queue messages before startup"""
    ...     conduit.send_request_nowait('tx', *args)
    ...     conduit.send_request_nowait('getdata', *args)
    ...     tasks = [asyncio.create_task(conduit.run()),
    ...              asyncio.create_task(main_app(conduit))]
    ...     await asyncio.gather(tasks)  # conduit will run indefinitely as a worker task
    >>>
    >>> asyncio.run()




Features
--------
- 1) asyncio-based server that maintains connection(s) to remote bitcoin node(s)
- 2) serializes binary messages to the 'outbound queue' (for the server to transmit)
- 3) deserializes messages from the 'inbound queue'.
- 4) sorts peers by ping / latency etc. automatically
- 5) can easily be used from synchronous or asynchronous code
- 6) lightweight, straight-forward design.

Installation
------------

.. code-block:: bash

    $ pip install conduit
