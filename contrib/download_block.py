import json
import os

import requests
import shutil


def download_file(url):
    local_filename = "block593161.hex"
    data = {
        "jsonrpc": "2.0",
        "method": "getblockbyheight",
        "params": [593161, 0],
        "id": 1,
    }
    # NOTE the stream=True parameter below
    with requests.post(url, data=json.dumps(data), stream=True) as r:
        r.raise_for_status()
        with open(local_filename, "wb") as f:
            for chunk in r.iter_content(chunk_size=1_000_000):
                # If you have chunk encoded response uncomment if
                # and set chunk_size parameter to None.
                # if chunk:
                f.write(chunk)
    return local_filename


import asyncio
from aiohttp.client import ClientSession


async def download_one(url, dest_file):
    data = {
        "jsonrpc": "2.0",
        "method": "getblockbyheight",
        "params": [593161, 0],
        "id": 1,
    }
    async with ClientSession() as session:
        print(f"Downloading {url}")

        async with session.post(url, data=json.dumps(data)) as resp:
            if resp.status != 200:
                print(f"Download failed: {resp.status}")
                return

            with open(dest_file, "wb") as fd:
                async for chunk in resp.content.iter_chunked(1_000_000):
                    fd.write(chunk)


if __name__ == "__main__":
    url = f"http://rpcuser:rpcpassword@127.0.0.1:8332"
    dest_file = "block593161.hex"
    asyncio.run(download_one(url, dest_file=dest_file))
