import json
import time

import requests


def get_peerinfo():
    payload = json.dumps(
                {"jsonrpc": "2.0", "method": f"getpeerinfo", "params": [], "id": 0})
    result = requests.post(f"http://rpcuser:rpcpassword@127.0.0.1:8332", data=payload)
    return result.json()['result']


def setban(ip):
    payload = json.dumps(
                {"jsonrpc": "2.0", "method": f"setban", "params": [ip, 'add'], "id": 0})
    result = requests.post(f"http://rpcuser:rpcpassword@127.0.0.1:8332", data=payload)
    return result.json()['result']

def listbanned():
    payload = json.dumps(
                {"jsonrpc": "2.0", "method": f"listbanned", "params": [], "id": 0})
    result = requests.post(f"http://rpcuser:rpcpassword@127.0.0.1:8332", data=payload)
    return result.json()['result']


while True:
    peerinfo = get_peerinfo()
    # print(peerinfo)
    for peer in peerinfo:
        print(peer['subver'])

        if 'Cash Node' in peer['subver']:
            setban(peer['addr'].split(":")[0])
            # setban(peer['addrlocal'])

        if 'ABC' in peer['subver']:
            setban(peer['addr'].split(":")[0])
            # setban(peer['addrlocal'])

        # if '/Bitcoin SV' in peer['subver'] and "1.0.8" not in peer['subver']:
        #     setban(peer['addr'].split(":")[0])

    banned = listbanned()
    if banned:
        print(f"banned: {len(listbanned())} ip addresses; ")
    time.sleep(5)
