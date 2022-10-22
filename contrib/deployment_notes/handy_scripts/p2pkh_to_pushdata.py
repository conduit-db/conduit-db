import bitcoinx

from conduit_lib.utils import address_to_pushdata_hash

pushdata = address_to_pushdata_hash("1LeEVPCzjhWGgKNf5cbrMz1rWrVdzk4XhB", bitcoinx.Bitcoin)
print(pushdata.hex())
