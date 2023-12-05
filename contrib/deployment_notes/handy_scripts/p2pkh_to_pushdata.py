# Copyright (c) 2020-2023, Hayden Donnelly
#
# All rights reserved.
#
# Licensed under the MIT License; see LICENCE for details.

import bitcoinx

from conduit_lib.utils import address_to_pushdata_hash

pushdata = address_to_pushdata_hash("1LeEVPCzjhWGgKNf5cbrMz1rWrVdzk4XhB", bitcoinx.Bitcoin)
print(pushdata.hex())
