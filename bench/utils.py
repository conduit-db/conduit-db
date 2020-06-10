def print_results(count_txs, t1, block_view):
    rate = count_txs / t1
    av_tx_size = round(len(block_view) / count_txs)
    bytes_per_sec = rate * av_tx_size
    MB_per_sec = round(bytes_per_sec / (1024 * 1024))

    print(
        f"block parsing took {round(t1, 5)} seconds for {count_txs} txs and"
        f" {len(block_view)} "
        f"bytes - therefore {rate} txs per second for an average tx size "
        f"of {av_tx_size} bytes - therefore {MB_per_sec} MB/sec"
    )


def print_results_asyncpg(count_txs, t1):
    rate = count_txs / t1

    print(
        f"block parsing took {round(t1, 5)} seconds for {count_txs} txs"
        f"therefore {rate} txs per second"
    )
