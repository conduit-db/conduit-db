def print_results(count, t1, block_view):
    rate = count/t1
    av_tx_size = len(block_view) / count
    bytes_per_sec = rate * av_tx_size
    MB_per_sec = bytes_per_sec/(1024*1024)

    print(
        f"block parsing took {round(t1, 5)} seconds for {count} txs and"
        f" {len(block_view)} "
        f"bytes - therefore {rate} txs per second for an average tx size "
        f"of {av_tx_size} bytes - therefore {MB_per_sec} MB/sec"
    )
