import io
import multiprocessing
import bitcoinx

from .logs import logs

logger = logs.get_logger("handlers")


class BlockParser(multiprocessing.Process):

    def __init__(self, proc_message_queue, proc_blocks_done_queue):
        super(BlockParser, self).__init__()
        self.proc_message_queue = proc_message_queue
        self.proc_blocks_done_queue = proc_blocks_done_queue

    def run(self):
        while True:
            try:
                # currently this queue only takes memoryviews of full blocks but later can
                # change it to be subviews of individual txs (after a pre-processor has
                # scanned each block)
                message = self.proc_message_queue.get()

                if not message:
                    return
                raw_header = message[0:80]
                header_hash = bitcoinx.double_sha256(raw_header)
                # logger.info("block size=%s bytes", len(message))

                block_subview = message[80:]
                stream: io.BytesIO = io.BytesIO(block_subview)

                txs = []  # todo only pass memory views of txs across IPC channels
                count = bitcoinx.read_varint(stream.read)
                for i in range(count):
                    tx = bitcoinx.Tx.read(stream.read)
                    txs.append(tx)

                self.proc_blocks_done_queue.put((header_hash, txs))
            except Exception as e:
                logger.exception(e)
