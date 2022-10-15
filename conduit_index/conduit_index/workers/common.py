import logging
import time

from ..types import ProcessedBlockAcks, MySQLFlushBatchWithAcks, MempoolTxAck
from conduit_lib import MySQLDatabase
from conduit_lib.database.mysql.types import MempoolTransactionRow, \
    ConfirmedTransactionRow, InputRow, OutputRow, PushdataRow


def reset_rows() -> MySQLFlushBatchWithAcks:
    txs: list[MempoolTransactionRow | ConfirmedTransactionRow] = []
    ins: list[InputRow] = []
    outs: list[OutputRow] = []
    pds: list[PushdataRow] = []
    acks: MempoolTxAck | ProcessedBlockAcks = []
    return MySQLFlushBatchWithAcks(txs, ins, outs, pds, acks)


def maybe_refresh_mysql_connection(mysql_db: MySQLDatabase,
        last_mysql_activity: int, logger: logging.Logger) -> tuple[MySQLDatabase, int]:
    REFRESH_TIMEOUT = 600
    if int(time.time()) - last_mysql_activity > REFRESH_TIMEOUT:
        logger.info(f"Refreshing MySQLDatabase connection due to {REFRESH_TIMEOUT} "
            f"second refresh timeout")
        mysql_db.close()
        mysql_db = mysql_db.mysql_conn.ping(reconnect=True)
        last_mysql_activity = int(time.time())
        return mysql_db, last_mysql_activity
    else:
        return mysql_db, last_mysql_activity
