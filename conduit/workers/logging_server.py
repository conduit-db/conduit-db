import os
import pickle
import logging
import logging.handlers
import socket
import socketserver
import struct
import multiprocessing
import threading
import time
from pathlib import Path


# Log level
PROFILING = 9
MODULE_DIR = os.path.dirname(os.path.abspath(__file__))


class LogRecordStreamHandler(socketserver.StreamRequestHandler):
    """Handler for a streaming logging request.

    This basically logs the record using whatever logging policy is
    configured locally.
    """

    def handle(self):
        """
        Handle multiple requests - each expected to be a 4-byte length,
        followed by the LogRecord in pickle format. Logs the record
        according to whatever policy is configured locally.
        """
        while True:
            chunk = self.connection.recv(4)
            if len(chunk) < 4:
                break
            slen = struct.unpack(">L", chunk)[0]
            chunk = self.connection.recv(slen)
            while len(chunk) < slen:
                chunk = chunk + self.connection.recv(slen - len(chunk))

            if chunk == b"stop":
                break

            obj = self.unPickle(chunk)
            record = logging.makeLogRecord(obj)
            self.handleLogRecord(record)

        logging.debug("server stopping")
        self.server.stop_event.set()

    def unPickle(self, data):
        return pickle.loads(data)

    def handleLogRecord(self, record):
        if self.server.logname is not None:
            name = self.server.logname
        else:
            name = record.name
        logger = logging.getLogger(name)
        logger.handle(record)


class LogRecordSocketReceiver(socketserver.ThreadingTCPServer):
    """
    Simple TCP socket-based logging receiver suitable for testing.
    """

    allow_reuse_address = True

    def __init__(
        self,
        stop_event,
        host="localhost",
        port=logging.handlers.DEFAULT_TCP_LOGGING_PORT,
        handler=LogRecordStreamHandler,
    ):
        socketserver.ThreadingTCPServer.__init__(self, (host, port), handler)
        self.abort = 0
        self.timeout = 1
        self.logname = None
        self.stop_event = stop_event


class TCPLoggingServer(multiprocessing.Process):
    """Centralizes logging via streamhandler.
    Gracefully shutdown via tcp b"stop" with big-ending unsigned long int = len msg"""

    def __init__(self):
        super(TCPLoggingServer, self).__init__()
        self.tcpserver = None

    def setup_local_logging_policy(self):
        rootLogger = logging.getLogger('')
        logging.addLevelName(PROFILING, 'PROFILING')

        FORMAT = "%(asctime)-25s %(levelname)-10s %(name)-20s %(message)s"
        logging.basicConfig(format=FORMAT, level=PROFILING)

        log_dir = Path(MODULE_DIR).parent.parent.joinpath(f"logs")
        os.makedirs(log_dir, exist_ok=True)
        logfile_path = os.path.join(log_dir, time.strftime("%Y%m%d-%H%M%S") + ".log")
        file_handler = logging.FileHandler(logfile_path)
        formatter = logging.Formatter(FORMAT)
        file_handler.setFormatter(formatter)
        file_handler.setLevel(PROFILING)
        rootLogger.addHandler(file_handler)

    def main_thread(self):
        self.tcpserver.serve_forever()

    def run(self):
        log_path = Path(os.getcwd()).parent.parent.joinpath("logs").__str__()
        os.makedirs(log_path, exist_ok=True)
        self.setup_local_logging_policy()

        self.stop_event = threading.Event()
        self.tcpserver = LogRecordSocketReceiver(self.stop_event)
        self.logger = logging.getLogger("logging-server")
        self.logger.debug(f'starting {self.__class__.__name__}...')

        main_thread = threading.Thread(target=self.main_thread)
        main_thread.start()

        while True:
            self.stop_event.wait()
            self.tcpserver.shutdown()
            break
        self.logger.debug("tcp server stopped")

if __name__ == "__main__":
    TCPLoggingServer().start()
    time.sleep(5)

    logging.debug("shutting down...")
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('127.0.0.1', logging.handlers.DEFAULT_TCP_LOGGING_PORT))
    len_msg = struct.pack(">L", 4)
    s.sendall(len_msg + b"stop")
