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
        # if a name is specified, we use the named logger rather than the one
        # implied by the record.
        if self.server.logname is not None:
            name = self.server.logname
        else:
            name = record.name
        logger = logging.getLogger(name)
        # N.B. EVERY record gets logged. This is because Logger.handle
        # is normally called AFTER logger-level filtering. If you want
        # to do filtering, do it at the client end to save wasting
        # cycles and network bandwidth!
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

class LoggingServer(multiprocessing.Process):
    """Centralizes logging via streamhandler.
    Gracefully shutdown via tcp b"stop" with big-ending unsigned long int = len msg"""

    def __init__(self):
        super(LoggingServer, self).__init__()
        self.tcpserver = None

    def setup_local_logging_policy(self):
        FORMAT = "%(asctime)-25s %(name)-20s %(message)s"
        logging.basicConfig(format=FORMAT, level=logging.DEBUG)
        """
        logfile_path = os.path.join(log_path, time.strftime("%Y%m%d-%H%M%S") + ".log")
        file_handler = logging.FileHandler(logfile_path)
        formatter = logging.Formatter("%(asctime)s:" + logging.BASIC_FORMAT)
        file_handler.setFormatter(formatter)
        """

    def main_thread(self):
        self.tcpserver.serve_forever()

    def run(self):
        log_path = Path(os.getcwd()).parent.parent.joinpath("logs").__str__()
        os.makedirs(log_path, exist_ok=True)
        self.setup_local_logging_policy()

        self.stop_event = threading.Event()
        self.tcpserver = LogRecordSocketReceiver(self.stop_event)
        self.logger = logging.getLogger("logging-server")
        self.logger.debug('About to start TCP server...')

        main_thread = threading.Thread(target=self.main_thread)
        main_thread.start()

        while True:
            self.stop_event.wait()
            self.tcpserver.shutdown()
            break
        self.logger.debug("tcp server stopped")

if __name__ == "__main__":
    worker_in_queue_logging = multiprocessing.Queue()
    LoggingServer().start()
    time.sleep(5)

    logging.debug("shutting down...")
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(('127.0.0.1', logging.handlers.DEFAULT_TCP_LOGGING_PORT))
    len_msg = struct.pack(">L", 4)
    s.sendall(len_msg + b"stop")