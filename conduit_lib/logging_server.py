import os
import logging
import logging.handlers
import pickle
from pathlib import Path
import socket
import socketserver
import struct
import multiprocessing
import threading
import time
from typing import Any, Callable

import zmq

# Log level
PROFILING = 9
MODULE_DIR = os.path.dirname(os.path.abspath(__file__))


class LogRecordStreamHandler(socketserver.StreamRequestHandler):
    """Handler for a streaming logging request.

    This basically logs the record using whatever logging policy is
    configured locally.
    """

    logger = logging.getLogger("TCPServer-Handler")

    def handle(self) -> None:
        """
        Handle multiple requests - each expected to be a 4-byte length,
        followed by the LogRecord in pickle format. Logs the record
        according to whatever policy is configured locally.
        """
        try:
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
                record: logging.LogRecord = logging.makeLogRecord(obj)
                self.handleLogRecord(record)

        except ConnectionResetError:
            self.logger.info(f"Forceful disconnect from {repr(self.connection.getpeername())}")

    def unPickle(self, data: bytes) -> Any:
        return pickle.loads(data)

    def handleLogRecord(self, record: logging.LogRecord) -> None:
        if self.server.logname is not None:  # type: ignore[attr-defined]
            name = self.server.logname  # type: ignore[attr-defined]
        else:
            name = record.name
        logger = logging.getLogger(name)
        logger.handle(record)


class LogRecordSocketReceiver(socketserver.ThreadingTCPServer):
    """
    Simple TCP socket-based logging receiver suitable for testing.
    """

    logger = logging.getLogger("TCPServer")
    allow_reuse_address = True

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 63451,
        handler: Callable[..., socketserver.StreamRequestHandler] = LogRecordStreamHandler,
    ) -> None:
        socketserver.ThreadingTCPServer.__init__(self, (host, port), handler)
        self.abort: int = 0
        self.timeout: int = 1
        self.logname: int | None = None


class TCPLoggingServer(multiprocessing.Process):
    """Centralizes logging via streamhandler.
    Gracefully shutdown via tcp b"stop" with big-ending unsigned long int = len msg
    """

    def __init__(self, port: int, service_name: str, kill_port: int = 46464) -> None:
        super(TCPLoggingServer, self).__init__()
        self.port: int = port
        self.kill_port: int = kill_port
        self.tcpserver: LogRecordSocketReceiver | None = None
        self.service_name: str = service_name

    def setup_local_logging_policy(self) -> None:
        rootLogger = logging.getLogger("")
        logging.addLevelName(PROFILING, "PROFILING")

        FORMAT = "%(asctime)-25s %(levelname)-10s %(name)-28s %(message)s"
        logging.basicConfig(format=FORMAT, level=PROFILING)

        log_dir = Path(MODULE_DIR).parent.joinpath(f"logs")
        os.makedirs(log_dir, exist_ok=True)

        logfile_path = os.path.join(log_dir, self.service_name + ".log")
        if os.path.exists(logfile_path):
            i = 1
            basename = os.path.join(log_dir, self.service_name)
            while os.path.exists(logfile_path):
                i += 1
                logfile_path = f"{basename}{i}.log"

        file_handler = logging.FileHandler(logfile_path)
        formatter = logging.Formatter(FORMAT)
        file_handler.setFormatter(formatter)
        file_handler.setLevel(PROFILING)
        rootLogger.addHandler(file_handler)

    def main_thread(self) -> None:
        assert self.tcpserver is not None
        self.tcpserver.serve_forever()

    def run(self) -> None:
        self.setup_local_logging_policy()

        self.stop_event = threading.Event()
        self.tcpserver = LogRecordSocketReceiver(port=self.port)
        self.logger = logging.getLogger("logging-server")
        self.logger.info(f'Starting {self.__class__.__name__}')

        # PUB-SUB from Controller to worker to kill the worker
        context3 = zmq.Context[zmq.Socket[bytes]]()

        # Todo there is cross-talk of the stop_signal from ConduitIndex and ConduitRaw because
        #  they both import this common library and use remote_port: 63241
        self.kill_worker_socket = context3.socket(zmq.SUB)
        self.kill_worker_socket.connect(f"tcp://127.0.0.1:{self.kill_port}")
        self.kill_worker_socket.setsockopt(zmq.SUBSCRIBE, b"stop_signal")

        main_thread = threading.Thread(target=self.main_thread, daemon=True)
        main_thread.start()

        try:
            while True:
                message = self.kill_worker_socket.recv()
                if message == b"stop_signal":
                    break
                time.sleep(0.2)
        except KeyboardInterrupt:
            self.logger.debug("ThreadingTCPServer stopping...")
        except Exception as e:
            self.logger.exception("Caught exception")
        finally:
            if self.tcpserver is not None:
                self.tcpserver.shutdown()
                self.logger.info("Process Stopped")


if __name__ == "__main__":
    TCPLoggingServer(54545, service_name="test_server").start()
    time.sleep(5)

    logging.debug("shutting down...")
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect(("127.0.0.1", 46464))
    len_msg = struct.pack(">L", 4)
    s.sendall(len_msg + b"stop")
