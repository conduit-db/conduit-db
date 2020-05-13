"""directly taken from electrumsv.logs"""
import logging
import time


class Logs(object):
    """Manages various aspects of logging."""

    def __init__(self):
        # by default this show warnings and above.  root is a public attribute.
        self.root = logging.getLogger()
        self.stream_handler = logging.StreamHandler()
        self.add_handler(self.stream_handler)

    def add_handler(self, handler):
        formatter = logging.Formatter("%(asctime)s:" + logging.BASIC_FORMAT)
        handler.setFormatter(formatter)
        self.root.addHandler(handler)

    def remove_handler(self, handler):
        self.root.removeHandler(handler)

    def add_file_output(self, path):
        self.add_handler(logging.FileHandler(path))

    def set_stream_output(self, stream):
        self.remove_handler(self.stream_handler)
        self.stream_handler = logging.StreamHandler(stream)
        self.add_handler(self.stream_handler)

    def get_logger(self, name):
        return logging.getLogger(name)

    def set_level(self, level):
        """Level can be a string, such as "info", or a constant from logging module."""
        if isinstance(level, str):
            level = level.upper()
        self.root.setLevel(level)

    def level(self):
        return self.root.level

    def is_debug_level(self):
        return self.level() == logging.DEBUG


logs = Logs()
cur_time = int(time.time())
logs.add_file_output(f"logs/{cur_time}.log")
logs.set_level(logging.DEBUG)
