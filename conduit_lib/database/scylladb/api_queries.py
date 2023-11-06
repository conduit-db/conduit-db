import os

import typing

MODULE_DIR = os.path.dirname(os.path.abspath(__file__))

if typing.TYPE_CHECKING:
    from .scylladb import ScyllaDB


class ScyllaDBAPIQueries:
    def __init__(self, scylladb: "ScyllaDB") -> None:
        self.scylladb = scylladb
