from bitcoinx import Headers


class Storage:
    def __init__(self, headers, pg_db=None, memcached=None):
        self.headers: Headers = headers
        self.pg_db = pg_db  # NotImplemented
        self.memcached = memcached  # NotImplemented
