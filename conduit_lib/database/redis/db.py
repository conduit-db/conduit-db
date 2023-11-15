import os
from typing import Sequence

import redis

from conduit_lib.utils import Timer


class RedisCache:
    BATCH_SIZE = 5000

    def __init__(self) -> None:
        # decode_responses is set to false because it allows for mypy type safety
        # it also minimizes unnecessary type conversion overheads
        # integer inputs are still auto-converted to a utf-8 string representation which needs
        # to be decoded and converted back into an integer.
        self.r = redis.Redis(host=os.getenv('SCYLLA_HOST', '127.0.0.1'), port=6379, decode_responses=False)

    def get(self, key: bytes) -> bytes | None:
        return self.r.get(key)

    def set(self, key: bytes, value: bytes) -> bool | None:
        return self.r.set(key, value)

    def delete(self, key: bytes) -> int:
        return self.r.delete(key)

    def get_in_namespace(self, namespace: bytes, key: bytes) -> bytes | None:
        return self.r.get(namespace + key)

    def scan_in_namespace(self, namespace: bytes) -> list[bytes]:
        cursor = None
        all_keys = []
        while cursor != 0:
            cursor, keys = self.r.scan(
                cursor=cursor if cursor else 0, match=namespace + b"*", count=self.BATCH_SIZE
            )
            all_keys.extend([key[len(namespace):] for key in keys])
        return all_keys

    def bulk_load_in_namespace(self, namespace: bytes, pairs: Sequence[tuple[bytes, bytes | int]]) -> None:
        """This only accepts a two-tuple. The first is the key. The second is the value.
        For values with more than one element, a different redis data type will be needed"""
        count = len(pairs)
        with Timer(count=count, name='redis-pipeline'):
            for batch_start in range(0, count, self.BATCH_SIZE):
                pipe = self.r.pipeline()
                for i in range(batch_start, min(batch_start + self.BATCH_SIZE, count)):
                    pipe.set(namespace + pairs[i][0], pairs[i][1])
                pipe.execute()
                pipe.close()

    def bulk_delete_in_namespace(self, namespace: bytes, pairs: Sequence[tuple[bytes, bytes | int]]) -> None:
        """This only accepts a two-tuple. The first is the key. The second is the value.
        For values with more than one element, a different redis data type will be needed"""
        count = len(pairs)
        if not count:
            return
        with Timer(count=count, name='redis-pipeline'):
            for batch_start in range(0, count, self.BATCH_SIZE):
                pipe = self.r.pipeline()
                for i in range(batch_start, min(batch_start + self.BATCH_SIZE, count)):
                    pipe.delete(namespace + pairs[i][0])
                pipe.execute()
                pipe.close()

    def bulk_delete_all_in_namespace(self, prefix: bytes) -> None:
        with Timer(name='redis-pipeline-delete') as timer:
            pipe = self.r.pipeline()
            cursor = None
            timer.count = 0
            while cursor != 0:
                cursor, keys = self.r.scan(
                    cursor=cursor if cursor else 0, match=prefix + b"*", count=self.BATCH_SIZE
                )
                timer.count += len(keys)
                for k in keys:
                    pipe.delete(k)
                    if len(pipe) >= self.BATCH_SIZE:
                        pipe.execute()
                        pipe = self.r.pipeline()
            if len(pipe) > 0:
                pipe.execute()
            pipe.close()
