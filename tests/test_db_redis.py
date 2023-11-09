from typing import Any

import pytest

from conduit_lib.database.redis.db import RedisCache


class TestRedisCache:
    @pytest.fixture(scope="class")
    def cache(self) -> RedisCache:
        cache = RedisCache()
        cache.r.flushall()
        cache.bulk_delete_all_in_namespace(b"namespace")
        yield RedisCache()
        cache.bulk_delete_all_in_namespace(b"namespace")

    def test_get_in_namespace(self, cache: RedisCache) -> Any:
        try:
            cache.set(b"namespace" + b"key", b"value")
            assert cache.get_in_namespace(namespace=b"namespace", key=b"key") == b"value"
        finally:
            cache.delete(b"namespace" + b"key")

    def test_bulk_load_delete_scan_in_namespace(self, cache: RedisCache) -> None:
        try:
            for batch_size in [1, 5000]:  # for 100% test coverage of all code paths
                cache.BATCH_SIZE = batch_size
                assert cache.get(b"namespace" + b"key1") is None
                assert cache.get(b"namespace" + b"key2") is None
                cache.bulk_load_in_namespace(
                    namespace=b"namespace", pairs=[(b"key1", b"value1"), (b"key2", b"value2")]
                )
                assert cache.get(b"namespace" + b"key1") == b"value1"
                assert cache.get(b"namespace" + b"key2") == b"value2"
                all_keys = cache.scan_in_namespace(namespace=b"namespace")
                for key in all_keys:
                    assert key in [b"namespace" + b"key1", b"namespace" + b"key2"]
                cache.bulk_delete_all_in_namespace(b"namespace")
                assert cache.get(b"namespace" + b"key1") is None
                assert cache.get(b"namespace" + b"key2") is None
                all_keys = cache.scan_in_namespace(namespace=b"namespace")
                assert all_keys == []
        finally:
            cache.BATCH_SIZE = 5000
