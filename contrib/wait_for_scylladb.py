import os
import sys

from cassandra import ProtocolVersion, ConsistencyLevel
from cassandra.cluster import Cluster, TokenAwarePolicy, DCAwareRoundRobinPolicy, NoHostAvailable


def main():
    while True:
        cluster = None
        session = None
        try:
            cluster = Cluster(
                contact_points=[os.getenv('SCYLLA_HOST', '127.0.0.1')],
                port=int(os.getenv('SCYLLA_PORT', 19042)),
                protocol_version=ProtocolVersion.V4,
                load_balancing_policy=TokenAwarePolicy(DCAwareRoundRobinPolicy()),
                executor_threads=4,
                connect_timeout=30,
                # auth_provider=auth_provider,
            )
            session = cluster.connect()
            session.default_timeout = 120
            session.default_consistency_level = ConsistencyLevel.LOCAL_QUORUM
            print('ScyllaDB is now available')
            return True
        except (NoHostAvailable, ConnectionRefusedError):
            print("ScyllaDB is not yet available")
        except Exception as e:
            print(f"Unexpected exception type: {e}. Exiting loop")
            return False
        finally:
            if session:
                session.shutdown()
            if cluster:
                cluster.shutdown()


if __name__ == '__main__':
    if main():
        sys.exit(0)
    else:
        sys.exit(1)
