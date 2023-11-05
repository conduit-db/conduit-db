import uuid

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider

# Step 2.1: Set up the connection to the cluster
# If you have authentication enabled, use the PlainTextAuthProvider
# auth_provider = PlainTextAuthProvider(username='your_username', password='your_password')
cluster = Cluster()  # Add auth_provider=auth_provider as an argument if needed
session = cluster.connect()

session.default_timeout = 30  # increase this as needed

# Step 2.2: Create a keyspace. Note that replication settings should be adjusted for production use
session.execute(
    """
    CREATE KEYSPACE IF NOT EXISTS mykeyspace
    WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '1' }
"""
)

# Step 2.3: Use the new keyspace
session.set_keyspace('mykeyspace')

# Step 2.4: Create a table
session.execute(
    """
    CREATE TABLE IF NOT EXISTS users (
        id UUID PRIMARY KEY,
        name text,
        age int
    )
"""
)

# Step 2.5: Insert data into the table
from cassandra import ConsistencyLevel
from cassandra.query import SimpleStatement

insert_stmt = session.prepare("INSERT INTO users (id, name, age) VALUES (?, ?, ?)")
batch = [
    (uuid.uuid1(), 'Alice', 32),
    (uuid.uuid1(), 'Bob', 27),
    (uuid.uuid1(), 'Charlie', 36),
]

for user in batch:
    session.execute(insert_stmt.bind(user))

# Step 2.6: Query the data and print the results
rows = session.execute("SELECT id, name, age FROM users")
for row in rows:
    print(row.id, row.name, row.age)

# Clean up and close the connection
cluster.shutdown()
