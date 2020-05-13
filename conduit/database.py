from peewee import PostgresqlDatabase, Model, BlobField, IntegerField

from constants import (
    DATABASE_NAME_VARNAME,
    DATABASE_USER_VARNAME,
    DATABASE_HOST_VARNAME,
    DATABASE_PORT_VARNAME,
    DATABASE_PASSWORD_VARNAME,
)


db = PostgresqlDatabase(None)


class BaseModel(Model):
    class Meta:
        database = db


class Transaction(BaseModel):
    tx_hash = BlobField(primary_key=True)
    height = IntegerField()
    rawtx = BlobField()


def load(env_vars) -> PostgresqlDatabase:
    database_name = env_vars[DATABASE_NAME_VARNAME]
    database_user = env_vars[DATABASE_USER_VARNAME]
    host = env_vars[DATABASE_HOST_VARNAME]
    port = env_vars[DATABASE_PORT_VARNAME]
    password = env_vars[DATABASE_PASSWORD_VARNAME]

    db.init(
        database=database_name,
        user=database_user,
        host=host,
        port=port,
        password=password,
    )
    db.connect()
    db.drop_tables([Transaction], safe=True)  # Todo - remove when finished testing
    db.create_tables([Transaction], safe=True)
    db.close()
    return db


MAX_VARS = 999
