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


def load(config) -> PostgresqlDatabase:
    database_name = config[DATABASE_NAME_VARNAME]
    database_user = config[DATABASE_USER_VARNAME]
    host = config[DATABASE_HOST_VARNAME]
    port = config[DATABASE_PORT_VARNAME]
    password = config[DATABASE_PASSWORD_VARNAME]

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
