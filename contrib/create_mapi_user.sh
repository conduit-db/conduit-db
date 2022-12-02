#!/bin/bash
set -e

# This is a modification of this:
# https://github.com/bitcoin-sv/merchantapi-reference/blob/master/src/MerchantAPI/APIGateway/APIGateway.Database/APIGateway.Database/Scripts/Postgres/00_CreateDB/01_SYS_CreateRole.sql
# Initialization script

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL

    CREATE ROLE mapi_crud LOGIN
      PASSWORD 'merchant'
      NOSUPERUSER
      INHERIT
      NOCREATEDB
      NOCREATEROLE
      NOREPLICATION;

    DROP ROLE IF EXISTS merchant;

    CREATE ROLE merchant LOGIN
      PASSWORD 'merchant'
      NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;

    GRANT mapi_crud TO merchant;

    DROP ROLE IF EXISTS merchantddl;

    CREATE ROLE merchantddl LOGIN
      PASSWORD 'merchant'
    NOSUPERUSER INHERIT NOCREATEDB NOCREATEROLE NOREPLICATION;

EOSQL

