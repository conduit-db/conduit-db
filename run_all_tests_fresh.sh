#!/usr/bin/env bash

docker-compose -f .\docker-compose.yml kill
docker-compose -f .\docker-compose.yml down
docker volume prune --force

docker-compose -f docker-compose.yml build conduit-raw conduit-index
docker-compose -f .\docker-compose.yml up -d

export DEFAULT_DB_TYPE=SCYLLADB
py -m pytest tests_functional --verbose
py -m pytest tests --verbose
