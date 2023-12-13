@echo off

docker-compose -f .\docker-compose.yml kill
docker-compose -f .\docker-compose.yml down
docker volume prune --force

docker-compose -f docker-compose.yml build conduit-raw conduit-index --no-cache
docker-compose -f .\docker-compose.yml up -d

set DEFAULT_DB_TYPE=SCYLLADB
REM Give time for ScyllaDB to boostrap
timeout /t 5
py -m pytest tests_functional --verbose
py -m pytest tests --verbose
