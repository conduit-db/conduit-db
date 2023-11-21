@echo off

docker-compose -f .\docker-compose.yml kill
docker-compose -f .\docker-compose.yml down
docker volume prune --force

docker-compose -f docker-compose.yml build conduit-raw conduit-index
docker-compose -f .\docker-compose.yml up -d

set DEFAULT_DB_TYPE=MYSQL
py -m pytest tests_functional --verbose
py -m pytest tests --verbose
