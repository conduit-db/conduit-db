@echo off
REM tears down all running containers
REM builds only the service types you have selected (as args passed to this script)
REM no args will rebuild all


REM docker-compose -f docker-compose.yml -f docker-compose.indexing.yml down
REM docker volume prune --force
REM docker-compose -f docker-compose.yml -f docker-compose.indexing.yml build %* --parallel
REM docker-compose -f docker-compose.yml -f docker-compose.indexing.yml up


docker-compose -f docker-compose.yml down
docker volume prune --force
docker-compose -f docker-compose.yml build %* --parallel
docker-compose -f docker-compose.yml up