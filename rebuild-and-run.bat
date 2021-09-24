@echo off
REM tears down all running containers
REM builds only the service types you have selected (as args passed to this script)
REM no args will rebuild all
docker-compose -f docker-compose.yml -f docker-compose.indexing.yml down
docker volume prune --force
docker-compose -f docker-compose.yml -f docker-compose.indexing.yml build %* --parallel
docker-compose -f docker-compose.yml -f docker-compose.indexing.yml up
