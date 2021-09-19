@echo off
REM tears down all running containers
REM builds only the service types you have selected (as args passed to this script)
REM no args will rebuild all
docker-compose -f docker-compose.yml -f docker-compose.indexing.yml down
docker-compose -f docker-compose.yml -f docker-compose.indexing.yml build %*
docker volume prune -y
docker-compose -f docker-compose.yml -f docker-compose.indexing.yml up
