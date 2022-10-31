@echo off

REM Install everything anew
py -3.10 -m pip install pylint -U
docker-compose -f .\docker-compose.yml down
docker volume prune --force
docker-compose -f .\docker-compose.yml build %* --parallel
docker-compose -f .\docker-compose.yml up --detach node mysql conduit-raw conduit-index

REM Run the tests
py -3.10 -m pytest tests
py -3.10 -m pytest tests_functional
