@echo off

docker-compose -f .\docker-compose.yml kill
docker-compose -f .\docker-compose.yml down
docker volume prune --force

docker-compose -f docker-compose.yml build conduit-raw conduit-index

docker-compose -f .\docker-compose.yml up node mysql scylladb redis reference_server --detach

py -3.10 ./contrib/wait_for_scylladb.py

docker-compose -f .\docker-compose.yml up conduit-raw conduit-index --detach

REM start /MAX cmd /k "set PYTHONPATH=. && coverage run --parallel-mode ./conduit_raw/run_conduit_raw.py"
REM start /MAX cmd /k "set PYTHONPATH=. && coverage run --parallel-mode ./conduit_index/run_conduit_index.py"

REM Wait for the servers to initialize
timeout /t 25


set DEFAULT_DB_TYPE=SCYLLADB
py -m pytest tests_functional --verbose
REM py -m pytest tests --verbose


REM coverage run --parallel-mode -m pytest tests --verbose
REM coverage run --parallel-mode -m pytest tests_functional --verbose
REM coverage combine
REM coverage report
