@echo off

docker-compose -f .\docker-compose.yml kill
docker-compose -f .\docker-compose.yml down
docker volume prune --force
docker-compose -f .\docker-compose.yml up node mysql scylladb redis reference_server conduit-raw conduit-index --detach

REM Wait for the servers to initialize
timeout /t 10

REM start /MAX cmd /k "set PYTHONPATH=. && coverage run --parallel-mode ./conduit_raw/run_conduit_raw.py"
REM start /MAX cmd /k "set PYTHONPATH=. && coverage run --parallel-mode ./conduit_index/run_conduit_index.py"

REM Wait for the servers to initialize
timeout /t 10

REM Now run your functional tests
REM coverage run --parallel-mode -m pytest tests --verbose
REM coverage run --parallel-mode -m pytest tests_functional --verbose
py -m pytest tests_functional --verbose
py -m pytest tests --verbose
REM coverage combine
REM coverage report
