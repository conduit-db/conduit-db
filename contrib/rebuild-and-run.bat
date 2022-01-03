@echo off
REM tears down all running containers
REM builds only the service types you have selected (as args passed to this script)
REM no args will rebuild all


REM Get current folder with no trailing slash
SET ScriptDir=%~dp0
SET TLD=%ScriptDir%\..
echo %ScriptDir%
cd %ScriptDir%

docker-compose -f ..\docker-compose.yml down
docker volume prune --force
docker-compose -f ..\docker-compose.yml build %* --parallel
REM docker-compose -f ..\docker-compose.yml up node mysql
docker-compose -f ..\docker-compose.yml up node mysql conduit-raw conduit-index
