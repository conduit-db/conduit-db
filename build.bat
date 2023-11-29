@echo off

docker build -f ./contrib/python_base/Dockerfile . -t python_base
docker-compose -f docker-compose.yml build --parallel --no-cache
