@echo off

docker build -t static-checks . --no-cache
docker run static-checks
