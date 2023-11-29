@echo off

docker build -t static-checks .
docker run static-checks
