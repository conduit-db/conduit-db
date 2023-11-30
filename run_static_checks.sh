#!/usr/bin/env bash

docker build -t static-checks .
docker run static-checks
