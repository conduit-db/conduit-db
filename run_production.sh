#!/usr/bin/env bash

# Load environment variables
set -a
export $(grep -v '^#' .env.docker.production | xargs)
set +a
docker build -f ./contrib/python_base/Dockerfile . -t python_base
docker compose -f docker-compose.production.yml stop
docker compose -f docker-compose.production.yml down
docker compose -f docker-compose.production.yml build --parallel --no-cache
docker compose -f docker-compose.production.yml up -d
docker compose -f docker-compose.production.yml logs -f
