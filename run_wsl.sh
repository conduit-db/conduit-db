#!/usr/bin/env bash

# Load environment variables
set -a
export $(grep -v '^#' .env.docker.wsl | xargs)
set +a
docker compose -f docker-compose.wsl.yml stop
docker compose -f docker-compose.wsl.yml down
docker compose -f docker-compose.wsl.yml build conduit-raw conduit-index --parallel
docker compose -f docker-compose.wsl.yml up -d scylladb redis conduit-raw
# This isn't strictly necessary but Scylla likes to have some time to take stock
# of its compaction state and do automated repairs if needed.
sleep 20
docker compose -f docker-compose.wsl.yml up -d conduit-index reference_server
docker compose -f docker-compose.wsl.yml logs -f
