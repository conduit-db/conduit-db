#!/usr/bin/env bash

# Load environment variables
set -a
export $(grep -v '^#' .env.docker.production | xargs)
set +a
docker compose -f docker-compose.production.yml stop
docker compose -f docker-compose.production.yml down
docker compose -f docker-compose.production.yml build --parallel --no-cache
docker compose -f docker-compose.production.yml up -d scylladb redis conduit-raw
# This isn't strictly necessary but Scylla likes to have some time to take stock
# of its compaction state and do automated repairs if needed.
sleep 20
docker compose -f docker-compose.production.yml up -d conduit-index reference_server
docker compose -f docker-compose.production.yml logs -f
