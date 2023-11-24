#!/usr/bin/env bash

# Load environment variables
set -a
source .env.docker.production
set +a

# Check if at least one argument is provided
if [ $# -eq 0 ]; then
    echo "No container names provided. Tailing logs for all containers."
    docker-compose -f docker-compose.production.yml logs -f
else
    echo "Tailing logs for containers: " + "$@"
    docker-compose -f docker-compose.production.yml logs -f "$@"
fi
