docker-compose -f docker-compose.yml -f docker-compose.indexing.yml down
docker volume prune --force
docker-compose -f docker-compose.yml -f docker-compose.indexing.yml build "$@" --parallel
docker-compose -f docker-compose.yml -f docker-compose.indexing.yml up
